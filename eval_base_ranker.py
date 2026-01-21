import argparse
import json
import math
import os
import re
import multiprocessing as mp
import itertools
from decimal import Decimal, getcontext
from datetime import datetime
from pathlib import Path

from clause import Ranking
from clause import TripleSet


def log_step(message: str) -> None:
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[STEP {now}] {message}")


getcontext().prec = 50


def parse_rule_line(line: str):
    parts = line.split("\t", 3)
    if len(parts) < 4:
        return None
    try:
        body_size = int(parts[0])
        support = int(parts[1])
        score = float(parts[2])
    except ValueError:
        return None
    return {
        "bodySize": body_size,
        "support": support,
        "score": score,
        "rule": parts[3],
    }


def rule_surprisal(body_size: int, support: int, num_unseen: int) -> float:
    denom = support + num_unseen
    if denom <= 0:
        return 0.0
    ratio = body_size / denom
    if ratio >= 1:
        ratio = 1 - 1e-12
    if ratio <= 0:
        return 0.0
    return -math.log(1.0 - ratio)


def is_d_rule(rule_str: str) -> bool:
    parts = rule_str.split("<=", 1)
    if len(parts) != 2:
        return False
    body = parts[1]
    # d rule: body contains exactly one occurrence of "(A," or ",A)"
    return body.count("(A,") + body.count(",A)") == 1


def is_z_rule(rule_str: str) -> bool:
    parts = rule_str.split("<=", 1)
    if len(parts) != 2:
        return False
    return parts[1].strip() == ""


def is_b_rule(rule_str: str) -> bool:
    parts = rule_str.split("<=", 1)
    if len(parts) != 2:
        return False
    head = parts[0].strip()
    return "(X,Y)" in head


def load_rule_surprisals(
    rules_path: Path,
    num_unseen: int,
    d_weight: float,
    z_weight: float,
    use_rule_confidence: bool
) -> dict[int, float]:
    rule_surprisal_map: dict[int, float] = {}
    cnt = 0
    with open(rules_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            parsed = parse_rule_line(line.strip())
            if not parsed:
                continue
            if is_z_rule(parsed["rule"]):
                weight = z_weight
            elif is_d_rule(parsed["rule"]):
                weight = d_weight
            else:
                weight = 1.0
            conf = 0.0
            denom = parsed["bodySize"] + num_unseen
            if denom > 0:
                conf = parsed["support"] / denom
            if use_rule_confidence:
                if parsed["score"] != conf and cnt <= 10:
                    cnt +=1
                    print("Line {}: score={} computed_conf={}".format(line_num, parsed["score"], conf))
                conf = parsed["score"]

            conf = min(max(conf, 0.0), 1.0 - 1e-12)
            rule_surprisal_map[line_num] = weight * (-math.log(1.0 - conf))
    return rule_surprisal_map


def load_rule_type_sets(rules_path: Path) -> tuple[set[int], set[int]]:
    b_rules: set[int] = set()
    d_rules: set[int] = set()
    with open(rules_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            parsed = parse_rule_line(line.strip())
            if not parsed:
                continue
            rule = parsed["rule"]
            if is_d_rule(rule):
                d_rules.add(line_num)
            if is_b_rule(rule):
                b_rules.add(line_num)
    return b_rules, d_rules


def load_dependency_graph(
    dep_path: Path, dep_threshold: float
    , disabled_rule_ids: set[int] | None = None
) -> tuple[dict[int, dict[int, float]], dict[int, dict[int, float]]]:
    with open(dep_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    graph_pos: dict[int, dict[int, float]] = {}
    graph_neg: dict[int, dict[int, float]] = {}
    if not isinstance(data, dict):
        return graph_pos, graph_neg
    for k1, v in data.items():
        if not isinstance(v, dict):
            continue
        try:
            src = int(k1)
        except (TypeError, ValueError):
            continue
        if disabled_rule_ids and src in disabled_rule_ids:
            continue
        dsts_pos: dict[int, float] = {}
        dsts_neg: dict[int, float] = {}
        for k2, lift in v.items():
            try:
                dst = int(k2)
                lift_val = float(lift)
            except (TypeError, ValueError):
                continue
            if disabled_rule_ids and dst in disabled_rule_ids:
                continue
            if lift_val > dep_threshold:
                dsts_pos[dst] = lift_val
            elif lift_val < -dep_threshold:
                dsts_neg[dst] = lift_val
        if dsts_pos:
            graph_pos[src] = dsts_pos
        if dsts_neg:
            graph_neg[src] = dsts_neg
    return graph_pos, graph_neg


def _summarize_ranking(ranking: dict) -> tuple[int, int, int]:
    relation_count = len(ranking)
    query_count = 0
    candidate_total = 0
    for queries in ranking.values():
        query_count += len(queries)
        for candidates in queries.values():
            candidate_total += len(candidates)
    return relation_count, query_count, candidate_total


def _safe_rule_id(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_decay_coeff(aggregation: str) -> Decimal | None:
    if not aggregation.startswith("decay"):
        return None
    digits = aggregation[5:]
    if not digits.isdigit() or digits == "":
        return None
    return Decimal("0." + digits)


def parse_maxplus_dep_k(aggregation: str) -> int | None:
    if not aggregation.startswith("maxplus+dep"):
        return None
    digits = aggregation[len("maxplus+dep"):]
    if not digits.isdigit() or digits == "":
        return None
    return int(digits)

def select_non_overlapping_lifts(edges: list[tuple[int, int, float]], k: int) -> list[float]:
    used = set()
    selected = []
    for u, v, lift in edges:
        if u in used or v in used:
            continue
        used.add(u)
        used.add(v)
        selected.append(lift)
        if k > 0 and len(selected) >= k:
            break
    return selected


def parse_noisyor_dep_k(aggregation: str) -> int | None:
    if not aggregation.startswith("noisyor+dep"):
        return None
    digits = aggregation[len("noisyor+dep"):]
    if not digits.isdigit() or digits == "":
        return None
    return int(digits)

def parse_noisyor_depm_k(aggregation: str) -> int | None:
    if not aggregation.startswith("noisyor+depm"):
        return None
    digits = aggregation[len("noisyor+depm"):]
    if not digits.isdigit() or digits == "":
        return None
    return int(digits)


def parse_noisyor_dep_minus_k(aggregation: str) -> int | None:
    if not aggregation.startswith("noisyor-dep"):
        return None
    digits = aggregation[len("noisyor-dep"):]
    if not digits.isdigit() or digits == "":
        return None
    return int(digits)


def parse_noisyor_dep_minus_m_k(aggregation: str) -> int | None:
    if not aggregation.startswith("noisyor-depm"):
        return None
    digits = aggregation[len("noisyor-depm"):]
    if not digits.isdigit() or digits == "":
        return None
    return int(digits)


def aggregate_surprisals(values: list[float], aggregation: str) -> Decimal:
    if not values:
        return Decimal(0)
    if aggregation.startswith("maxplus+dep"):
        aggregation = "maxplus"
    if aggregation.startswith("noisyor+depm"):
        aggregation = "noisyor"
    if aggregation.startswith("noisyor+dep"):
        aggregation = "noisyor"
    if aggregation.startswith("noisyor-dep"):
        aggregation = "noisyor"
    if aggregation.startswith("noisyor-depm"):
        aggregation = "noisyor"
    sorted_vals = sorted(values, reverse=True)
    if aggregation == "max":
        return Decimal(str(sorted_vals[0]))
    if aggregation == "noisyor":
        total = Decimal(0)
        for v in sorted_vals:
            total += Decimal(str(v))
        return total
    if aggregation == "maxplus":
        coeff = Decimal("0.1")
    else:
        coeff = parse_decay_coeff(aggregation)
        if coeff is None:
            raise ValueError(f"Unknown aggregation: {aggregation}")

    total = Decimal(0)
    factor = Decimal(1)
    for v in sorted_vals:
        total += Decimal(str(v)) * factor
        factor *= coeff
    return total


_RULE_SURPRISAL_MAP: dict[int, float] | None = None
_RULE_WEIGHT_MAP: dict[int, float] | None = None
_DEP_GRAPH_POS: dict[int, dict[int, float]] | None = None
_DEP_GRAPH_NEG: dict[int, dict[int, float]] | None = None
_ENTITY_FREQ: dict[str, int] | None = None
_TIE_HANDLING: str | None = None
_AGGREGATION: str | None = None


def select_noisyor_dep_lifts(
    rule_sorted: list[tuple[int, float]],
    dep_graph: dict[int, dict[int, float]],
    k: int,
) -> list[float]:
    used = set()
    selected = []
    for u, _ in rule_sorted:
        if u in used:
            continue
        lifts = dep_graph.get(u, {})
        if not lifts:
            continue
        # pick largest edge for this node
        v, lift = max(lifts.items(), key=lambda x: x[1])
        if v in used:
            continue
        used.add(u)
        used.add(v)
        selected.append(lift)
        if k > 0 and len(selected) >= k:
            break
    return selected


def _init_worker(rule_surprisal_map, rule_weight_map, dep_graph_pos, dep_graph_neg, entity_freq, tie_handling, aggregation):
    global _RULE_SURPRISAL_MAP, _RULE_WEIGHT_MAP, _DEP_GRAPH_POS, _DEP_GRAPH_NEG, _ENTITY_FREQ, _TIE_HANDLING, _AGGREGATION
    _RULE_SURPRISAL_MAP = rule_surprisal_map
    _RULE_WEIGHT_MAP = rule_weight_map
    _DEP_GRAPH_POS = dep_graph_pos
    _DEP_GRAPH_NEG = dep_graph_neg
    _ENTITY_FREQ = entity_freq
    _TIE_HANDLING = tie_handling
    _AGGREGATION = aggregation


def _score_query(item: tuple[str, str, dict, str]) -> tuple[str, str, str, list[tuple[str, float]], list[tuple[str, float]] | None, int, int, int]:
    relation, constant, candidate_dict, direction = item
    rule_surprisal_map = _RULE_SURPRISAL_MAP or {}
    rule_weight_map = _RULE_WEIGHT_MAP or {}
    dep_graph_pos = _DEP_GRAPH_POS or {}
    dep_graph_neg = _DEP_GRAPH_NEG or {}
    entity_freq = _ENTITY_FREQ
    tie_handling = _TIE_HANDLING or "frequency"
    aggregation = _AGGREGATION or "noisyor"
    dep_k = parse_maxplus_dep_k(aggregation)
    noisyor_dep_k = parse_noisyor_dep_k(aggregation)
    noisyor_depm_k = parse_noisyor_depm_k(aggregation)
    noisyor_dep_minus_k = parse_noisyor_dep_minus_k(aggregation)
    noisyor_dep_minus_m_k = parse_noisyor_dep_minus_m_k(aggregation)

    want_base = (
        aggregation.startswith("noisyor+dep")
        or aggregation.startswith("noisyor+depm")
        or aggregation.startswith("maxplus+dep")
        or aggregation.startswith("noisyor-dep")
        or aggregation.startswith("noisyor-depm")
    )
    pairs: list[tuple[str, float]] = []
    base_pairs: list[tuple[str, float]] | None = [] if want_base else None
    maxplus_dep_keys: dict[str, tuple] | None = {} if dep_k is not None else None
    maxplus_keys: dict[str, tuple] | None = (
        {} if aggregation == "maxplus" or aggregation.startswith("maxplus+dep") else None
    )
    for candidate, rule_ids in candidate_dict.items():
        surprisal_list: list[float] = []
        rule_list: list[tuple[int, float]] = []
        for rid in rule_ids:
            rule_id = _safe_rule_id(rid)
            if rule_id is None:
                continue
            weight = rule_weight_map.get(rule_id, 1.0)
            surprisal_list.append(weight * rule_surprisal_map.get(rule_id, 0.0))
            rule_list.append((rule_id, weight * rule_surprisal_map.get(rule_id, 0.0)))
        agg_score = aggregate_surprisals(surprisal_list, aggregation)
        base_score = float(agg_score)
        if noisyor_depm_k is not None:
            rule_ids_set = {rid for rid, _ in rule_list}
            edges = []
            # limit to top 100 rules to reduce computation
            for u in list(rule_ids_set)[:100]:
                for v, lift in dep_graph_pos.get(u, {}).items():
                    if v in rule_ids_set:
                        edges.append((u, v, lift))
            edges.sort(key=lambda x: x[2], reverse=True)
            k = noisyor_depm_k
            if k == 0:
                k = -1
            selected = select_non_overlapping_lifts(edges, k)
            if selected:
                agg_score = Decimal(str(float(agg_score) + sum(selected)))
        if noisyor_dep_k is not None:
            rule_sorted = sorted(rule_list, key=lambda x: (-x[1], x[0]))
            k = noisyor_dep_k
            if k == 0:
                k = -1
            selected = select_noisyor_dep_lifts(rule_sorted, dep_graph_pos, k)
            if selected:
                agg_score = Decimal(str(float(agg_score) + sum(selected)))
        if noisyor_dep_minus_k is not None:
            rule_ids_set = {rid for rid, _ in rule_list}
            incoming: dict[int, list[int]] = {}
            # limit to top 100 rules to reduce computation
            for u in list(rule_ids_set)[:100]:
                for v in dep_graph_neg.get(u, {}).keys():
                    if v in rule_ids_set and v != u:
                        incoming.setdefault(v, []).append(u)
            rule_sorted = sorted(rule_list, key=lambda x: (-x[1], x[0]))
            score_map = {rid: score for rid, score in rule_sorted}
            k = noisyor_dep_minus_k
            if k == 0:
                k = -1
            suppressed = 0
            blocked_sources = set()
            suppressed_sum = 0.0
            for idx, (rid, score) in enumerate(rule_sorted):
                if idx == 0:
                    continue
                if k > 0 and suppressed >= k:
                    break
                candidates = [u for u in incoming.get(rid, []) if u not in blocked_sources]
                if not candidates:
                    continue
                # choose highest-surprisal incoming node
                _ = max(candidates, key=lambda u: score_map.get(u, 0.0))
                blocked_sources.add(rid)
                suppressed += 1
                suppressed_sum += score
            total_score = sum(score_map.values())
            agg_score = Decimal(str(float(total_score - suppressed_sum)))
        if noisyor_dep_minus_m_k is not None:
            rule_ids_set = {rid for rid, _ in rule_list}
            edges: list[tuple[int, int, float]] = []
            # limit to top 100 rules to reduce computation
            for u in list(rule_ids_set)[:100]:
                for v, lift in dep_graph_neg.get(u, {}).items():
                    if v in rule_ids_set and v != u:
                        edges.append((u, v, abs(lift)))
            edges.sort(key=lambda x: x[2], reverse=True)
            rule_sorted = sorted(rule_list, key=lambda x: (-x[1], x[0]))
            score_map = {rid: score for rid, score in rule_sorted}
            k = noisyor_dep_minus_m_k
            if k == 0:
                k = -1
            suppressed = 0
            used_nodes = set()
            suppressed_sum = 0.0
            for u, v, _ in edges:
                if k > 0 and suppressed >= k:
                    break
                if u in used_nodes or v in used_nodes:
                    continue
                if v not in score_map:
                    continue
                used_nodes.add(u)
                used_nodes.add(v)
                suppressed += 1
                suppressed_sum += score_map.get(v, 0.0)
            total_score = sum(score_map.values())
            agg_score = Decimal(str(float(total_score - suppressed_sum)))
        pairs.append((str(candidate), float(agg_score)))
        if base_pairs is not None:
            base_pairs.append((str(candidate), base_score))
        if maxplus_keys is not None:
            sorted_vals = [c for _, c in sorted(rule_list, key=lambda x: (-x[1], x[0]))]
            maxplus_keys[str(candidate)] = tuple(sorted_vals)
        if maxplus_dep_keys is not None:
            rule_sorted = sorted(rule_list, key=lambda x: (-x[1], x[0]))
            v1 = rule_sorted[0][0] if rule_sorted else None
            w1 = rule_sorted[0][1] if rule_sorted else 0.0
            dep_vals = []
            if v1 is not None:
                lifts = dep_graph_pos.get(v1, {})
                w_map = {rid: w for rid, w in rule_sorted}
                lift_scores = []
                for rid, lift in lifts.items():
                    wj = w_map.get(rid, 0.0)
                    lift_scores.append(lift + wj)
                top_lifts = sorted(lift_scores, reverse=True)[:dep_k]
                dep_vals = top_lifts + [0.0] * max(0, dep_k - len(top_lifts))
            w2 = [c for _, c in rule_sorted[1:]]
            maxplus_dep_keys[str(candidate)] = (
                -w1,
                tuple(-v for v in dep_vals),
                tuple(-v for v in w2),
            )

    tie_total = 0
    tie_dep_used = 0
    tie_unresolved = 0
    if dep_k is not None and maxplus_dep_keys is not None:
        if tie_handling == "frequency" and entity_freq is not None:
            pairs.sort(
                key=lambda x: (
                    maxplus_dep_keys.get(x[0], (0.0, (), ())),
                    -entity_freq.get(x[0], 0),
                    x[0],
                )
            )
        else:
            pairs.sort(key=lambda x: (maxplus_dep_keys.get(x[0], (0.0, (), ())), x[0]))
        w1s = [key[0] for key in maxplus_dep_keys.values()]
        if w1s and len(set(w1s)) < len(w1s):
            tie_total = 1
            dep_keys = [key[1] for key in maxplus_dep_keys.values()]
            if dep_keys and len(set(dep_keys)) > 1:
                tie_dep_used = 1
            else:
                tie_unresolved = 1
    elif aggregation == "maxplus" and maxplus_keys is not None:
        if tie_handling == "frequency" and entity_freq is not None:
            pairs.sort(key=lambda x: (tuple(-v for v in maxplus_keys.get(x[0], ())), -entity_freq.get(x[0], 0), x[0]))
        else:
            pairs.sort(key=lambda x: (tuple(-v for v in maxplus_keys.get(x[0], ())), x[0]))
    else:
        if tie_handling == "frequency" and entity_freq is not None:
            pairs.sort(key=lambda x: (-x[1], -entity_freq.get(x[0], 0), x[0]))
        else:
            pairs.sort(key=lambda x: (-x[1], x[0]))

    if base_pairs is not None:
        if aggregation.startswith("maxplus+dep") and maxplus_keys is not None:
            if tie_handling == "frequency" and entity_freq is not None:
                base_pairs.sort(
                    key=lambda x: (
                        tuple(-v for v in maxplus_keys.get(x[0], ())),
                        -entity_freq.get(x[0], 0),
                        x[0],
                    )
                )
            else:
                base_pairs.sort(key=lambda x: (tuple(-v for v in maxplus_keys.get(x[0], ())), x[0]))
        else:
            if tie_handling == "frequency" and entity_freq is not None:
                base_pairs.sort(key=lambda x: (-x[1], -entity_freq.get(x[0], 0), x[0]))
            else:
                base_pairs.sort(key=lambda x: (-x[1], x[0]))

    return direction, relation, constant, pairs, base_pairs, tie_total, tie_dep_used, tie_unresolved


def build_scores_parallel(
    head_applied: dict,
    tail_applied: dict,
    rule_surprisal_map: dict[int, float],
    rule_weight_map: dict[int, float] | None,
    dep_graph_pos: dict[int, dict[int, float]] | None,
    dep_graph_neg: dict[int, dict[int, float]] | None,
    entity_freq: dict[str, int] | None,
    tie_handling: str,
    aggregation: str,
    workers: int,
    chunksize: int,
) -> tuple[dict, dict, dict | None, dict | None]:
    head_result: dict = {}
    tail_result: dict = {}
    want_base = (
        aggregation.startswith("noisyor+dep")
        or aggregation.startswith("noisyor+depm")
        or aggregation.startswith("maxplus+dep")
        or aggregation.startswith("noisyor-dep")
        or aggregation.startswith("noisyor-depm")
        or aggregation.startswith("noisyor-depm")
    )
    base_head: dict | None = {} if want_base else None
    base_tail: dict | None = {} if want_base else None
    rule_weight_map = rule_weight_map or {}
    dep_graph_pos = dep_graph_pos or {}
    dep_graph_neg = dep_graph_neg or {}
    if workers == 0:
        workers = os.cpu_count() or 1

    tasks = (
        (relation, str(constant), candidate_dict, "head")
        for relation, constant_dict in head_applied.items()
        for constant, candidate_dict in constant_dict.items()
    )
    tasks_tail = (
        (relation, str(constant), candidate_dict, "tail")
        for relation, constant_dict in tail_applied.items()
        for constant, candidate_dict in constant_dict.items()
    )

    tie_total = 0
    tie_dep_used = 0
    tie_unresolved = 0

    log_step(f"Scoring queries with {workers} workers...")
    with mp.Pool(
        processes=workers,
        initializer=_init_worker,
        initargs=(rule_surprisal_map, rule_weight_map, dep_graph_pos, dep_graph_neg, entity_freq, tie_handling, aggregation),
    ) as pool:
        for idx, (direction, relation, constant, pairs, base_pairs, t_total, t_used, t_unres) in enumerate(
            pool.imap_unordered(
            _score_query,
            itertools.chain(tasks, tasks_tail),
            chunksize=chunksize,
            ),
            1,
        ):
            if direction == "head":
                head_result.setdefault(relation, {})[constant] = pairs
                if base_head is not None and base_pairs is not None:
                    base_head.setdefault(relation, {})[constant] = base_pairs
            else:
                tail_result.setdefault(relation, {})[constant] = pairs
                if base_tail is not None and base_pairs is not None:
                    base_tail.setdefault(relation, {})[constant] = base_pairs
            tie_total += t_total
            tie_dep_used += t_used
            tie_unresolved += t_unres
            if idx % 2000 == 0:
                log_step(f"Processed {idx} queries")

    dep_k = parse_maxplus_dep_k(aggregation)
    if dep_k is not None:
        print(
            f"[STAT] maxplus+dep{dep_k} tie queries: {tie_total}, dependency used: {tie_dep_used}, unresolved: {tie_unresolved}"
        )
    return head_result, tail_result, base_head, base_tail


def load_entity_freq(train_path: Path) -> dict[str, int]:
    freq: dict[str, int] = {}
    with open(train_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            h, _, t = parts[0], parts[1], parts[2]
            freq[h] = freq.get(h, 0) + 1
            freq[t] = freq.get(t, 0) + 1
    return freq


def compare_rankings(
    base: dict,
    ref: dict
) -> None:
    shared_queries = []
    for rel, const_dict in base.items():
        ref_const = ref.get(rel, {})
        for const in const_dict:
            if const in ref_const:
                shared_queries.append((rel, const))

    print(f"Shared queries: {len(shared_queries)}")
    if not shared_queries:
        return

    topk=100
    diff_limit=5
    sample = shared_queries[:10]
    overlap_scores = []
    for rel, const in sample:
        base_list = base[rel][const][:topk]
        ref_list = ref[rel][const][:topk]
        base_set = {c for c, _ in base_list}
        ref_set = {c for c, _ in ref_list}
        if not base_set and not ref_set:
            overlap = 1.0
        else:
            overlap = len(base_set & ref_set) / max(len(base_set | ref_set), 1)
        overlap_scores.append(overlap)

    avg_overlap = sum(overlap_scores) / len(overlap_scores)
    print(f"Sample avg top{topk} Jaccard overlap: {avg_overlap:.4f}")

    diff_count = 0
    for rel, const in sample:
        base_list = base[rel][const][:topk]
        ref_list = ref[rel][const][:topk]
        base_rank = {c: (i + 1, s) for i, (c, s) in enumerate(base_list)}
        ref_rank = {c: (i + 1, s) for i, (c, s) in enumerate(ref_list)}
        only_base = [c for c in base_rank.keys() if c not in ref_rank]
        only_ref = [c for c in ref_rank.keys() if c not in base_rank]
        if not only_base and not only_ref:
            continue
        print(f"Query diff: rel={rel}, const={const}")
        if only_base:
            print("  Only in base:")
            for c in only_base[:diff_limit]:
                r, s = base_rank[c]
                print(f"    {c}\t rank={r}\t score={s:.6g}")
        if only_ref:
            print("  Only in eval:")
            for c in only_ref[:diff_limit]:
                r, s = ref_rank[c]
                print(f"    {c}\t rank={r}\t score={s:.6g}")
        diff_count += 1
        if diff_count >= diff_limit:
            break


def _get_rank(ranking: dict, relation: str, constant: str, target: str) -> int | None:
    const_bucket = ranking.get(relation, {})
    candidates = const_bucket.get(constant)
    if not candidates:
        return None
    for idx, (cand, _) in enumerate(candidates, 1):
        if cand == target:
            return idx
    return None


def load_triples_from_file(path: str) -> list[tuple[str, str, str]]:
    triples: list[tuple[str, str, str]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            h, r, t = parts[0], parts[1], parts[2]
            triples.append((h, r, t))
    return triples


def _bucket_label(rule_count: int) -> str | None:
    if rule_count <= 5:
        return "≤5"
    if rule_count <= 10:
        return "≤10"
    if rule_count <= 20:
        return "≤20"
    if rule_count <= 40:
        return "≤40"
    if rule_count <= 80:
        return "≤80"
    if rule_count <= 120:
        return "≤120"
    return "120+"


def _get_query_rule_count(applied: dict, relation: str, constant: str) -> int:
    candidate_map = applied.get(relation, {}).get(constant)
    if not isinstance(candidate_map, dict):
        return 0
    rule_set = set()
    for rule_ids in candidate_map.values():
        if rule_ids is None:
            continue
        try:
            for rid in rule_ids:
                rule_set.add(rid)
        except TypeError:
            continue
    return len(rule_set)


def compute_rank_change_stats(
    triples: list[tuple[str, str, str]],
    base_head: dict,
    base_tail: dict,
    lift_head: dict,
    lift_tail: dict,
    head_applied: dict,
    tail_applied: dict,
    enable_buckets: bool = True,
) -> tuple[int, int, int, int, float, dict[str, dict[str, float]] | None]:
    changed = 0
    improved = 0
    worsened = 0
    total = 0
    delta_mrr_sum = 0.0
    buckets = {"≤5": {}, "≤10": {}, "≤20": {}, "≤40": {}, "≤80": {}, "≤120": {}, "120+": {}}
    for stats in buckets.values():
        stats["improved"] = 0
        stats["worsened"] = 0
        stats["delta_pos"] = 0.0
        stats["delta_neg"] = 0.0

    for h, r, t in triples:
        # head prediction
        r_base = _get_rank(base_head, r, t, h)
        r_lift = _get_rank(lift_head, r, t, h)
        if r_base is not None and r_lift is not None:
            total += 1
            delta = (1.0 / r_lift) - (1.0 / r_base)
            delta_mrr_sum += delta
            if r_lift != r_base:
                changed += 1
                if r_lift < r_base:
                    improved += 1
                elif r_lift > r_base:
                    worsened += 1
            if enable_buckets:
                rc = _get_query_rule_count(head_applied, r, t)
                label = _bucket_label(rc)
                if label is not None:
                    if delta > 0:
                        buckets[label]["delta_pos"] += delta
                    elif delta < 0:
                        buckets[label]["delta_neg"] += delta
                    if r_lift < r_base:
                        buckets[label]["improved"] += 1
                    elif r_lift > r_base:
                        buckets[label]["worsened"] += 1

        # tail prediction
        r_base = _get_rank(base_tail, r, h, t)
        r_lift = _get_rank(lift_tail, r, h, t)
        if r_base is not None and r_lift is not None:
            total += 1
            delta = (1.0 / r_lift) - (1.0 / r_base)
            delta_mrr_sum += delta
            if r_lift != r_base:
                changed += 1
                if r_lift < r_base:
                    improved += 1
                elif r_lift > r_base:
                    worsened += 1
            if enable_buckets:
                rc = _get_query_rule_count(tail_applied, r, h)
                label = _bucket_label(rc)
                if label is not None:
                    if delta > 0:
                        buckets[label]["delta_pos"] += delta
                    elif delta < 0:
                        buckets[label]["delta_neg"] += delta
                    if r_lift < r_base:
                        buckets[label]["improved"] += 1
                    elif r_lift > r_base:
                        buckets[label]["worsened"] += 1
    return total, changed, improved, worsened, delta_mrr_sum, (buckets if enable_buckets else None)


def get_num_entities(testset: TripleSet) -> int | None:
    for attr in ("entities", "entity_ids", "entity2id", "id2entity", "entity_dict"):
        if hasattr(testset, attr):
            try:
                return len(getattr(testset, attr))
            except TypeError:
                continue
    return None


argparser = argparse.ArgumentParser(description="Base ranker evaluation using applied_rules")
argparser.add_argument("--dataset", type=str, default="wnrr", help="dataset to use")
argparser.add_argument("--rules", type=str, default="", help="rules file to use")
argparser.add_argument("--applied_rules", type=str, default="", help="applied_rules json file")
argparser.add_argument("--compare_eval_ranking", type=str, default="", help="eval ranking dump json to compare")
argparser.add_argument("--valid", action="store_true", help="whether to use valid set for evaluation")
argparser.add_argument("--test_valid_split", type=str, default="", help="valid/test split suffix")
argparser.add_argument("--num_unseen", type=int, default=5, help="num_unseen for surprisal")
argparser.add_argument("--d_weight", type=float, default=0.1, help="weight for d rules")
argparser.add_argument("--z_weight", type=float, default=0.01, help="weight for z rules")
argparser.add_argument("--use_rule_confidence", action="store_true", help="use third-column confidence")
argparser.add_argument("--tie_handling", type=str, default="frequency", help="tie handling: frequency/random")
argparser.add_argument("--dependency_json", type=str, default="", help="dependency.json path for rule weights")
argparser.add_argument("--dep_threshold", type=float, default=0.0, help="|lift| threshold for dependency edges")
argparser.add_argument(
    "--dep_disable",
    type=str,
    default="d",
    help="disable dependency edges with rule types in set (b,c,d). default: d",
)
argparser.add_argument(
    "--aggregation",
    type=str,
    default="noisyor",
    help="aggregation: max|maxplus|maxplus+depK|noisyor|noisyor+depK|noisyor-depK|noisyor-depmK|decayXX",
)
argparser.add_argument("--workers", type=int, default=0, help="num processes for ranking build, 0 for cpu count")
argparser.add_argument("--chunksize", type=int, default=64, help="chunk size for multiprocessing")

args = argparser.parse_args()
start_time = datetime.now()

dataset = args.dataset
log_step("Parsed arguments")
rules_path = Path(args.rules if args.rules else f"data/rules/{dataset}.txt")
applied_rules_path = Path(
    args.applied_rules if args.applied_rules else f"out/{dataset}/applied_rules.json"
)

if args.valid:
    target = f"data/{dataset}/valid{args.test_valid_split}.txt"
else:
    target = f"data/{dataset}/test{args.test_valid_split}.txt"

if not rules_path.exists():
    raise FileNotFoundError(f"rules file not found: {rules_path}")
if not applied_rules_path.exists():
    raise FileNotFoundError(f"applied_rules file not found: {applied_rules_path}")

log_step(f"Rules path: {rules_path}")
log_step(f"Applied rules path: {applied_rules_path}")

log_step("Loading rule surprisals...")
rule_surprisal_map = load_rule_surprisals(
    rules_path,
    args.num_unseen,
    args.d_weight,
    args.z_weight,
    args.use_rule_confidence
)
log_step(f"Loaded rule surprisals: {len(rule_surprisal_map)}")

disable_types = {c for c in args.dep_disable.lower() if c in {"b", "c", "d"}}
disabled_rule_ids: set[int] = set()
if disable_types:
    b_rules, d_rules = load_rule_type_sets(rules_path)
    if "b" in disable_types:
        disabled_rule_ids.update(b_rules)
    if "d" in disable_types:
        disabled_rule_ids.update(d_rules)
    # "c" means rules that are neither b nor d
    if "c" in disable_types:
        disabled_rule_ids.update(
            rid for rid in rule_surprisal_map.keys() if rid not in b_rules and rid not in d_rules
        )

rule_weight_map = {}
dep_graph_pos = {}
dep_graph_neg = {}
dep_k = parse_maxplus_dep_k(args.aggregation)
if dep_k is not None and not args.dependency_json:
    log_step("Warning: maxplus+depK selected but dependency_json not provided")
if args.dependency_json:
    log_step(f"Loading dependency graph: {args.dependency_json}")
    dep_graph_pos, dep_graph_neg = load_dependency_graph(
        Path(args.dependency_json),
        args.dep_threshold,
        disabled_rule_ids if disable_types else None,
    )
    log_step(f"Loaded dependency graph: pos={len(dep_graph_pos)}, neg={len(dep_graph_neg)}")
else:
    log_step(f"Dependency json not found: {args.dependency_json} (skip)")

entity_freq = None
if args.tie_handling == "frequency":
    train_path = Path(f"data/{dataset}/train.txt")
    if train_path.exists():
        entity_freq = load_entity_freq(train_path)

log_step("Loading applied_rules...")
with open(applied_rules_path, "r", encoding="utf-8") as f:
    applied_data = json.load(f)

head_applied = applied_data.get("head", {})
tail_applied = applied_data.get("tail", {})

log_step("Building rankings (head+tail)...")
headRanking, tailRanking, baseHeadRanking, baseTailRanking = build_scores_parallel(
    head_applied,
    tail_applied,
    rule_surprisal_map,
    rule_weight_map,
    dep_graph_pos,
    dep_graph_neg,
    entity_freq,
    args.tie_handling,
    args.aggregation,
    args.workers,
    args.chunksize,
)

head_rel, head_query, head_cand = _summarize_ranking(headRanking)
tail_rel, tail_query, tail_cand = _summarize_ranking(tailRanking)
print(
    "Head ranking: relations={0}, queries={1}, candidates={2}".format(
        head_rel, head_query, head_cand
    )
)
print(
    "Tail ranking: relations={0}, queries={1}, candidates={2}".format(
        tail_rel, tail_query, tail_cand
    )
)

if args.compare_eval_ranking:
    with open(args.compare_eval_ranking, "r", encoding="utf-8") as f:
        eval_rank = json.load(f)
    print("Comparing with eval ranking dump...")
    compare_rankings(headRanking, eval_rank.get("head", {}))
    compare_rankings(tailRanking, eval_rank.get("tail", {}))

log_step(f"Loading testset: {target}")
testset = TripleSet(target)
if baseHeadRanking is not None and baseTailRanking is not None:
    triples = load_triples_from_file(target)
    total, changed, improved, worsened, delta_mrr_sum, bucket_stats = compute_rank_change_stats(
        triples,
        baseHeadRanking,
        baseTailRanking,
        headRanking,
        tailRanking,
        head_applied,
        tail_applied,
    )
    print("[STAT] rank change vs base ranker")
    print(f"[STAT] total={total}")
    print(f"[STAT] changed={changed}")
    print(f"[STAT] improved={improved}")
    print(f"[STAT] worsened={worsened}")
    print(f"[STAT] delta_mrr_sum={delta_mrr_sum:.6f}")
    if bucket_stats is not None:
        print("[STAT] bucket stats by GT rule count")
        for label in ("≤5", "≤10", "≤20", "≤40", "≤80", "≤120", "120+"):
            stats = bucket_stats[label]
            print(
                "[STAT] bucket={0} improved={1} worsened={2} delta_pos={3:.6f} delta_neg={4:.6f}".format(
                    label,
                    int(stats["improved"]),
                    int(stats["worsened"]),
                    stats["delta_pos"],
                    stats["delta_neg"],
                )
            )
log_step("Scoring rankings...")
ranking = Ranking(k=100)
ranking.convert_handler_ranking(headRanking, tailRanking, testset)
ranking.compute_scores(testset.triples)

print("*** EVALUATION RESULTS ****")
print("Num triples: " + str(len(testset.triples)))
print("MRR     " + "{0:.6f}".format(ranking.hits.get_mrr()))
num_entities = get_num_entities(testset)
print("hits@1  " + "{0:.6f}".format(ranking.hits.get_hits_at_k(1)))
print("hits@3  " + "{0:.6f}".format(ranking.hits.get_hits_at_k(3)))
print("hits@10 " + "{0:.6f}".format(ranking.hits.get_hits_at_k(10)))
print()

print(
    "MRR "
    + "{0:.6f}".format(ranking.hits.get_mrr())
    + ", hits@1 "
    + "{0:.6f}".format(ranking.hits.get_hits_at_k(1))
    + ", hits@3 "
    + "{0:.6f}".format(ranking.hits.get_hits_at_k(3))
    + ", hits@10 "
    + "{0:.6f}".format(ranking.hits.get_hits_at_k(10))
)

end_time = datetime.now()
elapsed_time = end_time - start_time
print()
print(f"Evaluation completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total runtime: {elapsed_time}")

