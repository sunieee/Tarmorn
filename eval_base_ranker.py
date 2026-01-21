import argparse
import json
import math
import os
import re
from datetime import datetime
from pathlib import Path

from clause import Ranking
from clause import TripleSet


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


def build_scores(
    applied_rules: dict,
    rule_surprisal_map: dict[int, float],
    entity_freq: dict[str, int] | None,
    tie_handling: str,
) -> dict:
    result: dict = {}
    for relation, constant_dict in applied_rules.items():
        rel_bucket = result.setdefault(relation, {})
        for constant, candidate_dict in constant_dict.items():
            pairs: list[tuple[str, float]] = []
            for candidate, rule_ids in candidate_dict.items():
                sum_surprisal = 0.0
                for rid in rule_ids:
                    rule_id = _safe_rule_id(rid)
                    if rule_id is None:
                        continue
                    sum_surprisal += rule_surprisal_map.get(rule_id, 0.0)
                pairs.append((str(candidate), float(sum_surprisal)))
            if tie_handling == "frequency" and entity_freq is not None:
                pairs.sort(
                    key=lambda x: (-x[1], -entity_freq.get(x[0], 0), x[0])
                )
            else:
                pairs.sort(key=lambda x: (-x[1], x[0]))
            pairs = [(c, 1.0 - math.exp(-s) if s > 0 else 0.0) for c, s in pairs]
            rel_bucket[str(constant)] = pairs
    return result


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

    topk=20
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


def main():
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

    args = argparser.parse_args()
    start_time = datetime.now()

    dataset = args.dataset
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

    rule_surprisal_map = load_rule_surprisals(
        rules_path,
        args.num_unseen,
        args.d_weight,
        args.z_weight,
        args.use_rule_confidence
    )

    entity_freq = None
    if args.tie_handling == "frequency":
        train_path = Path(f"data/{dataset}/train.txt")
        if train_path.exists():
            entity_freq = load_entity_freq(train_path)

    with open(applied_rules_path, "r", encoding="utf-8") as f:
        applied_data = json.load(f)

    head_applied = applied_data.get("head", {})
    tail_applied = applied_data.get("tail", {})

    headRanking = build_scores(head_applied, rule_surprisal_map, entity_freq, args.tie_handling)
    tailRanking = build_scores(tail_applied, rule_surprisal_map, entity_freq, args.tie_handling)

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

    testset = TripleSet(target)
    ranking = Ranking(k=100)
    ranking.convert_handler_ranking(headRanking, tailRanking, testset)
    ranking.compute_scores(testset.triples)

    print("*** EVALUATION RESULTS ****")
    print("Num triples: " + str(len(testset.triples)))
    print("MRR     " + "{0:.6f}".format(ranking.hits.get_mrr()))
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


if __name__ == "__main__":
    main()
