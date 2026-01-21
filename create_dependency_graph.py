#!/usr/bin/env python3
"""
从 applied_rules.json 构建 dependency_graph.csv。
"""

import argparse
import csv
import json
import math
from pathlib import Path
from statistics import pstdev


NUM_UNSEEN_DEFAULT = 5


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
        "rule": parts[3]
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


def load_rules(rules_path: Path, num_unseen: int):
    rule_info = {}
    with open(rules_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            parsed = parse_rule_line(line.strip())
            if not parsed:
                continue
            parsed["surprisal"] = rule_surprisal(
                parsed["bodySize"], parsed["support"], num_unseen
            )
            rule_info[line_num] = parsed
    return rule_info


def load_dependency(dependency_path: Path):
    dep_index = {}
    with open(dependency_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 8:
                continue
            try:
                body_size = int(parts[0])
                supp = int(parts[1])
                conf = float(parts[2])
                lift = float(parts[3])
                conf1 = float(parts[4])
                conf2 = float(parts[5])
                id1 = int(parts[6])
                id2 = int(parts[7])
            except ValueError:
                continue

            if conf1 >= conf2:
                src_id, dst_id = id1, id2
                src_conf, dst_conf = conf1, conf2
            else:
                src_id, dst_id = id2, id1
                src_conf, dst_conf = conf2, conf1

            dep_index.setdefault(src_id, {})[dst_id] = {
                "bodySize": body_size,
                "supp": supp,
                "conf": conf,
                "lift": lift,
                "conf1": src_conf,
                "conf2": dst_conf
            }
    return dep_index


def load_test_set(test_path: Path):
    test_set = set()
    with open(test_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 3:
                continue
            h, r, t = parts[0], parts[1], parts[2]
            test_set.add((h, r, t))
    return test_set


def percentile(sorted_values, p: float):
    if not sorted_values:
        return 0.0
    idx = int((len(sorted_values) - 1) * p)
    return sorted_values[idx]


def build_components(nodes, edges):
    if not edges:
        return 0, 0
    adj = {n: set() for n in nodes}
    for u, v in edges:
        if u not in adj:
            adj[u] = set()
        if v not in adj:
            adj[v] = set()
        adj[u].add(v)
        adj[v].add(u)
    visited = set()
    num_components = 0
    largest_size = 0
    for n in adj:
        if n in visited:
            continue
        stack = [n]
        visited.add(n)
        size = 0
        while stack:
            cur = stack.pop()
            size += 1
            for nxt in adj[cur]:
                if nxt not in visited:
                    visited.add(nxt)
                    stack.append(nxt)
        num_components += 1
        largest_size = max(largest_size, size)
    return num_components, largest_size


def compute_features(rule_ids, rule_info, dep_index):
    rule_ids = [rid for rid in rule_ids if rid in rule_info]
    num_rules = len(rule_ids)

    if num_rules == 0:
        return {
            "num_rules": 0,
            "w_1": 0.0,
            "w_2": 0.0,
            "w_3": 0.0,
            "w_4": 0.0,
            "w_5": 0.0,
            "w25": 0.0,
            "w50": 0.0,
            "w75": 0.0,
            "w_std": 0.0,
            "num_neg_edges": 0,
            "num_rules_with_neg_incoming": 0,
            "num_rules_with_neg_outgoing": 0,
            "max_neg_indegree": 0,
            "mean_neg_indegree": 0.0,
            "max_neg_outdegree": 0,
            "num_neg_components": 0,
            "num_neg_component": 0,
            "largest_neg_component_size": 0,
            "num_pos_edges": 0,
            "num_rules_with_pos_edges": 0,
            "num_pos_components": 0,
            "largest_pos_component_size": 0,
            "max_neg_outdegree_minus_indegree": 0,
            "max_pos_outdegree": 0,
            "outdegree_of_top_rule": 0,
            "indegree_of_top_rule": 0,
            "score_noisyor": 0.0,
            "score_maxplus": 0.0,
            "score_expdecay_tau_0.25": 0.0,
            "score_expdecay_tau_0.5": 0.0,
            "score_expdecay_tau_1": 0.0,
            "score_expdecay_tau_2": 0.0,
            "score_expdecay_tau_4": 0.0,
            "num_lift_pos_gt_1": 0,
            "num_lift_neg_gt_0.5": 0,
            "max_lift_neg": 0.0,
            "max_lift_pos": 0.0,
            "sum_lift_pos": 0.0,
            "sum_lift_neg": 0.0,
            "sum_top3_lift_pos": 0.0,
            "sum_top3_lift_neg": 0.0,
        }

    surprisals = [(rid, rule_info[rid]["surprisal"]) for rid in rule_ids]
    surprisals.sort(key=lambda x: x[1], reverse=True)
    sorted_surprisal_values = [s for _, s in surprisals]

    w_values = sorted_surprisal_values + [0.0] * 5
    w_1, w_2, w_3, w_4, w_5 = w_values[:5]

    w25 = percentile(sorted_surprisal_values, 0.25)
    w50 = percentile(sorted_surprisal_values, 0.50)
    w75 = percentile(sorted_surprisal_values, 0.75)
    w_std = pstdev(sorted_surprisal_values) if len(sorted_surprisal_values) > 1 else 0.0

    rule_id_set = set(rule_ids)
    neg_edges = []
    pos_edges = []
    neg_in = {rid: 0 for rid in rule_id_set}
    neg_out = {rid: 0 for rid in rule_id_set}
    pos_in = {rid: 0 for rid in rule_id_set}
    pos_out = {rid: 0 for rid in rule_id_set}
    pos_lifts = []
    neg_lifts = []

    for src in rule_id_set:
        if src not in dep_index:
            continue
        for dst, info in dep_index[src].items():
            if dst not in rule_id_set:
                continue
            lift = info.get("lift", 0.0)
            if lift < 0:
                neg_edges.append((src, dst))
                neg_out[src] += 1
                neg_in[dst] += 1
                neg_lifts.append(lift)
            else:
                pos_edges.append((src, dst))
                pos_out[src] += 1
                pos_in[dst] += 1
                pos_lifts.append(lift)

    num_neg_edges = len(neg_edges)
    num_pos_edges = len(pos_edges)
    num_rules_with_neg_incoming = sum(1 for v in neg_in.values() if v > 0)
    num_rules_with_neg_outgoing = sum(1 for v in neg_out.values() if v > 0)
    max_neg_indegree = max(neg_in.values()) if neg_in else 0
    mean_neg_indegree = sum(neg_in.values()) / len(neg_in) if neg_in else 0.0
    max_neg_outdegree = max(neg_out.values()) if neg_out else 0
    num_rules_with_pos_edges = sum(1 for r in rule_id_set if pos_in.get(r, 0) + pos_out.get(r, 0) > 0)

    neg_nodes = {u for edge in neg_edges for u in edge}
    pos_nodes = {u for edge in pos_edges for u in edge}
    num_neg_components, largest_neg_component_size = build_components(neg_nodes, neg_edges)
    num_pos_components, largest_pos_component_size = build_components(pos_nodes, pos_edges)

    max_neg_outdegree_minus_indegree = max(
        (neg_out[r] - neg_in[r] for r in rule_id_set),
        default=0
    )
    max_pos_outdegree = max(pos_out.values()) if pos_out else 0

    top_rule_id = surprisals[0][0]
    outdegree_of_top_rule = pos_out.get(top_rule_id, 0) + neg_out.get(top_rule_id, 0)
    indegree_of_top_rule = pos_in.get(top_rule_id, 0) + neg_in.get(top_rule_id, 0)

    sum_surprisal = sum(sorted_surprisal_values)
    score_noisyor = 1.0 - math.exp(-sum_surprisal) if sum_surprisal > 0 else 0.0
    score_maxplus = sorted_surprisal_values[0] if sorted_surprisal_values else 0.0

    def expdecay_score(tau: float):
        return sum(s * math.exp(-tau * idx) for idx, s in enumerate(sorted_surprisal_values))

    score_expdecay_tau_0_25 = expdecay_score(0.25)
    score_expdecay_tau_0_5 = expdecay_score(0.5)
    score_expdecay_tau_1 = expdecay_score(1.0)
    score_expdecay_tau_2 = expdecay_score(2.0)
    score_expdecay_tau_4 = expdecay_score(4.0)

    num_lift_pos_gt_1 = sum(1 for v in pos_lifts if abs(v) >= 1.0)
    num_lift_neg_gt_0_5 = sum(1 for v in neg_lifts if abs(v) >= 0.5)
    max_lift_pos = max(pos_lifts) if pos_lifts else 0.0
    max_lift_neg = max((abs(v) for v in neg_lifts), default=0.0)
    sum_lift_pos = sum(pos_lifts) if pos_lifts else 0.0
    sum_lift_neg = sum(neg_lifts) if neg_lifts else 0.0
    sum_top3_lift_pos = sum(sorted(pos_lifts, reverse=True)[:3]) if pos_lifts else 0.0
    sum_top3_lift_neg = sum(sorted((abs(v) for v in neg_lifts), reverse=True)[:3]) if neg_lifts else 0.0

    return {
        "num_rules": num_rules,
        "w_1": w_1,
        "w_2": w_2,
        "w_3": w_3,
        "w_4": w_4,
        "w_5": w_5,
        "w25": w25,
        "w50": w50,
        "w75": w75,
        "w_std": w_std,
        "num_neg_edges": num_neg_edges,
        "num_rules_with_neg_incoming": num_rules_with_neg_incoming,
        "num_rules_with_neg_outgoing": num_rules_with_neg_outgoing,
        "max_neg_indegree": max_neg_indegree,
        "mean_neg_indegree": mean_neg_indegree,
        "max_neg_outdegree": max_neg_outdegree,
        "num_neg_components": num_neg_components,
        "num_neg_component": num_neg_components,
        "largest_neg_component_size": largest_neg_component_size,
        "num_pos_edges": num_pos_edges,
        "num_rules_with_pos_edges": num_rules_with_pos_edges,
        "num_pos_components": num_pos_components,
        "largest_pos_component_size": largest_pos_component_size,
        "max_neg_outdegree_minus_indegree": max_neg_outdegree_minus_indegree,
        "max_pos_outdegree": max_pos_outdegree,
        "outdegree_of_top_rule": outdegree_of_top_rule,
        "indegree_of_top_rule": indegree_of_top_rule,
        "score_noisyor": score_noisyor,
        "score_maxplus": score_maxplus,
        "score_expdecay_tau_0.25": score_expdecay_tau_0_25,
        "score_expdecay_tau_0.5": score_expdecay_tau_0_5,
        "score_expdecay_tau_1": score_expdecay_tau_1,
        "score_expdecay_tau_2": score_expdecay_tau_2,
        "score_expdecay_tau_4": score_expdecay_tau_4,
        "num_lift_pos_gt_1": num_lift_pos_gt_1,
        "num_lift_neg_gt_0.5": num_lift_neg_gt_0_5,
        "max_lift_neg": max_lift_neg,
        "max_lift_pos": max_lift_pos,
        "sum_lift_pos": sum_lift_pos,
        "sum_lift_neg": sum_lift_neg,
        "sum_top3_lift_pos": sum_top3_lift_pos,
        "sum_top3_lift_neg": sum_top3_lift_neg,
    }


def build_dependency_graph(
    applied_rules_path: Path,
    rules_path: Path,
    dependency_path: Path,
    test_path: Path,
    output_path: Path,
    num_unseen: int,
):
    print("开始构建 dependency_graph.csv")
    print(f"applied_rules: {applied_rules_path}")
    print(f"rules: {rules_path}")
    print(f"dependency: {dependency_path}")
    print(f"test: {test_path}")
    print(f"output: {output_path}")

    if not applied_rules_path.exists():
        print(f"错误: applied_rules.json 不存在: {applied_rules_path}")
        return
    if not rules_path.exists():
        print(f"错误: rules 文件不存在: {rules_path}")
        return
    if not dependency_path.exists():
        print(f"错误: dependency 文件不存在: {dependency_path}")
        return
    if not test_path.exists():
        print(f"错误: test 文件不存在: {test_path}")
        return

    print("加载规则列表...")
    rule_info = load_rules(rules_path, num_unseen)
    print(f"  规则数量: {len(rule_info)}")

    print("加载依赖列表...")
    dep_index = load_dependency(dependency_path)
    print(f"  依赖节点数: {len(dep_index)}")

    print("加载 test 集...")
    test_set = load_test_set(test_path)
    print(f"  test 三元组数量: {len(test_set)}")

    print("读取 applied_rules.json...")
    with open(applied_rules_path, "r", encoding="utf-8") as f:
        applied_data = json.load(f)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    header = [
        "relation",
        "constant",
        "candidate",
        "label",
        "if_head",
        "num_rules",
        "w_1",
        "w_2",
        "w_3",
        "w_4",
        "w_5",
        "w25",
        "w50",
        "w75",
        "w_std",
        "num_neg_edges",
        "num_rules_with_neg_incoming",
        "num_rules_with_neg_outgoing",
        "max_neg_indegree",
        "mean_neg_indegree",
        "max_neg_outdegree",
        "num_neg_components",
        "num_neg_component",
        "largest_neg_component_size",
        "num_pos_edges",
        "num_rules_with_pos_edges",
        "num_pos_components",
        "largest_pos_component_size",
        "max_neg_outdegree_minus_indegree",
        "max_pos_outdegree",
        "outdegree_of_top_rule",
        "indegree_of_top_rule",
        "score_noisyor",
        "score_maxplus",
        "score_expdecay_tau_0.25",
        "score_expdecay_tau_0.5",
        "score_expdecay_tau_1",
        "score_expdecay_tau_2",
        "score_expdecay_tau_4",
        "num_lift_pos_gt_1",
        "num_lift_neg_gt_0.5",
        "max_lift_neg",
        "max_lift_pos",
        "sum_lift_pos",
        "sum_lift_neg",
        "sum_top3_lift_pos",
        "sum_top3_lift_neg",
    ]

    print("开始写入 CSV...")
    total_rows = 0
    with open(output_path, "w", encoding="utf-8", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()

        for if_head_key, if_head_value in ("head", 1), ("tail", 0):
            section = applied_data.get(if_head_key, {})
            print(f"处理分支: {if_head_key} (records: {len(section)})")
            for relation, constant_dict in section.items():
                for constant, candidate_dict in constant_dict.items():
                    for candidate, rule_ids in candidate_dict.items():
                        rule_id_list = []
                        for rid in rule_ids:
                            try:
                                rule_id_list.append(int(rid))
                            except (TypeError, ValueError):
                                continue

                        if if_head_value == 1:
                            triple = (str(candidate), str(relation), str(constant))
                        else:
                            triple = (str(constant), str(relation), str(candidate))
                        label = 1 if triple in test_set else 0

                        features = compute_features(rule_id_list, rule_info, dep_index)
                        row = {
                            "relation": relation,
                            "constant": constant,
                            "candidate": candidate,
                            "label": label,
                            "if_head": if_head_value,
                        }
                        row.update(features)
                        writer.writerow(row)
                        total_rows += 1

    print(f"写入完成，总行数: {total_rows}")
    print(f"保存到: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="从 applied_rules.json 构建 dependency_graph.csv"
    )
    parser.add_argument(
        "--dataset",
        default="FB15k-237",
        help="数据集名称，用于拼接默认路径（默认: FB15k-237）"
    )
    parser.add_argument(
        "--out-dir",
        default="out",
        help="输出目录根路径（默认: out）"
    )
    parser.add_argument(
        "--rules-file",
        default=None,
        help="rules文件路径（默认: rules-100-3）"
    )
    parser.add_argument(
        "--dependency-file",
        default=None,
        help="dependency文件路径（默认: dependency.txt）"
    )
    parser.add_argument(
        "--applied-rules-file",
        default=None,
        help="applied_rules.json 文件路径（默认: applied_rules.json）"
    )
    parser.add_argument(
        "--test-file",
        default=None,
        help="test 文件路径（默认: data/<dataset>/test.txt）"
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help="输出 CSV 文件路径（默认: out/<dataset>/dependency_graph.csv）"
    )
    parser.add_argument(
        "--num-unseen",
        type=int,
        default=NUM_UNSEEN_DEFAULT,
        help=f"NUM_UNSEEN 默认值（默认: {NUM_UNSEEN_DEFAULT}）"
    )

    args = parser.parse_args()

    base_dir = Path(args.out_dir) / args.dataset
    rules_path = Path(args.rules_file or (base_dir / "rules-100-3"))
    dependency_path = Path(args.dependency_file or (base_dir / "dependency.txt"))
    applied_rules_path = Path(args.applied_rules_file or (base_dir / "applied_rules.json"))
    test_path = Path(args.test_file or (Path("data") / args.dataset / "test.txt"))
    output_path = Path(args.output_file or (base_dir / "dependency_graph.csv"))

    build_dependency_graph(
        applied_rules_path=applied_rules_path,
        rules_path=rules_path,
        dependency_path=dependency_path,
        test_path=test_path,
        output_path=output_path,
        num_unseen=args.num_unseen,
    )
