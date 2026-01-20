#!/usr/bin/env python3
"""
从单个log文件中提取以{"query":开头的行，
并根据rules与dependency文件补充ruleInfo和DepInfo。
"""

import json
import argparse
from functools import lru_cache
from pathlib import Path
from tqdm import tqdm


def extract_queries_from_log(log_file, rules_file, dependency_file, output_file):
    """
    从指定log文件中提取query行，并补充ruleInfo和DepInfo后保存到JSON文件。

    Args:
        log_file: log文件路径
        rules_file: rules文件路径
        dependency_file: dependency文件路径
        output_file: 输出的JSON文件路径
    """
    log_file = Path(log_file)
    rules_file = Path(rules_file)
    dependency_file = Path(dependency_file)

    seen_queries = set()
    results = []

    if not log_file.exists():
        print(f"错误: log文件不存在: {log_file}")
        return

    if not rules_file.exists():
        print(f"错误: rules文件不存在: {rules_file}")
        return

    if not dependency_file.exists():
        print(f"错误: dependency文件不存在: {dependency_file}")
        return

    print(f"处理log文件: {log_file.name}")

    def parse_rule_line(line: str):
        parts = line.split("\t", 3)
        if len(parts) < 4:
            return None
        try:
            return {
                "bodySize": int(parts[0]),
                "support": int(parts[1]),
                "score": parts[2],
                "rule": parts[3]
            }
        except ValueError:
            return None

    print("构建 dependency 索引...")
    dep_index = {}
    with open(dependency_file, 'r', encoding='utf-8') as f:
        for num, line in enumerate(f):
            if num % 1000000 == 0 and num > 0:
                print(f"  处理了 {num} 行 dependency 文件...")
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 8:
                continue
            try:
                id1 = int(parts[6])
                id2 = int(parts[7])
                conf1 = float(parts[4])
                conf2 = float(parts[5])
            except ValueError:
                continue
            if conf1 >= conf2:
                src_id, dst_id = id1, id2
                src_conf, dst_conf = conf1, conf2
            else:
                src_id, dst_id = id2, id1
                src_conf, dst_conf = conf2, conf1

            dep_index.setdefault(src_id, {})[dst_id] = {
                "bodySize": int(parts[0]),
                "supp": int(parts[1]),
                "conf": float(parts[2]),
                "lift": float(parts[3]),
                "conf1": src_conf,
                "conf2": dst_conf
            }

    
    print(f"  构建了 dependency 索引，包含 {len(dep_index)} 个规则ID")
    processed_count = 0
    rule_index = {}
    with open(rules_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            rule_index[line_num] = line.strip()

    valid_lines = []
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line.startswith('{"query":'):
                valid_lines.append(line)

    print("开始提取记录...")
    for line in tqdm(valid_lines):
        line = line.strip()
        data = json.loads(line)
        query = data.get("query", "")
        if query and query not in seen_queries:
            seen_queries.add(query)

            rule_ids = data.get("rules", [])
            rule_info = {}
            for rid in rule_ids:
                try:
                    rid_int = int(rid)
                except (TypeError, ValueError):
                    continue
                rule_line = rule_index.get(rid_int, "")
                parsed = parse_rule_line(rule_line)
                if parsed:
                    rule_info[str(rid_int)] = parsed

            rule_id_set = {int(r) for r in rule_ids if isinstance(r, int) or str(r).isdigit()}
            dep_info = {}
            for id1 in rule_id_set:
                if id1 not in dep_index:
                    continue
                for id2, info in dep_index[id1].items():
                    if id2 in rule_id_set:
                        dep_info.setdefault(str(id1), {})[str(id2)] = info

            data["ruleInfo"] = rule_info
            data["DepInfo"] = dep_info
            results.append(data)
            processed_count += 1

    print(f"  从该文件提取了 {processed_count} 条唯一记录")

    print(f"\n总共提取了 {len(results)} 条唯一记录")
    print(f"保存到: {output_file}")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('[\n')
        for i, record in enumerate(results):
            json_str = json.dumps(record, ensure_ascii=False)
            if i < len(results) - 1:
                f.write(f'{json_str},\n')
            else:
                f.write(f'{json_str}\n')
        f.write(']\n')

    print("完成！")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="从log中提取query并补充ruleInfo与DepInfo"
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
        "--log-file",
        default=None,
        help="log文件路径（默认: eval-negative.log）"
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

    args = parser.parse_args()

    base_dir = Path(args.out_dir) / args.dataset
    log_file_path = str(base_dir / (args.log_file or "eval-positive0.5.log"))
    rules_path = str(base_dir / (args.rules_file or "rules-100-3"))
    dependency_path = str(base_dir / (args.dependency_file or  "dependency.txt"))
    output_path = log_file_path.replace(".log", ".json")

    extract_queries_from_log(log_file_path, rules_path, dependency_path, output_path)
