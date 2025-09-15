#!/usr/bin/env python3
"""
规则比较脚本
比较两个规则文件，分析特定关系作为规则头的所有规则
"""

import re
import os
from typing import Set, List, Tuple, Dict
from collections import defaultdict

def parse_rule_line(line: str) -> Tuple[str, str, float, str]:
    """
    解析规则行
    返回: (头部关系, 完整规则, 置信度, 原始行)
    """
    parts = line.strip().split('\t')
    if len(parts) < 4:
        return None, None, 0.0, line
    
    try:
        count1 = int(parts[0])
        count2 = int(parts[1])
        confidence = float(parts[2])
        rule = parts[3]
        
        # 提取规则头部（<=左边的部分）
        if '<=' in rule:
            head_part = rule.split('<=')[0].strip()
            # 提取关系名（去掉变量和实体）
            match = re.match(r'([^(]+)', head_part)
            if match:
                relation = match.group(1).strip()
                return relation, rule, confidence, line
    except (ValueError, IndexError):
        pass
    
    return None, None, 0.0, line

def load_rules_with_target_relation(file_path: str, target_relation: str) -> Dict[str, List[Tuple[str, float, str]]]:
    """
    加载文件中包含目标关系作为头部的所有规则
    返回: {标准化规则: [(原始规则, 置信度, 原始行)]}
    """
    rules_dict = defaultdict(list)
    
    print(f"Loading rules from: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            line_count = 0
            target_rule_count = 0
            
            for line in f:
                line_count += 1
                if line_count % 100000 == 0:
                    print(f"  Processed {line_count} lines, found {target_rule_count} target rules")
                
                relation, rule, confidence, original_line = parse_rule_line(line)
                
                if relation and relation == target_relation:
                    target_rule_count += 1
                    # 标准化规则（移除置信度差异，便于比较）
                    normalized_rule = normalize_rule(rule)
                    rules_dict[normalized_rule].append((rule, confidence, original_line.strip()))
            
            print(f"  Total lines: {line_count}")
            print(f"  Found {target_rule_count} rules with target relation")
            print(f"  Unique normalized rules: {len(rules_dict)}")
            
    except FileNotFoundError:
        print(f"Error: File {file_path} not found")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    
    return dict(rules_dict)

def normalize_rule(rule: str) -> str:
    """
    标准化规则，移除变量绑定的差异，便于比较
    保留规则的结构和关系，但忽略具体的实体ID和变量名差异
    """
    # 简单的标准化：移除置信度相关的差异
    # 这里可以根据需要进一步完善标准化逻辑
    if '<=' in rule:
        head, body = rule.split('<=', 1)
        return f"{head.strip()} <= {body.strip()}"
    return rule

def compare_rules(rules1: Dict[str, List], rules2: Dict[str, List], file1_name: str, file2_name: str):
    """
    比较两个规则集合
    """
    print(f"\n=== 规则比较分析 ===")
    print(f"文件1: {file1_name}")
    print(f"文件2: {file2_name}")
    print(f"目标关系: /award/award_category/winners./award/award_honor/ceremony")
    
    set1 = set(rules1.keys())
    set2 = set(rules2.keys())
    
    print(f"\n=== 基本统计 ===")
    print(f"{file1_name} 中的规则数量: {len(set1)}")
    print(f"{file2_name} 中的规则数量: {len(set2)}")
    
    # 交集
    common_rules = set1 & set2
    print(f"共同规则数量: {len(common_rules)}")
    
    # 差集
    only_in_1 = set1 - set2
    only_in_2 = set2 - set1
    
    print(f"仅在 {file1_name} 中的规则数量: {len(only_in_1)}")
    print(f"仅在 {file2_name} 中的规则数量: {len(only_in_2)}")
    
    # 检查file2是否包含file1中的所有规则
    coverage = len(common_rules) / len(set1) if len(set1) > 0 else 0
    print(f"\n=== 覆盖率分析 ===")
    print(f"{file2_name} 包含 {file1_name} 中规则的比例: {coverage:.2%}")
    
    if coverage < 1.0:
        print(f"\n{file2_name} 未完全包含 {file1_name} 中的所有规则")
        print(f"缺失的规则数量: {len(only_in_1)}")
    else:
        print(f"\n{file2_name} 完全包含了 {file1_name} 中的所有规则")
    
    # 显示详细信息
    print(f"\n=== 详细分析 ===")
    
    if common_rules:
        print(f"\n共同规则示例 (前5个):")
        for i, rule in enumerate(list(common_rules)[:5]):
            print(f"  {i+1}. {rule}")
            # 显示置信度比较
            conf1 = rules1[rule][0][1] if rules1[rule] else "N/A"
            conf2 = rules2[rule][0][1] if rules2[rule] else "N/A"
            print(f"      {file1_name} 置信度: {conf1}")
            print(f"      {file2_name} 置信度: {conf2}")
    
    if only_in_1:
        print(f"\n仅在 {file1_name} 中的规则示例 (前5个):")
        for i, rule in enumerate(list(only_in_1)[:5]):
            print(f"  {i+1}. {rule}")
            conf1 = rules1[rule][0][1] if rules1[rule] else "N/A"
            print(f"      置信度: {conf1}")
    
    if only_in_2:
        print(f"\n仅在 {file2_name} 中的规则示例 (前5个):")
        for i, rule in enumerate(list(only_in_2)[:5]):
            print(f"  {i+1}. {rule}")
            conf2 = rules2[rule][0][1] if rules2[rule] else "N/A"
            print(f"      置信度: {conf2}")

def main():
    """
    主函数
    """
    # 文件路径
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file1 = os.path.join(base_dir, "out", "FB15k-237", "rules-10")
    file2 = os.path.join(base_dir, "out", "FB15k-237", "rule.txt")
    
    # 目标关系
    target_relation = "/award/award_category/winners./award/award_honor/ceremony"
    
    print("=== 规则文件比较工具 ===")
    print(f"比较文件:")
    print(f"  File 1: {file1}")
    print(f"  File 2: {file2}")
    print(f"目标关系: {target_relation}")
    
    # 检查文件是否存在
    if not os.path.exists(file1):
        print(f"错误: 文件 {file1} 不存在")
        return
    
    if not os.path.exists(file2):
        print(f"错误: 文件 {file2} 不存在")
        return
    
    # 加载规则
    print(f"\n=== 加载规则 ===")
    rules1 = load_rules_with_target_relation(file1, target_relation)
    rules2 = load_rules_with_target_relation(file2, target_relation)
    
    # 比较规则
    compare_rules(rules1, rules2, "rules-10", "rule.txt")

if __name__ == "__main__":
    main()