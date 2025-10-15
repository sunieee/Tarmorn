#!/usr/bin/env python3
"""
规则比较脚本
比较两个规则文件，分析特定关系作为规则头的所有规则
"""

import re
import os
from typing import Set, List, Tuple, Dict
from collections import defaultdict

# 导入analysis_rule模块
from analysis_rule import load_dataset, analyze_rule_from_string

def parse_rule_line(line: str) -> Tuple[str, Dict, str]:
    """
    解析规则行
    返回: (完整规则, 指标字典, 原始行)
    """
    parts = line.strip().split('\t')
    if len(parts) < 4:
        return None, None, line
    
    try:
        count1 = int(parts[0])  # bodySize
        count2 = int(parts[1])  # support
        confidence = float(parts[2])
        rule = parts[3]
        
        # 构建指标字典
        metrics = {
            'bodySize': count1,
            'support': count2,
            'confidence': confidence
        }
        
        return rule, metrics, line
    except (ValueError, IndexError):
        pass
    
    return None, None, line

def load_rules_with_target_relation(file_path: str, target_relation: str) -> Dict[str, List[Tuple[str, Dict, str]]]:
    """
    加载文件中包含目标关系作为头部的所有规则
    返回: {标准化规则: [(原始规则, 指标字典, 原始行)]}
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
                
                rule, metrics, original_line = parse_rule_line(line)
                
                # 使用 rule.startswith(target_relation) 来判断
                if rule and rule.startswith(target_relation):
                    target_rule_count += 1
                    # 标准化规则（移除置信度差异，便于比较）
                    normalized_rule = normalize_rule(rule)
                    rules_dict[normalized_rule].append((rule, metrics, original_line.strip()))
            
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

def compare_rules(rules1: Dict[str, List], rules2: Dict[str, List], file1_name: str, file2_name: str, kg=None):
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
    
    topN = 5
    if common_rules:
        print(f"\n共同规则示例 (前{topN}个):")
        for i, rule in enumerate(list(common_rules)[:topN]):
            print(f"  {i+1}. {rule}")
            # 显示指标比较
            metrics1 = rules1[rule][0][1] if rules1[rule] else {}
            metrics2 = rules2[rule][0][1] if rules2[rule] else {}
            print(f"      {file1_name} 指标: {metrics1}")
            print(f"      {file2_name} 指标: {metrics2}")
            
            # 计算真实支持度
            if kg is not None:
                real_result = analyze_rule_from_string(rule, kg)
                if real_result:
                    print(f"      真实结果: {real_result['join_result']}")
            print()
    
    if only_in_1:
        print(f"\n仅在 {file1_name} 中的规则示例 (前{topN}个):")
        for i, rule in enumerate(list(only_in_1)[:topN]):
            print(f"  {i+1}. {rule}")
            metrics1 = rules1[rule][0][1] if rules1[rule] else {}
            print(f"      指标: {metrics1}")
            
            # 计算真实支持度
            if kg is not None:
                real_result = analyze_rule_from_string(rule, kg)
                if real_result:
                    print(f"      真实结果: {real_result['join_result']}")
            print()
    
    if only_in_2:
        print(f"\n仅在 {file2_name} 中的规则示例 (前{topN}个):")
        for i, rule in enumerate(list(only_in_2)[:topN]):
            print(f"  {i+1}. {rule}")
            metrics2 = rules2[rule][0][1] if rules2[rule] else {}
            print(f"      指标: {metrics2}")
            
            # 计算真实支持度
            if kg is not None:
                real_result = analyze_rule_from_string(rule, kg)
                if real_result:
                    print(f"      真实结果: {real_result['join_result']}")
            print()

def main(dataset="FB15k-237", file1_name="rules-100-filtered", file2_name="rule.txt"):
    """
    主函数
    """
    # 文件路径
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file1 = os.path.join(base_dir, "out", dataset, file1_name)
    file2 = os.path.join(base_dir, "out", dataset, file2_name)
    dataset_path = os.path.join(base_dir, "data", dataset, "train.txt")

    # 目标关系
    target_relation = "/award/award_category/winners./award/award_honor/ceremony"
    
    print("=== 规则文件比较工具 ===")
    print(f"比较文件:")
    print(f"  File 1: {file1}")
    print(f"  File 2: {file2}")
    print(f"  数据集: {dataset_path}")
    print(f"目标关系: {target_relation}")
    
    # 检查文件是否存在
    if not os.path.exists(file1):
        print(f"错误: 文件 {file1} 不存在")
        return
    
    if not os.path.exists(file2):
        print(f"错误: 文件 {file2} 不存在")
        return
    
    if not os.path.exists(dataset_path):
        print(f"错误: 数据集文件 {dataset_path} 不存在")
        return
    
    # 加载规则
    print(f"\n=== 加载规则 ===")
    rules1 = load_rules_with_target_relation(file1, target_relation)
    rules2 = load_rules_with_target_relation(file2, target_relation)
    
    # 加载知识图谱
    print(f"\n=== 加载知识图谱 ===")
    try:
        kg = load_dataset(dataset_path)
        print(f"知识图谱加载成功")
    except Exception as e:
        print(f"加载知识图谱失败: {e}")
        kg = None
    
    # 比较规则
    compare_rules(rules1, rules2, file1_name, file2_name, kg)

if __name__ == "__main__":
    main()