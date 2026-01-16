#!/usr/bin/env python3
"""
Complex Rules验证脚本
从rule.txt中加载complex rules，采样并验证其置信度是否正确
set PYTHONIOENCODING=utf-8
"""

import os
import sys
import random
from typing import List, Dict, Tuple

# 导入analysis_rule模块
from analysis_rule import KnowledgeGraph, RuleParser, RuleSupportCalculator


def load_dataset(filepath: str) -> KnowledgeGraph:
    """加载数据集到知识图谱"""
    kg = KnowledgeGraph()
    
    print(f"正在加载数据集: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"数据集文件不存在: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) != 3:
                print(f"警告：第{line_num}行格式错误: {line}")
                continue
            
            head, relation, tail = parts
            kg.add_triple(head, relation, tail)
            
            if line_num % 50000 == 0:
                print(f"已加载 {line_num:,} 个三元组...")
    
    print(f"数据集加载完成:")
    print(f"  三元组数量: {len(kg.triples):,}")
    print(f"  实体数量: {kg._next_entity_id:,}")
    print(f"  原始关系数量: {len([r for r in kg.relations if not r.startswith('INVERSE_')]):,}")
    print(f"  总关系数量（含逆关系）: {len(kg.relations):,}")
    
    return kg


def load_rules_from_file(filepath: str) -> List[Dict]:
    """
    从rule.txt文件中加载规则
    
    文件格式：
    bodySize\tsupport\tconfidence\trule_string
    
    Returns:
        规则列表，每个规则包含：{
            'bodySize': int,
            'support': int,
            'confidence': float,
            'rule_string': str
        }
    """
    rules = []
    
    print(f"\n正在加载规则文件: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"规则文件不存在: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) != 4:
                print(f"警告：第{line_num}行格式错误: {line}")
                continue
            
            try:
                bodySize = int(parts[0])
                support = int(parts[1])
                confidence = float(parts[2])
                rule_string = parts[3]
                
                rules.append({
                    'bodySize': bodySize,
                    'support': support,
                    'confidence': confidence,
                    'rule_string': rule_string,
                    'line_num': line_num
                })
            except ValueError as e:
                print(f"警告：第{line_num}行解析错误: {e}")
                continue
    
    print(f"加载了 {len(rules)} 条规则")
    return rules


def is_complex_rule(rule_string: str) -> bool:
    """判断是否是complex rule（body包含分号）"""
    if '<=' not in rule_string:
        return False
    
    _, body_part = rule_string.split('<=', 1)
    return ';' in body_part


def classify_rule(rule_string: str) -> str:
    """
    分类规则类型：unary或binary
    
    通过解析规则的head部分判断
    """
    if '<=' not in rule_string:
        return 'unknown'
    
    head_part, _ = rule_string.split('<=', 1)
    head_part = head_part.strip()
    
    # 检查head是否包含变量
    if '(' in head_part and ')' in head_part:
        paren_content = head_part.split('(')[1].split(')')[0]
        # 如果括号中只有一个参数（不含逗号），是一元规则
        if ',' not in paren_content:
            return 'unary'
        else:
            # 有逗号，检查是否有两个变量（二元）还是一个变量一个常量（一元）
            args = [arg.strip() for arg in paren_content.split(',')]
            # 统计单字母变量（真正的变量）的数量
            var_count = sum(1 for arg in args if len(arg) == 1)
            return 'unary' if var_count == 1 else 'binary'
    else:
        # 没有括号，是简写的二元规则
        return 'binary'


def sample_rules(rules: List[Dict], n_unary: int = 50, n_binary: int = 50, 
                 is_complex: bool = True) -> Tuple[List[Dict], List[Dict]]:
    """
    从规则列表中采样rules
    
    Args:
        rules: 规则列表
        n_unary: 采样的一元规则数量
        n_binary: 采样的二元规则数量
        is_complex: True表示采样complex rules，False表示采样normal rules
        
    Returns:
        (unary_samples, binary_samples)
    """
    rule_type_name = "complex" if is_complex else "normal"
    
    # 筛选对应类型的rules
    if is_complex:
        filtered_rules = [rule for rule in rules if is_complex_rule(rule['rule_string'])]
    else:
        filtered_rules = [rule for rule in rules if not is_complex_rule(rule['rule_string'])]
    
    print(f"\n找到 {len(filtered_rules)} 条{rule_type_name} rules")
    
    # 按类型分类
    unary_rules = [rule for rule in filtered_rules if classify_rule(rule['rule_string']) == 'unary']
    binary_rules = [rule for rule in filtered_rules if classify_rule(rule['rule_string']) == 'binary']
    
    print(f"  一元{rule_type_name} rules: {len(unary_rules)}")
    print(f"  二元{rule_type_name} rules: {len(binary_rules)}")
    
    # 采样
    unary_samples = random.sample(unary_rules, min(n_unary, len(unary_rules)))
    binary_samples = random.sample(binary_rules, min(n_binary, len(binary_rules)))
    
    # 标记规则类型
    for rule in unary_samples:
        rule['is_complex'] = is_complex
        rule['rule_category'] = f"{rule_type_name} unary"
    
    for rule in binary_samples:
        rule['is_complex'] = is_complex
        rule['rule_category'] = f"{rule_type_name} binary"
    
    print(f"\n采样结果:")
    print(f"  一元规则: {len(unary_samples)}")
    print(f"  二元规则: {len(binary_samples)}")
    
    return unary_samples, binary_samples


def verify_rule(rule: Dict, kg: KnowledgeGraph, debug_instances: bool = False) -> Dict:
    """
    验证单个规则的置信度
    
    Args:
        debug_instances: 是否打印详细的instance信息（用于调试）
    
    Returns:
        验证结果字典，包含：{
            'rule_string': str,
            'expected_bodySize': int,
            'expected_support': int,
            'expected_confidence': float,
            'actual_bodySize': int,
            'actual_support': int,
            'actual_confidence': float,
            'bodySize_match': bool,
            'support_match': bool,
            'confidence_match': bool,
            'confidence_diff': float,
            'has_error': bool
        }
    """
    rule_string = rule['rule_string']
    expected_bodySize = rule['bodySize']
    expected_support = rule['support']
    expected_confidence = rule['confidence']
    
    try:
        # 解析规则
        head_relation, body_relations, variable_count, rule_info = RuleParser.parse_rule(rule_string)
        
        # 创建计算器
        calculator = RuleSupportCalculator(kg)
        
        # 获取head和body instances用于详细调试
        head_instances = calculator._get_head_instances(rule_info)
        body_instances = calculator._get_body_instances(rule_info)
        
        # 计算实际指标
        result = calculator.calculate_rule_support_join(rule_info)
        
        actual_headSize = result['headSize']
        actual_bodySize = result['bodySize']
        actual_support = result['support']
        actual_confidence = result['confidence']
        
        # 如果是一元规则且需要调试，打印详细信息
        if debug_instances and variable_count == 1:
            print(f"\n  [DEBUG] 一元规则详细分析:")
            print(f"  [DEBUG] 规范化规则: {rule_info.get('normalized_rule', 'N/A')}")
            print(f"  [DEBUG] Head relation: {rule_info.get('head_relation')}")
            print(f"  [DEBUG] Head constant: {rule_info.get('head_constant')}")
            print(f"  [DEBUG] Head instances数量: {len(head_instances)}")
            
            # 打印head instances样例
            if head_instances:
                head_sample = list(head_instances)[:10]
                print(f"  [DEBUG] Head instances样例 (前10个):")
                for entity_id in head_sample:
                    entity_str = kg.get_entity_str(entity_id)
                    print(f"    - {entity_str} (id={entity_id})")
            
            print(f"\n  [DEBUG] Body instances数量: {len(body_instances)}")
            
            # 打印body instances样例
            if body_instances:
                body_sample = list(body_instances)[:10]
                print(f"  [DEBUG] Body instances样例 (前10个):")
                for entity_id in body_sample:
                    entity_str = kg.get_entity_str(entity_id)
                    print(f"    - {entity_str} (id={entity_id})")
            
            # 检查交集
            if head_instances and body_instances:
                intersection = head_instances.intersection(body_instances)
                print(f"\n  [DEBUG] 交集instances数量: {len(intersection)}")
                if intersection:
                    intersection_sample = list(intersection)[:10]
                    print(f"  [DEBUG] 交集instances样例 (前10个):")
                    for entity_id in intersection_sample:
                        entity_str = kg.get_entity_str(entity_id)
                        print(f"    - {entity_str} (id={entity_id})")
                else:
                    print(f"  [DEBUG] 没有交集！检查是否有body instances在head中:")
                    # 检查前几个body instances是否在head中
                    body_sample = list(body_instances)[:5]
                    for entity_id in body_sample:
                        entity_str = kg.get_entity_str(entity_id)
                        in_head = entity_id in head_instances
                        print(f"    - {entity_str} (id={entity_id}): {'在head中' if in_head else '不在head中'}")
        
        # 计算差异
        confidence_diff = abs(actual_confidence - expected_confidence)
        
        # 判断是否匹配（置信度允许±10%的误差）
        confidence_match = confidence_diff <= 0.1
        bodySize_match = (actual_bodySize == expected_bodySize)
        support_match = (actual_support == expected_support)
        
        has_error = not (confidence_match and bodySize_match and support_match)
        
        return {
            'rule_string': rule_string,
            'normalized_rule': rule_info.get('normalized_rule', rule_string),
            'line_num': rule.get('line_num', 0),
            'variable_count': variable_count,
            'expected_bodySize': expected_bodySize,
            'expected_support': expected_support,
            'expected_confidence': expected_confidence,
            'actual_headSize': actual_headSize,
            'actual_bodySize': actual_bodySize,
            'actual_support': actual_support,
            'actual_confidence': actual_confidence,
            'bodySize_match': bodySize_match,
            'support_match': support_match,
            'confidence_match': confidence_match,
            'confidence_diff': confidence_diff,
            'has_error': has_error,
            'rule_category': rule.get('rule_category', 'unknown')
        }
        
    except Exception as e:
        print(f"  错误：验证规则失败 - {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'rule_string': rule_string,
            'normalized_rule': rule_string,
            'line_num': rule.get('line_num', 0),
            'variable_count': 0,
            'expected_bodySize': expected_bodySize,
            'expected_support': expected_support,
            'expected_confidence': expected_confidence,
            'actual_headSize': 0,
            'actual_bodySize': 0,
            'actual_support': 0,
            'actual_confidence': 0.0,
            'bodySize_match': False,
            'support_match': False,
            'confidence_match': False,
            'confidence_diff': 1.0,
            'has_error': True,
            'error': str(e),
            'rule_category': rule.get('rule_category', 'unknown')
        }


def verify_rules(rules: List[Dict], kg: KnowledgeGraph, rule_type: str, 
                 enable_debug: bool = False) -> List[Dict]:
    """
    验证一组规则
    
    Args:
        rules: 规则列表
        kg: 知识图谱
        rule_type: 规则类型（'unary' 或 'binary'）
        enable_debug: 是否启用详细调试（默认False，只对前3个规则启用）
        
    Returns:
        验证结果列表
    """
    results = []
    
    print(f"\n开始验证{len(rules)}条{rule_type}规则...")
    
    for i, rule in enumerate(rules, 1):
        print(f"\n{'='*100}")
        print(f"[{i}/{len(rules)}] 验证规则 (行{rule.get('line_num', 0)}) - {rule.get('rule_category', 'unknown')}")
        print(f"  原始规则: {rule['rule_string']}")
        print(f"  期望: bodySize={rule['bodySize']}, support={rule['support']}, confidence={rule['confidence']:.4f}")
        
        # 对于unary规则，只对前3个启用详细调试
        debug_instances = (rule_type == 'unary' and enable_debug and i <= 3)
        
        result = verify_rule(rule, kg, debug_instances=debug_instances)
        results.append(result)
        
        if 'error' not in result:
            print(f"\n  简写规则: {result['normalized_rule']}")
            print(f"  实际: headSize={result.get('actual_headSize', 'N/A')}, bodySize={result['actual_bodySize']}, support={result['actual_support']}, confidence={result['actual_confidence']:.4f}")
            print(f"  匹配: bodySize={result['bodySize_match']}, support={result['support_match']}, confidence={result['confidence_match']} (diff={result['confidence_diff']:.4f})")
            
            if result['has_error']:
                print(f"  [WARNING] 规则有问题！")
            else:
                print(f"  [OK] 规则验证通过")
        else:
            print(f"  [ERROR] 验证失败: {result.get('error', 'Unknown error')}")
    
    return results


def print_summary(all_results: List[Dict]):
    """打印验证摘要和统计矩阵"""
    print("\n" + "="*100)
    print("验证摘要")
    print("="*100)
    
    # 按类别分组统计
    categories = ['normal unary', 'normal binary', 'complex unary', 'complex binary']
    
    stats = {}
    for category in categories:
        category_results = [r for r in all_results if r.get('rule_category') == category]
        
        total = len(category_results)
        if total == 0:
            stats[category] = {
                'identical': 0,
                'similar': 0,
                'unmatch': 0,
                'total': 0
            }
            continue
        
        # identical: bodySize, support, confidence都匹配
        identical = sum(1 for r in category_results 
                       if r['bodySize_match'] and r['support_match'] and r['confidence_match'])
        
        # similar: confidence匹配（±10%内），但bodySize或support可能不匹配
        similar = sum(1 for r in category_results 
                     if r['confidence_match'] and not (r['bodySize_match'] and r['support_match']))
        
        # unmatch: confidence不匹配（差异>10%）
        unmatch = sum(1 for r in category_results 
                     if not r['confidence_match'])
        
        stats[category] = {
            'identical': identical,
            'similar': similar,
            'unmatch': unmatch,
            'total': total
        }
        
        print(f"\n{category} ({total}条):")
        print(f"  Identical (完全匹配): {identical} ({identical/total*100:.1f}%)")
        print(f"  Similar (相似): {similar} ({similar/total*100:.1f}%)")
        print(f"  Unmatch (不匹配): {unmatch} ({unmatch/total*100:.1f}%)")
    
    # 打印统计矩阵
    print("\n" + "="*100)
    print("统计矩阵")
    print("="*100)
    print()
    print(f"{'Category':<20} {'Identical':<12} {'Similar':<12} {'Unmatch':<12} {'Total':<12}")
    print("-" * 100)
    
    for category in categories:
        s = stats[category]
        print(f"{category:<20} {s['identical']:<12} {s['similar']:<12} {s['unmatch']:<12} {s['total']:<12}")
    
    # 打印总计
    total_identical = sum(s['identical'] for s in stats.values())
    total_similar = sum(s['similar'] for s in stats.values())
    total_unmatch = sum(s['unmatch'] for s in stats.values())
    total_all = sum(s['total'] for s in stats.values())
    
    print("-" * 100)
    print(f"{'Total':<20} {total_identical:<12} {total_similar:<12} {total_unmatch:<12} {total_all:<12}")
    print()
    
    if total_all > 0:
        print(f"总体准确率 (Identical): {total_identical/total_all*100:.1f}%")
        print(f"总体可接受率 (Identical + Similar): {(total_identical + total_similar)/total_all*100:.1f}%")
    

def main():
    """主函数"""
    # 配置路径
    dataset_path = "data/FB15k-237/train.txt"
    rules_path = "out/FB15k-237/rule.txt"
    
    print("="*100)
    print("Complex Rules验证脚本")
    print("="*100)
    
    try:
        # 加载数据集
        kg = load_dataset(dataset_path)
        
        # 加载规则
        rules = load_rules_from_file(rules_path)
        
        # 采样normal rules (50 unary + 50 binary)
        print("\n" + "="*100)
        print("采样 Normal Rules")
        print("="*100)
        normal_unary_samples, normal_binary_samples = sample_rules(
            rules, n_unary=50, n_binary=50, is_complex=False
        )
        
        # 采样complex rules (50 unary + 50 binary)
        print("\n" + "="*100)
        print("采样 Complex Rules")
        print("="*100)
        complex_unary_samples, complex_binary_samples = sample_rules(
            rules, n_unary=50, n_binary=50, is_complex=True
        )
        
        # 验证规则
        all_results = []
        
        # 验证normal unary rules
        print("\n" + "="*100)
        print("验证 Normal Unary Rules")
        print("="*100)
        normal_unary_results = verify_rules(normal_unary_samples, kg, 'unary', enable_debug=True)
        all_results.extend(normal_unary_results)
        
        # 验证normal binary rules
        print("\n" + "="*100)
        print("验证 Normal Binary Rules")
        print("="*100)
        normal_binary_results = verify_rules(normal_binary_samples, kg, 'binary', enable_debug=False)
        all_results.extend(normal_binary_results)
        
        # 验证complex unary rules
        print("\n" + "="*100)
        print("验证 Complex Unary Rules")
        print("="*100)
        complex_unary_results = verify_rules(complex_unary_samples, kg, 'unary', enable_debug=True)
        all_results.extend(complex_unary_results)
        
        # 验证complex binary rules
        print("\n" + "="*100)
        print("验证 Complex Binary Rules")
        print("="*100)
        complex_binary_results = verify_rules(complex_binary_samples, kg, 'binary', enable_debug=False)
        all_results.extend(complex_binary_results)
        
        # 打印统计矩阵
        print_summary(all_results)
        
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
