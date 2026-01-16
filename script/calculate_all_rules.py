#!/usr/bin/env python3
"""
计算rule.txt中所有规则的指标并输出为CSV（多进程版本）
"""

import os
import sys
import csv
import time
from typing import List, Dict
from multiprocessing import Pool, Manager, Lock
from collections import defaultdict

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


def load_rules_from_file(filepath: str, filter_normal: bool = False) -> List[Dict]:
    """
    从rule.txt文件中加载规则
    
    文件格式：
    bodySize\tsupport\tconfidence\trule_string
    
    Args:
        filepath: 规则文件路径
        filter_normal: 如果为True，过滤掉简单规则，只保留复杂规则
    
    Returns:
        规则列表
    """
    rules = []
    filtered_count = 0
    
    print(f"\n正在加载规则文件: {filepath}")
    if filter_normal:
        print("过滤模式：只加载复杂规则（包含分号的规则）")
    
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
                
                # 如果启用过滤，跳过简单规则
                if filter_normal and not is_complex_rule(rule_string):
                    filtered_count += 1
                    continue
                
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
    
    if filter_normal:
        print(f"过滤掉 {filtered_count} 条简单规则")
    print(f"加载了 {len(rules)} 条规则")
    return rules


def is_complex_rule(rule_string: str) -> bool:
    """判断是否是complex rule（body包含分号）"""
    if '<=' not in rule_string:
        return False
    
    _, body_part = rule_string.split('<=', 1)
    return ';' in body_part


def classify_rule_type(rule_string: str) -> str:
    """
    分类规则类型：unary或binary
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


def get_rule_category(rule_string: str) -> str:
    """
    获取规则的完整分类：normal/complex + unary/binary
    """
    is_complex = is_complex_rule(rule_string)
    rule_type = classify_rule_type(rule_string)
    
    prefix = 'complex' if is_complex else 'normal'
    return f"{prefix} {rule_type}"


def calculate_rule_attributes(rule_string: str) -> dict:
    """
    计算规则的属性：branch、depth、length
    
    - branch: body中的分支数量（用分号分隔）
      - branch=1: normal rules
      - branch>1: complex rules
    
    - depth: 
      - normal rules: depth = length = #bodyAtoms
      - complex rules: depth = max(#bodyAtoms for each body)
    
    - length:
      - normal rules: length = #bodyAtoms
      - complex rules: length = sum(#bodyAtoms for each body)
    
    Args:
        rule_string: 规则字符串
    
    Returns:
        包含 branch, depth, length 的字典
    """
    if '<=' not in rule_string:
        return {'branch': 0, 'depth': 0, 'length': 0}
    
    _, body_part = rule_string.split('<=', 1)
    body_part = body_part.strip()
    
    # 检查是否是U0规则（空body）
    if not body_part or body_part == '':
        return {'branch': 0, 'depth': 0, 'length': 0}
    
    # 检查是否是complex rule（body包含分号）
    if ';' in body_part:
        # Complex rule: 有多个分支
        branches = [b.strip() for b in body_part.split(';')]
        branch = len(branches)
        
        # 计算每个分支的原子数量
        branch_atom_counts = []
        for b in branches:
            # 计算body原子数量
            if '*' in b:
                # 简写格式：通过*连接的关系数量
                atom_count = b.count('*') + 1
            elif ', ' in b:
                # 完整格式：原子之间用", "分隔（逗号+空格）
                atom_count = b.count(', ') + 1
            else:
                # 单个原子
                atom_count = 1
            branch_atom_counts.append(atom_count)
        
        depth = max(branch_atom_counts)
        length = sum(branch_atom_counts)
    else:
        # Normal rule: 只有一个分支
        branch = 1
        
        # 计算body原子数量
        if '*' in body_part:
            # 简写格式：通过*连接的关系数量
            atom_count = body_part.count('*') + 1
        elif ', ' in body_part:
            # 完整格式：原子之间用", "分隔（逗号+空格）
            atom_count = body_part.count(', ') + 1
        else:
            # 单个原子
            atom_count = 1
        
        depth = atom_count
        length = atom_count
    
    return {'branch': branch, 'depth': depth, 'length': length}


def calculate_match_level(original_conf: float, calculated_conf: float) -> int:
    """
    计算匹配程度
    
    Args:
        original_conf: 原始置信度
        calculated_conf: 计算得到的置信度
        
    Returns:
        0: identical (完全一致)
        1: close (差异 < 10%)
        2: similar (差异 < 20%)
        3: unmatched (差异 >= 20%)
    """
    diff = abs(calculated_conf - original_conf)
    
    if diff == 0:
        return 0
    elif diff < 0.1:
        return 1
    elif diff < 0.2:
        return 2
    else:
        return 3


def calculate_rule_metrics(rule: Dict, kg: KnowledgeGraph) -> Dict:
    """
    计算单个规则的指标
    
    Returns:
        结果字典，包含：
        {
            'originalRule': str,
            'simplifiedRule': str,
            'originalMetric': dict,
            'calculatedMetric': dict,
            'match': int,
            'category': str,
            'success': bool,
            'error': str (if failed)
        }
    """
    rule_string = rule['rule_string']
    original_metric = {
        'bodySize': rule['bodySize'],
        'support': rule['support'],
        'confidence': rule['confidence']
    }
    
    # 获取规则分类
    category = get_rule_category(rule_string)
    
    # 计算规则属性
    attributes = calculate_rule_attributes(rule_string)
    
    try:
        # 解析规则
        head_relation, body_relations, variable_count, rule_info = RuleParser.parse_rule(rule_string)
        
        simplified_rule = rule_info.get('normalized_rule', rule_string)
        
        # 创建计算器
        calculator = RuleSupportCalculator(kg)
        
        # 计算实际指标
        result = calculator.calculate_rule_support_join(rule_info)
        
        calculated_metric = {
            'headSize': result['headSize'],
            'bodySize': result['bodySize'],
            'support': result['support'],
            'confidence': result['confidence']
        }
        
        # 计算匹配程度
        match = calculate_match_level(original_metric['confidence'], calculated_metric['confidence'])
        
        # 计算偏差值
        deviation = original_metric['confidence'] - calculated_metric['confidence']
        
        return {
            'originalRule': rule_string,
            'simplifiedRule': simplified_rule,
            'originalMetric': str(original_metric),
            'calculatedMetric': str(calculated_metric),
            'match': match,
            'category': category,
            'branch': attributes['branch'],
            'depth': attributes['depth'],
            'length': attributes['length'],
            'deviation': deviation,
            'success': True,
            'line_num': rule.get('line_num', 0)
        }
        
    except Exception as e:
        return {
            'originalRule': rule_string,
            'simplifiedRule': 'ERROR',
            'originalMetric': str(original_metric),
            'calculatedMetric': 'ERROR',
            'match': 3,
            'category': category,
            'branch': attributes['branch'],
            'depth': attributes['depth'],
            'length': attributes['length'],
            'deviation': 0,
            'success': False,
            'error': str(e),
            'line_num': rule.get('line_num', 0)
        }


# 全局变量，用于多进程
_global_kg = None

def init_worker(dataset_path):
    """初始化工作进程，加载知识图谱"""
    global _global_kg
    _global_kg = load_dataset(dataset_path)


def process_rule_wrapper(rule):
    """多进程处理规则的包装函数"""
    global _global_kg
    return calculate_rule_metrics(rule, _global_kg)


def print_statistics_matrix(results: List[Dict]):
    """打印统计矩阵"""
    categories = ['normal unary', 'normal binary', 'complex unary', 'complex binary']
    
    stats = {}
    for category in categories:
        category_results = [r for r in results if r.get('category') == category]
        
        total = len(category_results)
        if total == 0:
            stats[category] = {
                'identical': 0,
                'close': 0,
                'similar': 0,
                'unmatch': 0,
                'total': 0
            }
            continue
        
        # identical: match=0
        identical = sum(1 for r in category_results if r['match'] == 0)
        
        # close: match=1
        close = sum(1 for r in category_results if r['match'] == 1)
        
        # similar: match=2
        similar = sum(1 for r in category_results if r['match'] == 2)
        
        # unmatch: match=3
        unmatch = sum(1 for r in category_results if r['match'] == 3)
        
        stats[category] = {
            'identical': identical,
            'close': close,
            'similar': similar,
            'unmatch': unmatch,
            'total': total
        }
    
    # 打印统计矩阵
    print("\n" + "="*100)
    print("统计矩阵")
    print("="*100)
    print()
    print(f"{'Category':<20} {'Identical':<12} {'Close':<12} {'Similar':<12} {'Unmatch':<12} {'Total':<12}")
    print("-" * 100)
    
    for category in categories:
        s = stats[category]
        print(f"{category:<20} {s['identical']:<12} {s['close']:<12} {s['similar']:<12} {s['unmatch']:<12} {s['total']:<12}")
    
    # 打印总计
    total_identical = sum(s['identical'] for s in stats.values())
    total_close = sum(s['close'] for s in stats.values())
    total_similar = sum(s['similar'] for s in stats.values())
    total_unmatch = sum(s['unmatch'] for s in stats.values())
    total_all = sum(s['total'] for s in stats.values())
    
    print("-" * 100)
    print(f"{'Total':<20} {total_identical:<12} {total_close:<12} {total_similar:<12} {total_unmatch:<12} {total_all:<12}")
    print()
    
    if total_all > 0:
        print(f"Identical率: {total_identical/total_all*100:.1f}%")
        print(f"Close率: {total_close/total_all*100:.1f}%")
        print(f"Similar率: {total_similar/total_all*100:.1f}%")
        print(f"Unmatch率: {total_unmatch/total_all*100:.1f}%")
    print("="*100)


def process_all_rules(rules: List[Dict], dataset_path: str, output_csv: str, num_processes: int = 20):
    """
    使用多进程处理所有规则并输出到CSV
    
    Args:
        rules: 规则列表
        dataset_path: 数据集路径
        output_csv: 输出CSV文件路径
        num_processes: 进程数
    """
    print(f"\n开始处理 {len(rules)} 条规则，使用 {num_processes} 个进程...")
    
    all_results = []
    save_interval = 500  # 每处理500条规则保存一次
    
    start_time = time.time()
    
    # 创建进程池
    with Pool(processes=num_processes, initializer=init_worker, initargs=(dataset_path,)) as pool:
        # 使用imap_unordered来获取结果，这样可以逐个处理而不用等所有结果
        for i, result in enumerate(pool.imap_unordered(process_rule_wrapper, rules, chunksize=10), 1):
            all_results.append(result)
            
            # 每100条显示进度
            if i % 100 == 0:
                elapsed = time.time() - start_time
                speed = i / elapsed
                remaining = (len(rules) - i) / speed if speed > 0 else 0
                print(f"进度: {i}/{len(rules)} ({i/len(rules)*100:.1f}%) - "
                      f"速度: {speed:.1f} rules/s - "
                      f"预计剩余: {remaining/60:.1f} 分钟")
            
            # 每save_interval条保存一次结果
            if i % save_interval == 0:
                save_results_to_csv(all_results, output_csv)
                print(f"\n中间结果已保存 ({i} 条规则)")
                print_statistics_matrix(all_results)
    
    # 保存最终结果
    save_results_to_csv(all_results, output_csv)
    
    elapsed = time.time() - start_time
    print(f"\n处理完成！总耗时: {elapsed/60:.1f} 分钟")
    print(f"结果已保存到: {output_csv}")
    
    # 打印最终统计
    print_statistics_matrix(all_results)
    
    # 打印详细统计信息
    print_detailed_statistics(all_results)


def save_results_to_csv(results: List[Dict], output_csv: str):
    """保存结果到CSV文件"""
    with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['originalRule', 'simplifiedRule', 'originalMetric', 'calculatedMetric', 
                      'match', 'ruleType', 'branch', 'depth', 'length', 'deviation']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        for result in results:
            writer.writerow({
                'originalRule': result['originalRule'],
                'simplifiedRule': result['simplifiedRule'],
                'originalMetric': result['originalMetric'],
                'calculatedMetric': result['calculatedMetric'],
                'match': result['match'],
                'ruleType': result.get('category', 'unknown'),
                'branch': result.get('branch', 0),
                'depth': result.get('depth', 0),
                'length': result.get('length', 0),
                'deviation': result.get('deviation', 0)
            })


def print_detailed_statistics(results: List[Dict]):
    """打印详细统计信息"""
    stats = {
        'total': len(results),
        'success': sum(1 for r in results if r['success']),
        'failed': sum(1 for r in results if not r['success']),
        'match_0': sum(1 for r in results if r['match'] == 0),
        'match_1': sum(1 for r in results if r['match'] == 1),
        'match_2': sum(1 for r in results if r['match'] == 2),
        'match_3': sum(1 for r in results if r['match'] == 3)
    }
    
    print(f"\n详细统计信息:")
    print(f"  总规则数: {stats['total']}")
    print(f"  处理成功: {stats['success']} ({stats['success']/stats['total']*100:.1f}%)")
    print(f"  处理失败: {stats['failed']} ({stats['failed']/stats['total']*100:.1f}%)")
    print(f"\n匹配程度分布:")
    print(f"  Identical (0): {stats['match_0']} ({stats['match_0']/stats['total']*100:.1f}%)")
    print(f"  Close (1, <10%): {stats['match_1']} ({stats['match_1']/stats['total']*100:.1f}%)")
    print(f"  Similar (2, <20%): {stats['match_2']} ({stats['match_2']/stats['total']*100:.1f}%)")
    print(f"  Unmatched (3, >=20%): {stats['match_3']} ({stats['match_3']/stats['total']*100:.1f}%)")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='计算rule.txt中所有规则的指标')
    parser.add_argument('--rule_file', type=str, help='规则文件路径')
    parser.add_argument('--dataset', type=str, help='数据集路径')
    parser.add_argument('--output', type=str, help='输出CSV文件路径')
    parser.add_argument('--num_processes', type=int, default=28, help='进程数，默认20')
    parser.add_argument('--filter_normal', action='store_true', help='过滤掉简单规则，只处理复杂规则')
    
    args = parser.parse_args()
    
    # 如果没有提供参数，使用默认值
    dataset_path = args.dataset if args.dataset else "data/FB15k-237/train.txt"
    rules_path = args.rule_file if args.rule_file else "out/FB15k-237/rule.txt"
    output_csv = args.output if args.output else "out/FB15k-237/rule_metrics.csv"
    num_processes = args.num_processes
    filter_normal = args.filter_normal
    
    print("="*100)
    print("规则指标计算脚本（多进程版）")
    print("="*100)
    print(f"数据集: {dataset_path}")
    print(f"规则文件: {rules_path}")
    print(f"输出CSV: {output_csv}")
    print(f"进程数: {num_processes}")
    print(f"过滤简单规则: {filter_normal}")
    print("="*100)
    
    try:
        # 加载规则（可选过滤简单规则）
        rules = load_rules_from_file(rules_path, filter_normal)
        
        # 随机打乱规则顺序
        import random
        random.shuffle(rules)
        print(f"已随机打乱规则顺序")
        
        # 使用多进程处理所有规则并输出CSV
        process_all_rules(rules, dataset_path, output_csv, num_processes)
        
    except Exception as e:
        print(f"\n错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
