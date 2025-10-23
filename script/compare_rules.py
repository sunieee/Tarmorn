#!/usr/bin/env python3
"""
规则比较脚本
比较两个规则文件，分析特定关系作为规则头的所有规则
"""

import re
import os
import csv
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

def is_binary_rule(rule: str) -> bool:
    """
    判断是否为Binary规则（规则头包含(X,Y)）
    """
    if '<=' not in rule:
        return False
    head = rule.split('<=', 1)[0].strip()
    return '(X,Y)' in head

def get_head_variable_type(rule: str) -> str:
    """
    获取规则头的变量类型
    返回: 'binary' (X,Y), 'head_var' (X,c), 'tail_var' (c,X),'loop' (X,X), 'unknown'
    """
    if '<=' not in rule:
        return 'unknown'
    
    head = rule.split('<=', 1)[0].strip()
    
    # 提取括号中的内容
    match = re.search(r'\(([^)]+)\)', head)
    if not match:
        return 'unknown'
    
    args = match.group(1)
    
    if '(X,Y)' in head:
        return 'binary'
    elif '(X,X)' in head:
        return 'loop'
    elif '(X,' in head:
        return 'head_var'  # X在头部位置
    elif ',X)' in head:
        return 'tail_var'  # X在尾部位置
    else:
        return 'constant'

def get_relation_path_length(atom: str) -> int:
    """
    获取原子中关系路径的长度（关系的个数）
    通过计数"·"的个数+1来确定
    """
    # 提取关系部分（括号之前的部分）
    match = re.match(r'([^(]+)\(', atom.strip())
    if not match:
        return 0
    
    relation_path = match.group(1)
    # 计算"·"的个数
    dot_count = relation_path.count('·')
    # 关系路径长度 = "·"的个数 + 1
    return dot_count + 1

def analyze_rule_statistics(rules_dict: Dict[str, List], file_name: str) -> Dict:
    """
    分析规则的详细统计信息
    """
    stats = {
        'total_rules': len(rules_dict),
        'binary_rules': 0,
        'unary_rules': 0,
        'unary_head_var': 0,  # U规则中head variable
        'unary_tail_var': 0,  # U规则中tail variable
        'unary_loop': 0,       # U规则中loop (X,X)
        'unary_constant': 0,   # U规则中constant
        'body_relation_lengths': defaultdict(int),  # body中不同关系路径长度的分布
    }
    
    for normalized_rule, rule_list in rules_dict.items():
        rule = rule_list[0][0]  # 取第一个原始规则
        
        # 判断Binary还是Unary
        if is_binary_rule(rule):
            stats['binary_rules'] += 1
        else:
            stats['unary_rules'] += 1
            
            # 分析Unary规则的头部变量类型
            head_type = get_head_variable_type(rule)
            stats_key = f'unary_{head_type}'
            stats[stats_key] += 1
        
        # 分析body中原子的关系路径长度
        if '<=' in rule:
            body = rule.split('<=', 1)[1].strip()
            # 分割body中的多个原子（用逗号分隔）
            atoms = [atom.strip() for atom in body.split(',')]
            
            for atom in atoms:
                length = get_relation_path_length(atom)
                if length > 0:
                    stats['body_relation_lengths'][length] += 1
    
    return stats

def save_statistics_to_csv(stats1: Dict, stats2: Dict, file1_name: str, file2_name: str, 
                           set1: Set, set2: Set, rules1: Dict, rules2: Dict, output_file: str, kg=None):
    """
    将统计结果保存到CSV文件
    """
    with open(output_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.writer(csvfile)
        
        # ========== 基本统计 ==========
        writer.writerow(['基本统计'])
        writer.writerow(['统计项', file1_name, file2_name, '差异'])
        writer.writerow(['总规则数', stats1['total_rules'], stats2['total_rules'], 
                        stats2['total_rules'] - stats1['total_rules']])
        writer.writerow([])
        
        # ========== 规则类型分布 ==========
        writer.writerow(['规则类型分布'])
        writer.writerow(['类型', file1_name, file2_name, '差异'])
        writer.writerow(['Binary规则 (X,Y)', 
                        f"{stats1['binary_rules']} ({stats1['binary_rules']/stats1['total_rules']*100:.2f}%)",
                        f"{stats2['binary_rules']} ({stats2['binary_rules']/stats2['total_rules']*100:.2f}%)",
                        stats2['binary_rules'] - stats1['binary_rules']])
        writer.writerow(['Unary规则', 
                        f"{stats1['unary_rules']} ({stats1['unary_rules']/stats1['total_rules']*100:.2f}%)",
                        f"{stats2['unary_rules']} ({stats2['unary_rules']/stats2['total_rules']*100:.2f}%)",
                        stats2['unary_rules'] - stats1['unary_rules']])
        writer.writerow([])
        
        # ========== Unary规则头部变量类型分布 ==========
        if stats1['unary_rules'] > 0 or stats2['unary_rules'] > 0:
            writer.writerow(['Unary规则头部变量类型'])
            writer.writerow(['类型', file1_name, file2_name, '差异'])
            writer.writerow(['Head Variable (X,c)', 
                            f"{stats1['unary_head_var']} ({stats1['unary_head_var']/max(stats1['unary_rules'],1)*100:.2f}%)" if stats1['unary_rules'] > 0 else "0 (0.00%)",
                            f"{stats2['unary_head_var']} ({stats2['unary_head_var']/max(stats2['unary_rules'],1)*100:.2f}%)" if stats2['unary_rules'] > 0 else "0 (0.00%)",
                            stats2['unary_head_var'] - stats1['unary_head_var']])
            writer.writerow(['Tail Variable (c,X)', 
                            f"{stats1['unary_tail_var']} ({stats1['unary_tail_var']/max(stats1['unary_rules'],1)*100:.2f}%)" if stats1['unary_rules'] > 0 else "0 (0.00%)",
                            f"{stats2['unary_tail_var']} ({stats2['unary_tail_var']/max(stats2['unary_rules'],1)*100:.2f}%)" if stats2['unary_rules'] > 0 else "0 (0.00%)",
                            stats2['unary_tail_var'] - stats1['unary_tail_var']])
            writer.writerow(['Loop (X,X)', 
                            f"{stats1['unary_loop']} ({stats1['unary_loop']/max(stats1['unary_rules'],1)*100:.2f}%)" if stats1['unary_rules'] > 0 else "0 (0.00%)",
                            f"{stats2['unary_loop']} ({stats2['unary_loop']/max(stats2['unary_rules'],1)*100:.2f}%)" if stats2['unary_rules'] > 0 else "0 (0.00%)",
                            stats2['unary_loop'] - stats1['unary_loop']])
            writer.writerow(['Constant', 
                            f"{stats1['unary_constant']} ({stats1['unary_constant']/max(stats1['unary_rules'],1)*100:.2f}%)" if stats1['unary_rules'] > 0 else "0 (0.00%)",
                            f"{stats2['unary_constant']} ({stats2['unary_constant']/max(stats2['unary_rules'],1)*100:.2f}%)" if stats2['unary_rules'] > 0 else "0 (0.00%)",
                            stats2['unary_constant'] - stats1['unary_constant']])
            writer.writerow([])
        
        # ========== Body中关系路径长度分布 ==========
        writer.writerow(['Body中关系路径长度分布'])
        writer.writerow(['长度', file1_name, file2_name, '差异'])
        
        total_atoms1 = sum(stats1['body_relation_lengths'].values())
        total_atoms2 = sum(stats2['body_relation_lengths'].values())
        
        # 获取所有出现的长度
        all_lengths = sorted(set(stats1['body_relation_lengths'].keys()) | set(stats2['body_relation_lengths'].keys()))
        
        for length in all_lengths:
            count1 = stats1['body_relation_lengths'].get(length, 0)
            count2 = stats2['body_relation_lengths'].get(length, 0)
            pct1 = f"{count1/total_atoms1*100:.2f}%" if total_atoms1 > 0 else "0.00%"
            pct2 = f"{count2/total_atoms2*100:.2f}%" if total_atoms2 > 0 else "0.00%"
            writer.writerow([f'长度{length} (L{length})', 
                            f"{count1} ({pct1})",
                            f"{count2} ({pct2})",
                            count2 - count1])
        
        writer.writerow(['总原子数', total_atoms1, total_atoms2, total_atoms2 - total_atoms1])
        writer.writerow([])
        
        # ========== 规则覆盖情况 ==========
        common_rules = set1 & set2
        only_in_1 = set1 - set2
        only_in_2 = set2 - set1
        coverage = len(common_rules) / len(set1) if len(set1) > 0 else 0
        
        writer.writerow(['规则覆盖情况'])
        writer.writerow(['统计项', '数值'])
        writer.writerow(['共同规则数', len(common_rules)])
        writer.writerow([f'仅在{file1_name}中的规则数', len(only_in_1)])
        writer.writerow([f'仅在{file2_name}中的规则数', len(only_in_2)])
        writer.writerow([f'{file2_name}对{file1_name}的覆盖率', f'{coverage*100:.2f}%'])
        writer.writerow([])
        
        # ========== 共同规则示例 ==========
        topN = 20
        if common_rules:
            # 分离Binary和Unary规则
            binary_rules = [rule for rule in common_rules if is_binary_rule(rule)]
            unary_rules = [rule for rule in common_rules if not is_binary_rule(rule)]
            
            # 计算每种类型应该取多少条
            half_n = topN // 2
            selected_rules = binary_rules[:half_n] + unary_rules[:half_n]
            
            writer.writerow([f'共同规则示例 (共{len(common_rules)}条，展示前{topN}条: 前{half_n}条Binary, 后{half_n}条Unary)'])
            if kg is not None:
                writer.writerow(['规则', f'{file1_name}指标', f'{file2_name}指标', '真实结果'])
                for rule in selected_rules:
                    metrics1 = rules1[rule][0][1] if rules1[rule] else {}
                    metrics2 = rules2[rule][0][1] if rules2[rule] else {}
                    real_result = analyze_rule_from_string(rule, kg) if kg else None
                    real_result_str = str(real_result['join_result']) if real_result else 'N/A'
                    writer.writerow([rule, str(metrics1), str(metrics2), real_result_str])
            else:
                writer.writerow(['规则', f'{file1_name}指标', f'{file2_name}指标'])
                for rule in selected_rules:
                    metrics1 = rules1[rule][0][1] if rules1[rule] else {}
                    metrics2 = rules2[rule][0][1] if rules2[rule] else {}
                    writer.writerow([rule, str(metrics1), str(metrics2)])
            writer.writerow([])
        
        # ========== 仅在file1中的规则 ==========
        if only_in_1:
            # 分离Binary和Unary规则
            binary_rules = [rule for rule in only_in_1 if is_binary_rule(rule)]
            unary_rules = [rule for rule in only_in_1 if not is_binary_rule(rule)]
            
            # 计算每种类型应该取多少条
            half_n = topN // 2
            selected_rules = binary_rules[:half_n] + unary_rules[:half_n]
            
            writer.writerow([f'仅在{file1_name}中的规则 (共{len(only_in_1)}条，展示前{topN}条: 前{half_n}条Binary, 后{half_n}条Unary)'])
            if kg is not None:
                writer.writerow(['规则', '指标', '真实结果'])
                for rule in selected_rules:
                    metrics1 = rules1[rule][0][1] if rules1[rule] else {}
                    real_result = analyze_rule_from_string(rule, kg) if kg else None
                    real_result_str = str(real_result['join_result']) if real_result else 'N/A'
                    writer.writerow([rule, str(metrics1), real_result_str])
            else:
                writer.writerow(['规则', '指标'])
                for rule in selected_rules:
                    metrics1 = rules1[rule][0][1] if rules1[rule] else {}
                    writer.writerow([rule, str(metrics1)])
            writer.writerow([])
        
        # ========== 仅在file2中的规则 ==========
        if only_in_2:
            # 分离Binary和Unary规则
            binary_rules = [rule for rule in only_in_2 if is_binary_rule(rule)]
            unary_rules = [rule for rule in only_in_2 if not is_binary_rule(rule)]
            
            # 计算每种类型应该取多少条
            half_n = topN // 2
            selected_rules = binary_rules[:half_n] + unary_rules[:half_n]
            
            writer.writerow([f'仅在{file2_name}中的规则 (共{len(only_in_2)}条，展示前{topN}条: 前{half_n}条Binary, 后{half_n}条Unary)'])
            if kg is not None:
                writer.writerow(['规则', '指标', '真实结果'])
                for rule in selected_rules:
                    metrics2 = rules2[rule][0][1] if rules2[rule] else {}
                    real_result = analyze_rule_from_string(rule, kg) if kg else None
                    real_result_str = str(real_result['join_result']) if real_result else 'N/A'
                    writer.writerow([rule, str(metrics2), real_result_str])
            else:
                writer.writerow(['规则', '指标'])
                for rule in selected_rules:
                    metrics2 = rules2[rule][0][1] if rules2[rule] else {}
                    writer.writerow([rule, str(metrics2)])
    
    print(f"\n统计结果已保存到: {output_file}")

def compare_rules(rules1: Dict[str, List], rules2: Dict[str, List], file1_name: str, file2_name: str, kg=None):
    """
    比较两个规则集合
    """
    print(f"\n=== 规则比较分析 ===")
    print(f"文件1: {file1_name}")
    print(f"文件2: {file2_name}")
    
    # 详细统计
    stats1 = analyze_rule_statistics(rules1, file1_name)
    stats2 = analyze_rule_statistics(rules2, file2_name)
    
    set1 = set(rules1.keys())
    set2 = set(rules2.keys())
    
    # 返回统计数据，用于CSV导出
    return stats1, stats2, set1, set2

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
    print(f"已加载 {file1_name}: {len(rules1)} 条规则")
    print(f"已加载 {file2_name}: {len(rules2)} 条规则")
    
    # 加载知识图谱
    print(f"\n=== 加载知识图谱 ===")
    kg = None
    if os.path.exists(dataset_path):
        try:
            kg = load_dataset(dataset_path)
            print(f"知识图谱加载成功")
        except Exception as e:
            print(f"加载知识图谱失败: {e}")
            print(f"将不包含真实结果信息")
    else:
        print(f"数据集文件不存在: {dataset_path}")
        print(f"将不包含真实结果信息")
    
    # 比较规则并生成统计
    stats1, stats2, set1, set2 = compare_rules(rules1, rules2, file1_name, file2_name, None)
    
    # 导出到CSV
    csv_output_path = os.path.join(base_dir, "out", dataset, "comparison_result.csv")
    save_statistics_to_csv(stats1, stats2, file1_name, file2_name, set1, set2, rules1, rules2, csv_output_path, kg)

if __name__ == "__main__":
    main()