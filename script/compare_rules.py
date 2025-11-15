#!/usr/bin/env python3
"""
规则比较脚本
比较两个规则文件，分析特定关系作为规则头的所有规则
"""

import re
import os
import csv
from typing import Set, List, Tuple, Dict, Optional
from collections import defaultdict

# 导入analysis_rule模块
from analysis_rule import load_dataset, analyze_rule_from_string, RuleParser

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

def load_rules_with_target_relation(file_path: str, target_relation: str = None) -> Dict[str, List[Tuple[str, Dict, str]]]:
    """
    加载文件中的规则
    如果target_relation为None，加载所有规则
    如果target_relation不为None，只加载包含目标关系作为头部的规则
    返回: {标准化规则: [(原始规则, 指标字典, 原始行)]}
    """
    rules_dict = defaultdict(list)
    
    if target_relation is None:
        print(f"Loading all rules from: {file_path}")
    else:
        print(f"Loading rules from: {file_path}")
        print(f"Target relation: {target_relation}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            line_count = 0
            target_rule_count = 0
            
            for line in f:
                line_count += 1
                if line_count % 100000 == 0:
                    if target_relation is None:
                        print(f"  Processed {line_count} lines, loaded {target_rule_count} rules")
                    else:
                        print(f"  Processed {line_count} lines, found {target_rule_count} target rules")
                
                rule, metrics, original_line = parse_rule_line(line)
                
                # 如果target_relation为None，加载所有规则；否则只加载匹配的规则
                if rule and (target_relation is None or rule.startswith(target_relation)):
                    target_rule_count += 1
                    # 标准化规则（移除置信度差异，便于比较）
                    normalized_rule = normalize_rule(rule)
                    rules_dict[normalized_rule].append((rule, metrics, original_line.strip()))
            
            print(f"  Total lines: {line_count}")
            if target_relation is None:
                print(f"  Loaded {target_rule_count} total rules")
            else:
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

def convert_to_simplified_format(rule: str) -> str:
    """
    将带括号格式的规则转换为简写格式
    使用analysis_rule.py中的RuleParser._normalize_to_simplified方法
    
    例如:
    /award/award_category/winners./award/award_honor/ceremony(X,Y) <=
    /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)
    
    转换为:
    /award/award_category/winners./award/award_honor/ceremony <=
    /award/award_category/winners./award/award_honor/award_winner · INVERSE_/award/award_ceremony/awards_presented./award/award_honor/award_winner
    """
    if '<=' not in rule:
        return rule
    
    head_part, body_part = rule.split('<=', 1)
    head_part = head_part.strip()
    body_part = body_part.strip()
    
    # 检查是否已经是简写格式
    # 简写格式特征: 二元规则头部没有括号，或一元规则头部只有一个参数
    if '(' not in head_part or ')' not in head_part:
        # 已经是简写格式，但需要添加空格美化
        return beautify_simplified_rule(rule)
    
    paren_content = head_part.split('(')[1].split(')')[0]
    if ',' not in paren_content:
        # 一元简写格式，但需要添加空格美化
        return beautify_simplified_rule(rule)
    
    # 使用RuleParser转换
    try:
        normalized = RuleParser._normalize_to_simplified(head_part, body_part)
        # 美化输出：在·连接符左右添加空格
        normalized = beautify_simplified_rule(normalized)
        return normalized
    except Exception as e:
        # 如果转换失败，返回原始规则
        print(f"Warning: Failed to convert rule to simplified format: {rule}, error: {e}")
        return rule

def beautify_simplified_rule(rule: str) -> str:
    """
    美化简写格式规则：在·连接符左右添加空格
    
    例如：/rel1·/rel2·INVERSE_/rel3 => /rel1 · /rel2 · INVERSE_/rel3
    """
    # 替换·为 · （左右各添加一个空格）
    # 但要避免重复添加空格
    beautified = re.sub(r'\s*·\s*', ' · ', rule)
    return beautified


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
    返回: 'r(X,c)', 'r(c,X)', 'r(X,X)', 'binary'
    """
    if '<=' not in rule:
        raise ValueError(f"规则格式错误，缺少'<=': {rule}")
    
    head = rule.split('<=', 1)[0].strip()
    
    # 提取括号中的内容
    match = re.search(r'\(([^)]+)\)', head)
    if not match:
        raise ValueError(f"规则头格式错误，无法提取括号内容: {head}")
    
    args = match.group(1).split(',')
    if len(args) != 2:
        raise ValueError(f"规则头参数数量错误，期望2个参数: {args}")
    
    arg1, arg2 = args[0].strip(), args[1].strip()
    
    # 定义变量类型：X类变量和A类变量
    x_vars = {'X', 'Y', 'Z'}  # X类变量
    a_vars = {'A', 'B', 'C'}  # A类变量
    
    # 判断变量类型
    is_x_var1 = arg1 in x_vars
    is_x_var2 = arg2 in x_vars
    is_a_var1 = arg1 in a_vars
    is_a_var2 = arg2 in a_vars
    is_var1 = is_x_var1 or is_a_var1
    is_var2 = is_x_var2 or is_a_var2
    
    if arg1 == 'X' and arg2 == 'Y':
        return 'binary'
    elif is_x_var1 and is_x_var2 and arg1 == arg2:
        return 'r(X,X)'  # 两个相同的X类变量
    elif is_x_var1 and not is_var2:
        return 'r(X,c)'  # X类变量,常量
    elif not is_var1 and is_x_var2:
        return 'r(c,X)'  # 常量,X类变量
    else:
        raise ValueError(f"不支持的规则头类型: ({arg1},{arg2}) in {rule}")

def parse_body_atoms(body: str) -> List[str]:
    """
    正确解析body中的原子，考虑原子内部可能包含逗号
    例如: "/award/award_category/winners./award/award_honor/ceremony(/m/0gs9p,X), other_relation(Y,Z)"
    """
    atoms = []
    current_atom = ""
    paren_count = 0
    i = 0
    while i < len(body):
        char = body[i]
        
        if char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        elif char == ',' and paren_count == 0:
            # 只有在括号外的逗号才是原子分隔符
            if current_atom.strip():
                atoms.append(current_atom.strip())
            current_atom = ""
            i += 1
            continue
        
        current_atom += char
        i += 1
    
    # 添加最后一个原子
    if current_atom.strip():
        atoms.append(current_atom.strip())
    
    return atoms

def get_body_atom_type(atom: str) -> str:
    """
    获取body中原子的变量类型
    返回: 'rp(X,c)', 'rp(c,X)', 'rp(X,A)', 'rp(A,X)', 'rp(X,X)'
    """
    # 提取括号中的内容
    match = re.search(r'\(([^)]+)\)', atom.strip())
    if not match:
        print("Wrong format:", atom)
        raise ValueError(f"Body原子格式错误，无法提取括号内容: {atom}")
    
    args = match.group(1).split(',')
    if len(args) != 2:
        raise ValueError(f"Body原子参数数量错误，期望2个参数: {args} in {atom}")
    
    arg1, arg2 = args[0].strip(), args[1].strip()
    
    # 定义变量类型：X类变量和A类变量
    x_vars = {'X', 'Y', 'Z'}  # X类变量
    a_vars = {'A', 'B', 'C'}  # A类变量
    
    # 判断变量类型
    is_x_var1 = arg1 in x_vars
    is_x_var2 = arg2 in x_vars
    is_a_var1 = arg1 in a_vars
    is_a_var2 = arg2 in a_vars
    is_var1 = is_x_var1 or is_a_var1
    is_var2 = is_x_var2 or is_a_var2
    
    if is_x_var1 and is_x_var2 and arg1 == arg2:
        return 'rp(X,X)'  # 两个相同的X类变量
    elif is_x_var1 and is_a_var2:
        return 'rp(X,A)'  # X类变量和A类变量
    elif is_a_var1 and is_x_var2:
        return 'rp(A,X)'  # A类变量和X类变量
    elif is_x_var1 and not is_var2:
        return 'rp(X,c)'  # X类变量和常量
    elif not is_var1 and is_x_var2:
        return 'rp(c,X)'  # 常量和X类变量
    else:
        raise ValueError(f"不支持的Body原子类型: ({arg1},{arg2}) in {atom}")

def get_relation_path_length(atom: str) -> int:
    """
    获取原子中关系路径的长度（关系的个数）
    通过计数"·"的个数+1来确定
    
    注意：这个函数现在用于简写格式的原子
    简写格式的原子就是一个关系路径，可能包含多个关系用·连接
    """
    # 对于简写格式，atom可能不包含括号，就是纯关系路径
    # 或者是 relation_path(constant) 格式
    
    # 移除括号部分（如果有）
    if '(' in atom:
        relation_path = atom.split('(')[0].strip()
    else:
        relation_path = atom.strip()
    
    # 计算"·"的个数（忽略空格）
    # 移除所有空格后再计数
    relation_path_no_space = relation_path.replace(' ', '')
    dot_count = relation_path_no_space.count('·')
    # 关系路径长度 = "·"的个数 + 1
    return dot_count + 1

def get_rule_length_type(rule: str) -> str:
    """
    获取规则的长度类型
    返回: 'L1', 'L2', 'L3', 或 'other'
    
    注意：首先将规则转换为简写格式，然后基于简写格式判断长度类型
    这样可以正确区分L1/L2/L3，避免将L3误判为L1
    """
    if '<=' not in rule:
        return 'other'
    
    # 首先转换为简写格式
    simplified_rule = convert_to_simplified_format(rule)
    
    # 从简写格式中提取body
    body = simplified_rule.split('<=', 1)[1].strip()
    
    # 对于简写格式的二元规则，body就是一个关系路径（可能包含·连接多个关系）
    # 对于简写格式的一元规则，body是 relation_path(constant) 格式
    # 可能有多个这样的原子（虽然通常一元规则只有一个body原子）
    
    # 解析body原子
    # 简写格式的二元规则: body就是一个关系路径，没有逗号分隔
    # 简写格式的一元规则: body可能是 rel(c) 或 rel1·rel2(c)
    
    # 检查是否为二元规则（简写格式下，二元规则的body没有括号，或者head没有括号）
    head = simplified_rule.split('<=', 1)[0].strip()
    is_binary = '(' not in head or '(X,Y)' in head
    
    if is_binary and ',' not in body:
        # 二元规则简写格式：body是单个关系路径
        length = get_relation_path_length(body)
        return f'L{length}'
    else:
        # 一元规则或带逗号的body（可能转换失败）
        # 尝试按照简写格式解析
        atoms = parse_body_atoms(body)
        
        if len(atoms) == 1:
            # 单个原子，直接计算长度
            length = get_relation_path_length(atoms[0])
            return f'L{length}'
        else:
            # 多个原子，取最大长度
            lengths = []
            for atom in atoms:
                length = get_relation_path_length(atom)
                if length > 0:
                    lengths.append(length)
            
            if not lengths:
                print('No valid atoms in rule body:', rule)
                return 'L0'
                
            # 判断规则长度类型
            max_length = max(lengths)
            assert max_length <= 3, f"Unexpected relation path length: {max_length} in rule: {rule}"
            return f'L{max_length}'

def analyze_rule_statistics(rules_dict: Dict[str, List]) -> Dict:
    """
    分析规则的详细统计信息
    """
    stats = {
        'total_rules': len(rules_dict),
        'binary_rules': 0,
        'unary_rules': 0,
        'L0_unary': 0,
        'L1_unary': 0,
        'L2_unary': 0,
        'L0_binary': 0,
        'L1_binary': 0,
        'L2_binary': 0,
        'L3_binary': 0,
        'atom_relation_lengths': defaultdict(int),
    }
    
    # Unary规则矩阵：head_type × body_type
    head_types = ['r(X,c)', 'r(c,X)', 'r(X,X)']
    body_types = ['rp(X,c)', 'rp(c,X)', 'rp(X,A)', 'rp(A,X)', 'rp(X,X)']
    
    # 初始化矩阵
    stats['unary_matrix'] = {}
    for head_type in head_types:
        stats['unary_matrix'][head_type] = {}
        for body_type in body_types:
            stats['unary_matrix'][head_type][body_type] = 0
    
    for normalized_rule, rule_list in rules_dict.items():
        rule = rule_list[0][0]
        length_type = get_rule_length_type(rule)
        # 判断Binary还是Unary
        if is_binary_rule(rule):
            stats['binary_rules'] += 1
            if length_type in ['L0', 'L1', 'L2', 'L3']:
                stats[f'{length_type}_binary'] += 1
        else:
            stats['unary_rules'] += 1
            if length_type in ['L0', 'L1', 'L2']:
                stats[f'{length_type}_unary'] += 1
            # 分析Unary规则的头部和body类型
            try:
                head_type = get_head_variable_type(rule)
                if head_type in head_types:
                    body = rule.split('<=', 1)[1].strip()
                    atoms = parse_body_atoms(body)
                    for atom in atoms:
                        try:
                            body_type = get_body_atom_type(atom)
                            stats['unary_matrix'][head_type][body_type] += 1
                        except ValueError as e:
                            print(f"Warning: 无法解析原子 '{atom}' in rule '{rule}': {e}")
                            continue
                else:
                    print(f"Warning: Unary规则的头部类型不在预期范围内: {head_type} in {rule}")
            except ValueError as e:
                print(f"Warning: 无法解析规则头 '{rule}': {e}")
                continue
        # 分析body中原子的关系路径长度
        if '<=' in rule:
            body = rule.split('<=', 1)[1].strip()
            atoms = parse_body_atoms(body)
            for atom in atoms:
                length = get_relation_path_length(atom)
                if length > 0:
                    stats['atom_relation_lengths'][length] += 1
    
    return stats

def write_rule_section(writer, rules_set: Set, rules_dict: Dict, section_title: str, kg=None):
    """
    写入规则示例部分（辅助函数）
    
    Args:
        writer: CSV writer对象
        rules_set: 规则集合
        rules_dict: 规则字典
        section_title: 部分标题
        kg: 知识图谱（可选）
    """
    if not rules_set:
        return
    
    # 分离Binary和Unary规则，并按长度分类Binary规则
    binary_rules = [rule for rule in rules_set if is_binary_rule(rule)]
    unary_rules = [rule for rule in rules_set if not is_binary_rule(rule)]
    
    # 将Binary规则按长度分类
    l1_binary = [r for r in binary_rules if get_rule_length_type(r) == 'L1']
    l2_binary = [r for r in binary_rules if get_rule_length_type(r) == 'L2']
    l3_binary = [r for r in binary_rules if get_rule_length_type(r) == 'L3']
    other_binary = [r for r in binary_rules if get_rule_length_type(r) not in ['L1', 'L2', 'L3']]
    
    # 选择Binary规则：至少3个L1，3个L2，其余补充
    selected_binary = []
    selected_binary.extend(l1_binary[:3])  # 至少3个L1
    selected_binary.extend(l2_binary[:3])  # 至少3个L2
    
    # 补充到10个Binary规则
    remaining_needed = 10 - len(selected_binary)
    if remaining_needed > 0:
        # 从剩余的规则中补充
        remaining_rules = l1_binary[3:] + l2_binary[3:] + l3_binary + other_binary
        selected_binary.extend(remaining_rules[:remaining_needed])
    
    # 选择10个Unary规则
    selected_unary = unary_rules[:10]
    
    # 组合所有选择的规则
    selected_rules = selected_binary + selected_unary
    
    # 写入标题
    writer.writerow([f'{section_title} (共{len(rules_set)}条，展示前{len(selected_rules)}条: 前10条Binary(至少3个L1+3个L2), 后10条Unary)'])
    
    # 写入表头和数据
    if kg is not None:
        writer.writerow(['转换后规则', '指标', '真实结果'])
        for rule in selected_rules:
            simplified_rule = convert_to_simplified_format(rule)
            metrics = rules_dict[rule][0][1] if rules_dict[rule] else {}
            real_result = analyze_rule_from_string(rule, kg) if kg else None
            real_result_str = str(real_result['join_result']) if real_result else 'N/A'
            writer.writerow([simplified_rule, str(metrics), real_result_str])
    else:
        writer.writerow(['转换后规则', '指标'])
        for rule in selected_rules:
            simplified_rule = convert_to_simplified_format(rule)
            metrics = rules_dict[rule][0][1] if rules_dict[rule] else {}
            writer.writerow([simplified_rule, str(metrics)])
    writer.writerow([])

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
        
        # 定义规则类型
        rule_types = [
            ('L0 Unary', 'L0_unary'),
            ('L1 Unary', 'L1_unary'), 
            ('L2 Unary', 'L2_unary'),
            ('Unary', 'unary_rules'),
            ('L0 Binary', 'L0_binary'),
            ('L1 Binary', 'L1_binary'),
            ('L2 Binary', 'L2_binary'),
            ('L3 Binary', 'L3_binary'),
            ('Binary', 'binary_rules')
        ]
        
        # 遍历输出规则类型统计
        for display_name, stat_key in rule_types:
            count1 = stats1[stat_key]
            count2 = stats2[stat_key]
            pct1 = count1 / stats1['total_rules'] * 100
            pct2 = count2 / stats2['total_rules'] * 100
            writer.writerow([display_name,
                            f"{count1} ({pct1:.2f}%)",
                            f"{count2} ({pct2:.2f}%)",
                            count2 - count1])
        writer.writerow([])
        
        # ========== Unary规则矩阵分布 ==========
        if stats1['unary_rules'] > 0 or stats2['unary_rules'] > 0:
            head_types = ['r(X,c)', 'r(c,X)', 'r(X,X)', 'sum']
            body_types = ['rp(X,c)', 'rp(c,X)', 'rp(X,A)', 'rp(A,X)', 'rp(X,X)', 'sum']
            
            # 输出file1的矩阵
            writer.writerow([f'{file1_name} Unary规则矩阵'])
            # 表头
            header = ['head\\body'] + body_types
            writer.writerow(header)
            
            # 计算每行和每列的总和
            matrix1 = stats1.get('unary_matrix', {})
            for head_type in ['r(X,c)', 'r(c,X)', 'r(X,X)']:
                row = [head_type]
                row_sum = 0
                for body_type in ['rp(X,c)', 'rp(c,X)', 'rp(X,A)', 'rp(A,X)', 'rp(X,X)']:
                    count = matrix1.get(head_type, {}).get(body_type, 0)
                    row.append(count)
                    row_sum += count
                row.append(row_sum)  # 行总和
                writer.writerow(row)
            
            # 计算列总和
            sum_row = ['sum']
            total_sum = 0
            for body_type in ['rp(X,c)', 'rp(c,X)', 'rp(X,A)', 'rp(A,X)', 'rp(X,X)']:
                col_sum = sum(matrix1.get(head_type, {}).get(body_type, 0) 
                             for head_type in ['r(X,c)', 'r(c,X)', 'r(X,X)'])
                sum_row.append(col_sum)
                total_sum += col_sum
            sum_row.append(total_sum)  # 总总和
            writer.writerow(sum_row)
            writer.writerow([])
            
            # 输出file2的矩阵
            writer.writerow([f'{file2_name} Unary规则矩阵'])
            # 表头
            writer.writerow(header)
            
            # 计算每行和每列的总和
            matrix2 = stats2.get('unary_matrix', {})
            for head_type in ['r(X,c)', 'r(c,X)', 'r(X,X)']:
                row = [head_type]
                row_sum = 0
                for body_type in ['rp(X,c)', 'rp(c,X)', 'rp(X,A)', 'rp(A,X)', 'rp(X,X)']:
                    count = matrix2.get(head_type, {}).get(body_type, 0)
                    row.append(count)
                    row_sum += count
                row.append(row_sum)  # 行总和
                writer.writerow(row)
            
            # 计算列总和
            sum_row = ['sum']
            total_sum = 0
            for body_type in ['rp(X,c)', 'rp(c,X)', 'rp(X,A)', 'rp(A,X)', 'rp(X,X)']:
                col_sum = sum(matrix2.get(head_type, {}).get(body_type, 0) 
                             for head_type in ['r(X,c)', 'r(c,X)', 'r(X,X)'])
                sum_row.append(col_sum)
                total_sum += col_sum
            sum_row.append(total_sum)  # 总总和
            writer.writerow(sum_row)
            writer.writerow([])

        # ========== Atom中关系路径长度分布 ==========
        writer.writerow(['Atom中关系路径长度分布'])
        writer.writerow(['长度', file1_name, file2_name, '差异'])
        
        total_atoms1 = sum(stats1['atom_relation_lengths'].values())
        total_atoms2 = sum(stats2['atom_relation_lengths'].values())
        
        # 获取所有出现的长度
        all_lengths = sorted(set(stats1['atom_relation_lengths'].keys()) | set(stats2['atom_relation_lengths'].keys()))
        
        for length in all_lengths:
            count1 = stats1['atom_relation_lengths'].get(length, 0)
            count2 = stats2['atom_relation_lengths'].get(length, 0)
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
                writer.writerow(['转换后规则', f'{file1_name}指标', f'{file2_name}指标', '真实结果'])
                for rule in selected_rules:
                    simplified_rule = convert_to_simplified_format(rule)
                    metrics1 = rules1[rule][0][1] if rules1[rule] else {}
                    metrics2 = rules2[rule][0][1] if rules2[rule] else {}
                    real_result = analyze_rule_from_string(rule, kg) if kg else None
                    real_result_str = str(real_result['join_result']) if real_result else 'N/A'
                    writer.writerow([simplified_rule, str(metrics1), str(metrics2), real_result_str])
            else:
                writer.writerow(['转换后规则', f'{file1_name}指标', f'{file2_name}指标'])
                for rule in selected_rules:
                    simplified_rule = convert_to_simplified_format(rule)
                    metrics1 = rules1[rule][0][1] if rules1[rule] else {}
                    metrics2 = rules2[rule][0][1] if rules2[rule] else {}
                    writer.writerow([simplified_rule, str(metrics1), str(metrics2)])
            writer.writerow([])
        
        # ========== 仅在某个文件中的规则 ==========
        # 使用辅助函数处理两个部分
        write_rule_section(writer, only_in_1, rules1, f'仅在{file1_name}中的规则', kg)
        write_rule_section(writer, only_in_2, rules2, f'仅在{file2_name}中的规则', kg)

    print(f"\n统计结果已保存到: {output_file}")

def main(dataset="FB15k-237", file1_name="rules-100-10", file2_name="rule.txt", target_relation=None):
    """
    主函数
    """
    # 文件路径
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file1 = os.path.join(base_dir, "out", dataset, file1_name)
    file2 = os.path.join(base_dir, "out", dataset, file2_name)
    dataset_path = os.path.join(base_dir, "data", dataset, "train.txt")
    
    print("=== 规则文件比较工具 ===")
    print(f"比较文件:")
    print(f"  File 1: {file1}")
    print(f"  File 2: {file2}")
    if target_relation:
        print(f"目标关系: {target_relation}")
    else:
        print("分析模式: 全量规则分析")
    
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
    stats1 = analyze_rule_statistics(rules1)
    stats2 = analyze_rule_statistics(rules2)
    
    set1 = set(rules1.keys())
    set2 = set(rules2.keys())
    
    # 导出到CSV
    output_suffix = "all_rule" if target_relation is None else "rule_" + target_relation.split('/')[-1]
    csv_output_path = os.path.join(base_dir, "out", dataset, f"{output_suffix}_comparison.csv")
    save_statistics_to_csv(stats1, stats2, file1_name, file2_name, set1, set2, rules1, rules2, csv_output_path, kg)

if __name__ == "__main__":
    # 默认进行特定关系分析
    # main(target_relation='/film/film/film_art_direction_by')
    # main(target_relation="/award/award_category/winners./award/award_honor/ceremony")

    # 如果要进行全量分析
    main()