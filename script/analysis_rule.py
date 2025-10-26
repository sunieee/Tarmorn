#!/usr/bin/env python3
"""
规则支持度计算器（增强版）
计算给定规则的 headSize, bodySize 和 support

支持的规则格式：
1. 简写格式：
   /award/award_category/winners./award/award_honor/ceremony <= 
   /award/award_category/winners./award/award_honor/ceremony·/award/award_ceremony/awards_presented./award/award_honor/award_winner·INVERSE_/award/award_ceremony/awards_presented./award/award_honor/award_winner

2. 带括号格式：
   /award/award_category/winners./award/award_honor/ceremony(X,Y) <= 
   /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)

3. 支持单变量规则（一元）和双变量规则（二元）

算法特点：
1. 基于r2h2t索引结构，提供高效的关系查询
2. 自动建立inverse关系索引
3. 使用逐级连接算法计算复合关系路径
4. 通过连接节点优化，避免不必要的笛卡尔积计算
5. 将中间结果存储到r2h2t索引中，支持复用
6. 提供基于连接的高效算法和暴力搜索算法两种计算方式
"""

import os
import json
import re
from collections import defaultdict, Counter
from typing import Set, Tuple, Dict, List, Optional
from itertools import product

# DEBUG控制开关
DEBUG = __name__ == "__main__"

def debug(*args, **kwargs):
    """只在DEBUG模式下打印调试信息"""
    if DEBUG:
        print(*args, **kwargs)

class KnowledgeGraph:
    """知识图谱类，用于存储和查询三元组，基于r2h2t索引"""
    
    def __init__(self):
        # r2h2t索引: relation -> {head: set of tails}
        self.r2h2t = defaultdict(lambda: defaultdict(set))
        # 所有三元组
        self.triples = set()
        # 所有实体
        self.entities = set()
        # 所有关系（包括原始关系和inverse关系）
        self.relations = set()
        # 原始关系集合（用于区分基础关系和缓存的复合关系）
        self.base_relations = set()
    
    def add_triple(self, head: str, relation: str, tail: str):
        """添加三元组到知识图谱"""
        self.triples.add((head, relation, tail))
        self.entities.add(head)
        self.entities.add(tail)
        self.relations.add(relation)
        self.base_relations.add(relation)  # 标记为基础关系
        
        # 建立r2h2t索引
        self.r2h2t[relation][head].add(tail)
        
        # 建立inverse关系索引
        inverse_relation = f"INVERSE_{relation}"
        self.relations.add(inverse_relation)
        self.base_relations.add(inverse_relation)  # 逆关系也是基础关系
        self.r2h2t[inverse_relation][tail].add(head)
    
    def clear_cached_relations(self):
        """清除缓存的复合关系，保留基础关系"""
        # 找出所有非基础关系（即缓存的复合关系）
        cached_relations = [r for r in self.relations if r not in self.base_relations]
        
        # 从r2h2t中删除
        for relation in cached_relations:
            if relation in self.r2h2t:
                del self.r2h2t[relation]
            self.relations.discard(relation)
    
    def get_relation_pairs(self, relation: str) -> Set[Tuple[str, str]]:
        """获取某个关系的所有(head, tail)对"""
        pairs = set()
        if relation in self.r2h2t:
            for head, tails in self.r2h2t[relation].items():
                for tail in tails:
                    pairs.add((head, tail))
        return pairs
    
    def get_relation_instances_count(self, relation: str) -> int:
        """获取某个关系的实例数量"""
        count = 0
        if relation in self.r2h2t:
            for head, tails in self.r2h2t[relation].items():
                count += len(tails)
        return count
    
    @staticmethod
    def get_inverse_relation(relation: str) -> str:
        """
        获取关系的逆关系，支持复合关系路径
        
        Examples:
        - r1 -> INVERSE_r1
        - INVERSE_r1 -> r1
        - r1·r2·r3 -> INVERSE_r3·INVERSE_r2·INVERSE_r1
        - INVERSE_r3·INVERSE_r2·INVERSE_r1 -> r1·r2·r3
        """
        if '·' in relation:
            # 复合关系路径
            parts = relation.split('·')
            inverse_parts = []
            
            for part in reversed(parts):
                if part.startswith("INVERSE_"):
                    # 如果已经是逆关系，返回原关系
                    inverse_parts.append(part[8:])
                else:
                    # 如果是正向关系，返回逆关系
                    inverse_parts.append(f"INVERSE_{part}")
            
            return '·'.join(inverse_parts)
        else:
            # 单个关系
            if relation.startswith("INVERSE_"):
                # 如果已经是逆关系，返回原关系
                return relation[8:]
            else:
                # 如果是正向关系，返回逆关系
                return f"INVERSE_{relation}"
    
    def save_r2h2t_to_json(self, filepath: str):
        """将r2h2t索引保存到JSON文件"""
        debug(f"正在保存r2h2t索引到文件: {filepath}")
        
        # 转换r2h2t为可序列化的格式
        serializable_r2h2t = {}
        for relation, h2t_dict in self.r2h2t.items():
            serializable_r2h2t[relation] = {}
            for head, tails in h2t_dict.items():
                serializable_r2h2t[relation][head] = list(tails)
        
        # 保存到JSON文件
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(serializable_r2h2t, f, ensure_ascii=False, indent=2)
        
        debug(f"r2h2t索引已保存，包含 {len(serializable_r2h2t)} 个关系")


class RuleParser:
    """规则解析器，支持多种规则格式和简写"""
    
    @staticmethod
    def parse_rule(rule_str: str) -> Tuple[str, List[str], int, Dict]:
        """
        解析规则字符串，将所有规则转换为简写模式进行统一处理
        
        统一规则格式为简写模式：
        - 一元规则：/rel(/m/const) <= /rel1·/rel2(/m/const2)
        - 二元规则：/rel <= /rel1·INVERSE_/rel2
        
        Args:
            rule_str: 规则字符串
            
        Returns:
            (head_relation, body_relations, variable_count, rule_info)
        """
        if '<=' not in rule_str:
            raise ValueError("规则格式错误：缺少 '<='")
        
        head_part, body_part = rule_str.split('<=', 1)
        head_part = head_part.strip()
        body_part = body_part.strip()
        
        # 首先检测规则类型和转换为简写模式
        normalized_rule = RuleParser._normalize_to_simplified(head_part, body_part)
        
        rule_info = {
            'original_rule': rule_str,
            'normalized_rule': normalized_rule,
            'is_simplified': True  # 统一为简写模式
        }
        
        # 解析规范化后的简写规则
        norm_head, norm_body = normalized_rule.split('<=', 1)
        norm_head = norm_head.strip()
        norm_body = norm_body.strip()
        
        return RuleParser._parse_simplified_rule(norm_head, norm_body, rule_info)
    
    @staticmethod
    def _normalize_to_simplified(head_part: str, body_part: str) -> str:
        """
        将完整格式的规则转换为简写格式
        
        转换规则：
        1. 一元规则：rel(X,/m/const) <= body1(X,A), body2(A,/m/const2)
           -> rel(/m/const) <= body_path(/m/const2) 或 INVERSE_rel(/m/const) <= body_path(/m/const2)
        
        2. 二元规则：rel(X,Y) <= body1(X,A), body2(Y,A)  
           -> rel <= body_path
        
        Args:
            head_part: 头部字符串
            body_part: 身体字符串
            
        Returns:
            规范化后的简写规则字符串
        """
        # 检查是否已经是简写格式
        if '(' not in head_part or ')' not in head_part:
            # 二元规则简写格式
            return f"{head_part} <= {body_part}"
        
        # 检查头部是否有逗号
        paren_content = head_part.split('(')[1].split(')')[0]
        if ',' not in paren_content:
            # 一元规则简写格式
            return f"{head_part} <= {body_part}"
        
        # 完整格式，需要转换
        debug(f"[DEBUG] Converting to simplified format: {head_part} <= {body_part}")
        
        # 解析头部
        head_relation = head_part.split('(')[0].strip()
        head_args = [arg.strip() for arg in paren_content.split(',')]
        
        # 解析身体原子
        body_atoms = RuleParser._parse_body_atoms(body_part)
        
        # 分析变量
        all_vars = set()
        constants = set()
        for arg in head_args:
            if len(arg) == 1:  # 变量
                all_vars.add(arg)
            else:  # 常量
                constants.add(arg)
        
        for atom in body_atoms:
            if '(' in atom and ')' in atom:
                atom_args = RuleParser._extract_variables(atom)
                for arg in atom_args:
                    if len(arg) == 1:  # 变量
                        all_vars.add(arg)
                    else:  # 常量
                        constants.add(arg)
        
        # 确定规则类型
        # 自由变量就是头部参数中的单字母变量
        free_vars_count = sum(1 for arg in head_args if len(arg) == 1)
        
        if free_vars_count == 1:
            # 一元规则：头部有一个变量和一个常量
            return RuleParser._convert_unary_to_simplified(head_relation, head_args, body_atoms)
        else:
            # 二元规则：头部有两个变量
            return RuleParser._convert_binary_to_simplified(head_relation, head_args, body_atoms)
    
    @staticmethod
    def _convert_unary_to_simplified(head_relation: str, head_args: List[str], 
                                   body_atoms: List[str]) -> str:
        """
        将一元规则转换为简写格式
        
        例如：rel(X,/m/const) <= body1(X,A), body2(A,/m/const2)
        转换为：rel(/m/const) <= body_path(/m/const2) 或 INVERSE_rel(/m/const) <= body_path(/m/const2)
        
        一元规则的自由变量固定是X（头部中唯一的单字母变量）
        """
        # 找到自由变量和常量
        free_var = None
        head_constant = None
        free_var_pos_in_head = -1
        
        for i, arg in enumerate(head_args):
            if len(arg) == 1:  # 变量
                free_var = arg
                free_var_pos_in_head = i
            else:  # 常量
                head_constant = arg
        
        debug(f"[DEBUG] Unary conversion: free_var={free_var}, pos={free_var_pos_in_head}, constant={head_constant}")
        
        # 构建body路径
        body_path, body_constant = RuleParser._build_unary_body_path(body_atoms, free_var)
        
        # 确定头部的简写形式
        if free_var_pos_in_head == 0:
            # rel(X, /m/const) -> rel(/m/const)
            simplified_head = f"{head_relation}({head_constant})"
        else:
            # rel(/m/const, X) -> INVERSE_rel(/m/const)
            simplified_head = f"INVERSE_{head_relation}({head_constant})"
        
        # 构建完整的简写规则
        # 一元规则的body部分需要带括号：
        # - 如果有body常量，格式为 body_path(constant)
        # - 如果没有body常量（只有中间变量），格式为 body_path(·)，表示有中间变量
        if body_constant:
            simplified_body = f"{body_path}({body_constant})"
        else:
            # 检查是否有中间变量（body原子数 > 1，或者单个原子中有非自由变量）
            has_intermediate_var = len(body_atoms) > 1
            if not has_intermediate_var and len(body_atoms) == 1:
                # 单个原子，检查是否有中间变量
                atom = body_atoms[0]
                args = RuleParser._extract_variables(atom)
                # 如果有两个参数且都是变量（单字母），说明有中间变量
                has_intermediate_var = len(args) == 2 and all(len(arg) == 1 for arg in args)
            
            if has_intermediate_var:
                simplified_body = f"{body_path}(·)"
            else:
                simplified_body = body_path
        
        result = f"{simplified_head} <= {simplified_body}"
        debug(f"[DEBUG] Unary simplified result: {result}")
        return result
    
    @staticmethod
    def _build_unary_body_path(body_atoms: List[str], free_var: str) -> Tuple[str, Optional[str]]:
        """
        构建一元规则的body路径
        
        分析body原子中的连接方式，确定正确的关系路径和常量
        
        例如：body1(X,A), body2(A,/m/const) 
        -> X通过A连接到/m/const，路径为 body1·body2，常量为/m/const
        
        Args:
            body_atoms: body原子列表
            free_var: 自由变量
            
        Returns:
            (body_path, body_constant)
        """
        if not body_atoms:
            return "", None
        
        # 解析每个原子
        parsed_atoms = []
        body_constant = None
        
        for atom in body_atoms:
            relation = RuleParser._extract_relation_from_atom(atom)
            args = RuleParser._extract_variables(atom)
            parsed_atoms.append({'relation': relation, 'args': args})
            
            # 查找常量
            for arg in args:
                if len(arg) > 1:  # 常量
                    body_constant = arg
        
        debug(f"[DEBUG] Building unary body path: atoms={[(a['relation'], a['args']) for a in parsed_atoms]}")
        debug(f"[DEBUG] Free var: {free_var}, Body constant: {body_constant}")
        
        if len(parsed_atoms) == 1:
            # 单个原子
            atom = parsed_atoms[0]
            args = atom['args']
            
            # 确定自由变量的位置
            free_var_pos = -1
            for i, arg in enumerate(args):
                if arg == free_var:
                    free_var_pos = i
                    break
            
            if free_var_pos == 0:
                # 自由变量在head位置，直接使用关系
                return atom['relation'], body_constant
            else:
                # 自由变量在tail位置，使用逆关系
                return f"INVERSE_{atom['relation']}", body_constant
        
        # 多个原子，需要分析连接
        return RuleParser._analyze_unary_connection(parsed_atoms, free_var, body_constant)
    
    @staticmethod
    def _analyze_unary_connection(parsed_atoms: List[Dict], free_var: str, body_constant: str) -> Tuple[str, str]:
        """
        分析一元规则中多个原子的连接方式
        
        目标：构建从自由变量到常量的路径
        
        例如：body1(X,A), body2(A,/m/const)
        - X在body1的位置0，A在位置1
        - A在body2的位置0，常量在位置1  
        - 连接：X -> A -> /m/const
        - 路径：body1 ∘ body2
        
        Args:
            parsed_atoms: 解析后的原子列表
            free_var: 自由变量
            body_constant: body中的常量
            
        Returns:
            (relation_path, constant)
        """
        if len(parsed_atoms) <= 1:
            atom = parsed_atoms[0]
            return atom['relation'], body_constant
        
        # 找到包含自由变量的原子（起始原子）
        start_atom_idx = -1
        for i, atom in enumerate(parsed_atoms):
            if free_var in atom['args']:
                start_atom_idx = i
                break
        
        if start_atom_idx == -1:
            # 没找到包含自由变量的原子，使用第一个
            start_atom_idx = 0
        
        # 从起始原子开始构建路径
        path_relations = []
        current_atom = parsed_atoms[start_atom_idx]
        used_atoms = {start_atom_idx}
        
        # 确定自由变量在起始原子中的位置
        free_var_pos = -1
        for i, arg in enumerate(current_atom['args']):
            if arg == free_var:
                free_var_pos = i
                break
        
        # 根据自由变量位置决定是否需要逆关系
        if free_var_pos == 0:
            # 自由变量在head位置，直接使用关系
            path_relations.append(current_atom['relation'])
            current_var = current_atom['args'][1]  # 连接变量
        else:
            # 自由变量在tail位置，使用逆关系
            path_relations.append(f"INVERSE_{current_atom['relation']}")
            current_var = current_atom['args'][0]  # 连接变量
        
        debug(f"[DEBUG] Starting path with {path_relations[0]}, current_var={current_var}")
        
        # 继续连接剩余原子
        while len(used_atoms) < len(parsed_atoms):
            found_next = False
            
            for i, atom in enumerate(parsed_atoms):
                if i in used_atoms:
                    continue
                
                if current_var in atom['args']:
                    used_atoms.add(i)
                    
                    # 确定连接变量在当前原子中的位置
                    var_pos = atom['args'].index(current_var)
                    
                    if var_pos == 0:
                        # 连接变量在head位置，直接使用关系
                        path_relations.append(atom['relation'])
                        # 下一个变量是tail位置的变量
                        next_var = atom['args'][1] if len(atom['args']) > 1 else None
                    else:
                        # 连接变量在tail位置，使用逆关系
                        path_relations.append(f"INVERSE_{atom['relation']}")
                        # 下一个变量是head位置的变量
                        next_var = atom['args'][0]
                    
                    debug(f"[DEBUG] Added {path_relations[-1]}, next_var={next_var}")
                    current_var = next_var
                    found_next = True
                    break
            
            if not found_next:
                break
        
        # 构建最终路径
        final_path = '·'.join(path_relations)
        debug(f"[DEBUG] Final unary path: {final_path}")
        
        return final_path, body_constant
    
    @staticmethod
    def _convert_binary_to_simplified(head_relation: str, head_args: List[str], 
                                    body_atoms: List[str]) -> str:
        """
        将二元规则转换为简写格式
        
        例如：rel(X,Y) <= body1(X,A), body2(Y,A)
        转换为：rel <= body1·INVERSE_body2
        
        二元规则的自由变量固定是 head_args（即 X, Y，顺序确定）
        """
        # 提取自由变量（头部中的单字母变量）
        free_vars = [arg for arg in head_args if len(arg) == 1]
        
        if len(free_vars) != 2:
            # 如果不是严格的二元规则，返回原始格式
            return f"{head_relation}({','.join(head_args)}) <= {', '.join(body_atoms)}"
        
        # 构建body路径，传入有序的自由变量列表
        body_path = RuleParser._build_binary_body_path(body_atoms, free_vars)
        
        result = f"{head_relation} <= {body_path}"
        debug(f"[DEBUG] Binary simplified result: {result}")
        return result
    
    @staticmethod
    def _build_binary_body_path(body_atoms: List[str], free_vars: List[str]) -> str:
        """
        构建二元规则的body路径
        
        分析连接模式，构建正确的关系路径
        例如：
        - body1(X,A), body2(Y,A) -> body1·INVERSE_body2 (X->A<-Y)
        - body1(X,A), body2(A,B), body3(Y,B) -> body1·body2·INVERSE_body3 (X->A->B<-Y)
        """
        if not body_atoms:
            return ""
        
        # 解析原子
        parsed_atoms = []
        for atom in body_atoms:
            relation = RuleParser._extract_relation_from_atom(atom)
            args = RuleParser._extract_variables(atom)
            parsed_atoms.append({'relation': relation, 'args': args})
        
        debug(f"[DEBUG] Building binary body path: atoms={[(a['relation'], a['args']) for a in parsed_atoms]}")
        debug(f"[DEBUG] Free vars: {free_vars}")
        
        if len(parsed_atoms) == 1:
            # 单个原子，提取关系部分
            atom = parsed_atoms[0]
            # 二元规则的单个原子，直接返回关系（可能需要加INVERSE）
            if len(free_vars) == 2:
                X, Y = free_vars[0], free_vars[1]
                args = atom['args']
                # 检查参数顺序是否匹配
                if args[0] == X and args[1] == Y:
                    # 顺序匹配，直接使用关系
                    return atom['relation']
                elif args[0] == Y and args[1] == X:
                    # 顺序相反，使用逆关系
                    return f"INVERSE_{atom['relation']}"
                else:
                    # 其他情况，直接返回关系
                    return atom['relation']
            return atom['relation']
        
        if len(free_vars) != 2:
            # 简化处理：直接连接所有关系
            return '·'.join([atom['relation'] for atom in parsed_atoms])
        
        # 分析连接模式：从X开始，找到到Y的路径
        X, Y = free_vars[0], free_vars[1]
        
        # 找到包含X的原子作为起点
        start_atom_idx = -1
        for i, atom in enumerate(parsed_atoms):
            if X in atom['args']:
                start_atom_idx = i
                break
        
        if start_atom_idx == -1:
            # 没找到包含X的原子，使用简化处理
            return '·'.join([atom['relation'] for atom in parsed_atoms])
        
        # 构建从X到Y的连接路径
        path_relations = []
        current_atom = parsed_atoms[start_atom_idx]
        used_atoms = {start_atom_idx}
        
        # 确定X在起始原子中的位置
        x_pos = current_atom['args'].index(X)
        
        if x_pos == 0:
            # X在head位置，直接使用关系
            path_relations.append(current_atom['relation'])
            current_var = current_atom['args'][1]  # 连接变量
        else:
            # X在tail位置，使用逆关系
            path_relations.append(f"INVERSE_{current_atom['relation']}")
            current_var = current_atom['args'][0]  # 连接变量
        
        debug(f"[DEBUG] Starting from {X} with {path_relations[0]}, current_var={current_var}")
        
        # 继续连接直到找到Y
        while current_var != Y and len(used_atoms) < len(parsed_atoms):
            found_next = False
            
            for i, atom in enumerate(parsed_atoms):
                if i in used_atoms:
                    continue
                
                if current_var in atom['args']:
                    used_atoms.add(i)
                    
                    # 确定连接变量在当前原子中的位置
                    var_pos = atom['args'].index(current_var)
                    
                    if var_pos == 0:
                        # 连接变量在head位置，直接使用关系
                        path_relations.append(atom['relation'])
                        next_var = atom['args'][1]
                    else:
                        # 连接变量在tail位置，使用逆关系
                        path_relations.append(f"INVERSE_{atom['relation']}")
                        next_var = atom['args'][0]
                    
                    debug(f"[DEBUG] {current_var} -> {next_var} via {path_relations[-1]}")
                    current_var = next_var
                    found_next = True
                    break
            
            if not found_next:
                debug(f"[DEBUG] Cannot find connection from {current_var}")
                break
        
        final_path = '·'.join(path_relations)
        debug(f"[DEBUG] Final binary path: {final_path}")
        
        return final_path

    @staticmethod
    def _parse_simplified_rule(head_part: str, body_part: str, rule_info: Dict) -> Tuple[str, List[str], int, Dict]:
        """解析简写格式规则，使用统一的数据结构"""
        # 提取头部关系和可能的固定实体
        head_relation, fixed_entity, var_pos = RuleParser._parse_simplified_head(head_part)
        
        # 解析身体关系，可能包含实体约束
        body_relations, body_constant = RuleParser._parse_simplified_body(body_part)
        
        # 确定规则类型
        if fixed_entity or body_constant:
            # 一元规则：有固定实体
            variable_count = 1
            rule_info.update({
                'is_unary': True,
                'variable_count': 1,
                'head_relation': head_relation,
                'head_constant': fixed_entity,
                'body_relations': body_relations,
                'body_constant': body_constant,
                'free_variable': 'X'  # 统一使用X作为自由变量名
            })
            
            # 构建标准的head_atom结构
            rule_info['head_atom'] = {
                'relation': head_relation,
                'args': ['X', fixed_entity] if not head_relation.startswith('INVERSE_') else [fixed_entity, 'X']
            }
            rule_info['head_variables'] = ['X', fixed_entity] if not head_relation.startswith('INVERSE_') else [fixed_entity, 'X']
            rule_info['free_variables'] = ['X']
        else:
            # 二元规则：没有固定实体
            variable_count = 2
            rule_info.update({
                'is_unary': False,
                'variable_count': 2,
                'head_relation': head_relation,
                'head_constant': None,
                'body_relations': body_relations,
                'body_constant': None,
                'free_variables': ['X', 'Y']
            })
            
            rule_info['head_atom'] = {
                'relation': head_relation,
                'args': ['X', 'Y']
            }
            rule_info['head_variables'] = ['X', 'Y']
        
        return head_relation, body_relations, variable_count, rule_info
    
    @staticmethod
    def _parse_simplified_body(body_part: str) -> Tuple[List[str], Optional[str]]:
        """
        解析简写格式的body部分
        
        例如：/rel1·/rel2(/m/entity) 
        返回：(["/rel1", "/rel2"], "/m/entity")
        """
        body_constant = None
        
        # 检查是否有括号约束
        if '(' in body_part and ')' in body_part:
            # 找到最后一个括号，提取约束实体
            last_paren_start = body_part.rfind('(')
            last_paren_end = body_part.rfind(')')
            
            if last_paren_start < last_paren_end:
                entity_part = body_part[last_paren_start+1:last_paren_end].strip()
                if entity_part.startswith('/m/'):
                    body_constant = entity_part
                    # 移除括号部分
                    body_part = body_part[:last_paren_start].strip()
        
        # 解析关系路径
        if '·' in body_part:
            body_relations = [rel.strip() for rel in body_part.split('·')]
        else:
            body_relations = [body_part.strip()]
        
        return body_relations, body_constant
    
    @staticmethod
    def _parse_full_rule(head_part: str, body_part: str, rule_info: Dict) -> Tuple[str, List[str], int, Dict]:
        """解析完整格式规则，正确处理变量绑定模式"""
        # 提取头部关系和变量
        head_relation = RuleParser._extract_relation_from_atom(head_part)
        head_variables = RuleParser._extract_variables(head_part)
        
        # 解析身体原子
        body_atoms = RuleParser._parse_body_atoms(body_part)
        
        # 分析变量类型
        all_body_variables = []
        for atom in body_atoms:
            variables = RuleParser._extract_variables(atom)
            all_body_variables.extend(variables)
        
        free_vars, constraint_vars = RuleParser._analyze_variables(head_variables, all_body_variables)
        
        # 修正变量计数：考虑头部是否包含常量
        has_constant_in_head = any(len(var) > 1 for var in head_variables)
        if has_constant_in_head and len(free_vars) == 1:
            # 一元规则：头部有一个变量和一个常量
            variable_count = 1
        elif len(free_vars) == 2 and not has_constant_in_head:
            # 二元规则：头部有两个变量
            variable_count = 2
        else:
            # 其他情况，直接使用自由变量的数量
            variable_count = len(free_vars)
        
        debug(f"  [DEBUG] Head variables: {head_variables}, Free vars: {free_vars}, Has constant: {has_constant_in_head}, Variable count: {variable_count}")
        
        # 存储详细的原子信息
        parsed_body_atoms = []
        for atom in body_atoms:
            relation = RuleParser._extract_relation_from_atom(atom)
            variables = RuleParser._extract_variables(atom)
            parsed_body_atoms.append({
                'relation': relation,
                'args': variables,
                'original': atom
            })
        
        # 构建head_atom结构
        head_variables = RuleParser._extract_variables(head_part)
        rule_info['head_atom'] = {
            'relation': head_relation,
            'args': head_variables
        }
        
        rule_info['body_atoms'] = parsed_body_atoms
        rule_info['head_variables'] = head_variables
        # 保持自由变量的顺序与头部变量一致（按照head_variables中的出现顺序）
        rule_info['free_variables'] = [v for v in head_variables if v in free_vars]
        rule_info['constraint_variables'] = sorted(list(constraint_vars))  # 约束变量按字母顺序排序
        rule_info['variable_count'] = variable_count
        
        # 对于二元规则，构建连接路径
        if variable_count == 2:
            body_relations = RuleParser._build_connection_path(head_variables, parsed_body_atoms)
        else:
            # 一元规则或其他情况，直接提取关系名
            body_relations = [atom['relation'] for atom in parsed_body_atoms]
        
        # 将body_relations存储到rule_info中
        rule_info['body_relations'] = body_relations
        
        if variable_count == 1:
            rule_info['is_unary'] = True
            # 找到固定实体和变量位置
            for i, var in enumerate(head_variables):
                if len(var) > 1:  # 常量 (实体)
                    rule_info['fixed_entity'] = var
                    rule_info['variable_position'] = 'tail' if i == 1 else 'head'
                    break
        
        return head_relation, body_relations, variable_count, rule_info
    
    @staticmethod
    def _build_connection_path(head_variables: List[str], body_atoms: List[Dict]) -> List[str]:
        """
        根据变量绑定模式构建连接路径
        
        对于二元规则 Head(X,Y) <= Body1(...), Body2(...), ...
        需要找到从X到Y的连接路径
        """
        if len(head_variables) != 2:
            return [atom['relation'] for atom in body_atoms]
        
        X, Y = head_variables[0], head_variables[1]
        
        debug(f"[DEBUG] Building connection path for X={X}, Y={Y}")
        debug(f"[DEBUG] Body atoms: {[(atom['relation'], atom['args']) for atom in body_atoms]}")
        
        # 简化实现：假设是链式连接
        # 找到包含X的原子作为起点
        path_relations = []
        
        current_var = X
        used_atoms = set()
        
        while current_var != Y and len(used_atoms) < len(body_atoms):
            found_next = False
            
            for i, atom in enumerate(body_atoms):
                if i in used_atoms:
                    continue
                
                relation = atom['relation']
                args = atom['args']
                
                if current_var in args:
                    used_atoms.add(i)
                    
                    # 确定变量在关系中的位置，以及下一个变量
                    if args[0] == current_var:
                        # current_var在第一个位置
                        next_var = args[1]
                        # 使用正向关系：current_var -> next_var
                        path_relations.append(relation)
                    else:
                        # current_var在第二个位置
                        next_var = args[0]
                        # 使用反向关系：current_var <- next_var (即 next_var -> current_var)
                        path_relations.append(f"INVERSE_{relation}")
                    
                    debug(f"[DEBUG] {current_var} -> {next_var} via {path_relations[-1]}")
                    current_var = next_var
                    found_next = True
                    break
            
            if not found_next:
                debug(f"[DEBUG] Cannot find connection from {current_var}")
                break
        
        debug(f"[DEBUG] Final connection path: {path_relations}")
        return path_relations
    
    @staticmethod
    def _parse_simplified_head(head_part: str) -> Tuple[str, Optional[str], Optional[str]]:
        """
        解析简写格式的头部
        
        对于INVERSE_/rel(/m/entity)，等价于/rel(/m/entity, X)，实体在head位置
        对于/rel(/m/entity)，等价于/rel(X, /m/entity)，实体在tail位置
        
        Returns:
            (relation, fixed_entity, variable_position)
        """
        if '(' in head_part and ')' in head_part:
            # 有括号，可能是一元规则：/rel(/m/123) 或 INVERSE_/rel(/m/123)
            relation = head_part.split('(')[0].strip()
            entity_part = head_part.split('(')[1].split(')')[0].strip()
            
            if entity_part.startswith('/m/'):
                if relation.startswith('INVERSE_'):
                    # INVERSE_/rel(/m/entity) 等价于 /rel(/m/entity, X)
                    # 实体在head位置，变量在tail位置
                    return relation, entity_part, 'head'
                else:
                    # /rel(/m/entity) 等价于 /rel(X, /m/entity)
                    # 实体在tail位置，变量在head位置
                    return relation, entity_part, 'tail'
            else:
                # 没有固定实体，或者格式不对
                return relation, None, None
        else:
            # 没有括号，二元规则：/rel
            return head_part.strip(), None, None
    
    @staticmethod
    def _analyze_variables(head_variables: List[str], body_variables: List[str]) -> Tuple[Set[str], Set[str]]:
        """分析变量类型，区分自由变量和约束变量"""
        head_var_set = set(head_variables)
        body_var_set = set(body_variables)
        
        # 自由变量：X, Y (长度为1且出现在头部)
        free_variables = {var for var in head_var_set if len(var) == 1}
        
        # 约束变量：长度为1但不在头部的变量 (如A, B, C等)
        constraint_variables = set()
        for var in body_var_set:
            if len(var) == 1 and var not in head_var_set:
                constraint_variables.add(var)
        
        return free_variables, constraint_variables
    
    @staticmethod
    def _extract_relation_from_atom(atom: str) -> str:
        """从原子中提取关系名"""
        if '(' in atom:
            return atom.split('(')[0].strip()
        else:
            return atom.strip()
    
    @staticmethod
    def _extract_variables(atom: str) -> List[str]:
        """从原子中提取变量"""
        if '(' not in atom or ')' not in atom:
            return []
        
        var_part = atom.split('(')[1].split(')')[0]
        variables = [v.strip() for v in var_part.split(',')]
        return variables
    
    @staticmethod
    def _parse_body_atoms(body_part: str) -> List[str]:
        """解析身体部分的原子列表"""
        atoms = []
        current_atom = ""
        paren_count = 0
        
        for char in body_part:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                atoms.append(current_atom.strip())
                current_atom = ""
                continue
            
            current_atom += char
        
        if current_atom.strip():
            atoms.append(current_atom.strip())
        
        return atoms


class RuleSupportCalculator:
    """规则支持度计算器，基于r2h2t索引和逐级连接算法"""
    
    def __init__(self, kg: KnowledgeGraph):
        self.kg = kg
    
    def get_binary_instances_join(self, relation_path: List[str]) -> Set[Tuple[str, str]]:
        """
        使用连接算法获取二元规则的实例集合
        
        Args:
            relation_path: 关系路径列表
        
        Returns:
            (X, Y) 对的集合
        """
        debug(f"    [DEBUG] get_binary_instances_join: relation_path={relation_path}")
        
        if not relation_path:
            return set()
        
        if len(relation_path) == 1:
            result = self.kg.get_relation_pairs(relation_path[0])
            debug(f"    [DEBUG] Single relation {relation_path[0]} has {len(result)} instances")
            return result
        
        # 逐级连接关系
        current_relation = relation_path[-1]
        for i in range(len(relation_path) - 2, -1, -1):
            debug(f"    [DEBUG] Joining {relation_path[i]} with {current_relation}")
            current_relation = self.join_relations(relation_path[i], current_relation)
        
        result = self.kg.get_relation_pairs(current_relation)
        debug(f"    [DEBUG] Final joined relation {current_relation} has {len(result)} instances")
        return result
    
    def join_relations(self, r1: str, r2: str) -> str:
        """
        连接两个关系：r1 · r2
        返回新关系的名称，并将结果存储到kg.r2h2t中
        """
        new_relation_name = f"{r1}·{r2}"
        
        # 如果已经计算过，直接返回
        if new_relation_name in self.kg.r2h2t:
            debug(f"      [DEBUG] Using cached join result: {new_relation_name}")
            return new_relation_name
        
        debug(f"      [DEBUG] Computing join: {r1} · {r2}")
        
        # 获取r1和r2的r2h2t映射
        r1_h2t = self.kg.r2h2t.get(r1, {})
        r2_h2t = self.kg.r2h2t.get(r2, {})
        
        debug(f"      [DEBUG] r1 ({r1}) has {sum(len(tails) for tails in r1_h2t.values())} instances")
        debug(f"      [DEBUG] r2 ({r2}) has {sum(len(tails) for tails in r2_h2t.values())} instances")
        
        # 高效获取连接节点：r1的所有tail ∩ r2的所有head
        inverse_r1 = self.kg.get_inverse_relation(r1)
        r1_tails = set(self.kg.r2h2t.get(inverse_r1, {}).keys())
        r2_heads = set(r2_h2t.keys())
        connection_nodes = r1_tails.intersection(r2_heads)
        
        debug(f"      [DEBUG] r1 tails: {len(r1_tails)}, r2 heads: {len(r2_heads)}, connection nodes: {len(connection_nodes)}")
        
        # 对每个连接节点计算笛卡尔积
        result_h2t = defaultdict(set)  # r1·r2 的正向索引
        result_t2h = defaultdict(set)  # r1·r2 的逆向索引
        total_results = 0
        for node in connection_nodes:
            inverse_r1 = self.kg.get_inverse_relation(r1)
            r1_heads_to_node = self.kg.r2h2t.get(inverse_r1, {}).get(node, set())
            r2_tails_from_node = r2_h2t.get(node, set())
            
            for h1 in r1_heads_to_node:
                for t2 in r2_tails_from_node:
                    if h1 != t2:  # 规则语义：不允许 X == Y
                        result_h2t[h1].add(t2)  # 正向：h1 -> t2
                        result_t2h[t2].add(h1)  # 逆向：t2 -> h1
                        total_results += 1
        
        debug(f"      [DEBUG] Join result: {total_results} instances")
        
        # 将结果存储到kg的r2h2t中
        self.kg.r2h2t[new_relation_name] = result_h2t
        self.kg.relations.add(new_relation_name)
        
        # 同时存储逆关系
        inverse_r2 = self.kg.get_inverse_relation(r2)
        inverse_r1 = self.kg.get_inverse_relation(r1)
        inverse_relation_name = f"{inverse_r2}·{inverse_r1}"
        
        self.kg.r2h2t[inverse_relation_name] = result_t2h
        self.kg.relations.add(inverse_relation_name)
        
        debug(f"      [DEBUG] Also computed inverse relation: {inverse_relation_name}")
        
        return new_relation_name
    
    def get_unary_instances_bruteforce(self, rule_info: Dict) -> Set[str]:
        """
        使用暴力算法获取一元规则的实例集合
        
        Args:
            rule_info: 规则信息字典
        
        Returns:
            变量的所有可能值的集合
        """
        body_atoms = rule_info['body_atoms']
        variable = rule_info['free_variables'][0]
        
        # 从所有实体开始
        candidates = set(self.kg.entities)
        
        # 对每个body原子进行过滤
        for atom in body_atoms:
            relation = atom['relation']
            args = atom['args']
            
            valid_values = set()
            
            # 获取该关系的所有实例
            relation_instances = self.kg.get_relation_pairs(relation)
            
            for head, tail in relation_instances:
                # 检查是否满足当前原子的约束
                if self._matches_atom_pattern(head, tail, args, variable):
                    # 提取变量的值
                    if args[0] == variable:
                        valid_values.add(head)
                    elif args[1] == variable:
                        valid_values.add(tail)
            
            candidates = candidates.intersection(valid_values)
        
        return candidates
    
    def get_binary_instances_bruteforce(self, rule_info: Dict) -> Set[Tuple[str, str]]:
        """
        使用暴力算法获取二元规则的实例集合
        
        Args:
            rule_info: 规则信息字典
        
        Returns:
            (X, Y) 对的集合
        """
        body_atoms = rule_info['body_atoms']
        free_vars = rule_info['free_variables']
        
        if len(free_vars) != 2:
            return set()
        
        var_x, var_y = free_vars
        valid_pairs = set()
        
        # 暴力枚举所有可能的 (X, Y) 组合
        for x in self.kg.entities:
            for y in self.kg.entities:
                if x == y:
                    continue
                
                # 检查这个 (x, y) 是否满足所有body原子
                satisfies_all = True
                
                for atom in body_atoms:
                    if not self._satisfies_atom(atom, {var_x: x, var_y: y}):
                        satisfies_all = False
                        break
                
                if satisfies_all:
                    valid_pairs.add((x, y))
        
        return valid_pairs
    
    def _matches_atom_pattern(self, head: str, tail: str, args: List[str], variable: str) -> bool:
        """检查三元组是否匹配原子模式"""
        for i, arg in enumerate(args):
            if arg != variable and not arg.startswith('/m/'):  # 不是变量也不是常量
                continue
            
            if i == 0 and arg != variable and arg != head:
                return False
            elif i == 1 and arg != variable and arg != tail:
                return False
        
        return True
    
    def _satisfies_atom(self, atom: Dict, variable_assignment: Dict[str, str]) -> bool:
        """检查变量赋值是否满足原子"""
        relation = atom['relation']
        args = atom['args']
        
        # 替换变量
        head_val = variable_assignment.get(args[0], args[0])
        tail_val = variable_assignment.get(args[1], args[1])
        
        # 检查这个三元组是否存在
        return (head_val, relation, tail_val) in self.kg.triples
    
    def calculate_rule_support_join(self, rule_info: Dict) -> Dict:
        """
        使用连接算法计算规则支持度
        
        Args:
            rule_info: 规则信息字典
        
        Returns:
            包含 headSize, bodySize, support, confidence 的字典
        """
        debug(f"[DEBUG] rule_info keys: {list(rule_info.keys())}")
        debug(f"[DEBUG] variable_count: {rule_info.get('variable_count', 'NOT_SET')}")
        
        variable_count = rule_info.get('variable_count', 0)
        
        # 统一计算head和body实例集合
        head_instances = self._get_head_instances(rule_info)
        body_instances = self._get_body_instances(rule_info)
        
        head_size = len(head_instances)
        body_size = len(body_instances)
        
        debug(f"  [DEBUG] Head instances: {head_size}")
        if head_instances:
            head_sample = list(head_instances)[:5]
            debug(f"  [DEBUG] Head sample: {head_sample}")
            
        debug(f"  [DEBUG] Body instances: {body_size}")
        if body_instances:
            body_sample = list(body_instances)[:5]
            debug(f"  [DEBUG] Body sample: {body_sample}")
        
        # 计算支持度
        support_instances = head_instances.intersection(body_instances)
        support = len(support_instances)
        confidence = support / body_size if body_size > 0 else 0
        
        debug(f"  [DEBUG] Support: {support}, Confidence: {confidence}")
        
        return {
            'headSize': head_size,
            'bodySize': body_size,
            'support': support,
            'confidence': confidence
        }
    
    def _get_head_instances(self, rule_info: Dict) -> Set:
        """获取头部实例集合 - 使用统一的简写格式处理"""
        head_relation = rule_info.get('head_relation')
        variable_count = rule_info.get('variable_count', 0)
        
        if variable_count == 1:
            # 一元规则：返回变量的所有可能值
            head_constant = rule_info.get('head_constant')
            
            debug(f"  [DEBUG] Head relation: {head_relation}, constant: {head_constant}")
            
            if head_relation.startswith('INVERSE_'):
                # INVERSE_relation(constant) 表示 relation(constant, X)
                # 要获取X的值，查找 relation[constant]
                original_relation = head_relation[8:]  # 去掉INVERSE_前缀
                debug(f"  [DEBUG] Looking for {original_relation}[{head_constant}]")
                if original_relation in self.kg.r2h2t and head_constant in self.kg.r2h2t[original_relation]:
                    result = set(self.kg.r2h2t[original_relation][head_constant])
                    debug(f"  [DEBUG] Found {len(result)} head instances")
                    return result
                else:
                    debug(f"  [DEBUG] No instances found for {original_relation}[{head_constant}]")
            else:
                # relation(constant) 表示 relation(X, constant)
                # 要获取X的值，查找 INVERSE_relation[constant]
                inverse_relation = self.kg.get_inverse_relation(head_relation)
                debug(f"  [DEBUG] Looking for {inverse_relation}[{head_constant}]")
                if inverse_relation in self.kg.r2h2t and head_constant in self.kg.r2h2t[inverse_relation]:
                    result = set(self.kg.r2h2t[inverse_relation][head_constant])
                    debug(f"  [DEBUG] Found {len(result)} head instances")
                    return result
                else:
                    debug(f"  [DEBUG] No instances found for {inverse_relation}[{head_constant}]")
            
            return set()
        else:
            # 二元规则：返回(X,Y)对的集合
            return self.kg.get_relation_pairs(head_relation)
    
    def _get_body_instances(self, rule_info: Dict) -> Set:
        """获取身体实例集合 - 使用统一的简写格式处理"""
        variable_count = rule_info.get('variable_count', 0)
        
        if variable_count == 1:
            # 一元规则
            body_relations = rule_info.get('body_relations', [])
            body_constant = rule_info.get('body_constant')
            
            if not body_relations:
                debug(f"  [ERROR] No body relations found!")
                return set()
            
            # 连接所有body关系
            if len(body_relations) == 1:
                connected_relation = body_relations[0]
            else:
                connected_relation = body_relations[0]
                for i in range(1, len(body_relations)):
                    connected_relation = self.join_relations(connected_relation, body_relations[i])
            
            debug(f"  [DEBUG] Connected relation: {connected_relation}")
            debug(f"  [DEBUG] Body constant: {body_constant}")
            
            # 获取实例
            if body_constant is not None:
                # 有常量的情况：查询特定的实例
                # 对于简写格式，body_constant通常在tail位置
                # 所以需要使用逆关系来查询：INVERSE_connected_relation[body_constant]
                inverse_connected_relation = self.kg.get_inverse_relation(connected_relation)
                debug(f"  [DEBUG] Using inverse relation: {inverse_connected_relation}")
                
                if inverse_connected_relation in self.kg.r2h2t and body_constant in self.kg.r2h2t[inverse_connected_relation]:
                    result = set(self.kg.r2h2t[inverse_connected_relation][body_constant])
                    debug(f"  [DEBUG] Body instances from {inverse_connected_relation}[{body_constant}]: {len(result)}")
                    return result
                else:
                    debug(f"  [DEBUG] No instances found for {inverse_connected_relation}[{body_constant}]")
                    return set()
            else:
                # 没有常量的情况：获取整个关系的所有head实体
                if connected_relation in self.kg.r2h2t:
                    result = set(self.kg.r2h2t[connected_relation].keys())
                    debug(f"  [DEBUG] Body instances from {connected_relation} (all heads): {len(result)}")
                    return result
                else:
                    debug(f"  [DEBUG] No instances found for relation {connected_relation}")
                    return set()
        else:
            # 二元规则
            body_relations = rule_info.get('body_relations', [])
            return self.get_binary_instances_join(body_relations)
    
    def _extract_unary_body_info(self, rule_info: Dict) -> Tuple[str, int]:
        """
        从完整格式的一元规则中提取body的常量和变量位置信息
        
        对于规则如：head(X,/m/05pd94v) <= body1(X,A), body2(A,/m/0m2l9)
        需要从最后一个包含常量的原子中提取常量 /m/0m2l9
        
        Returns:
            (body_constant, body_variable_position)
        """
        body_atoms = rule_info.get('body_atoms', [])
        
        if not body_atoms:
            return None, 0
        
        debug(f"  [DEBUG] Extracting from body_atoms: {[(atom.get('relation'), atom.get('args')) for atom in body_atoms]}")
        
        # 查找包含常量的原子
        for atom in body_atoms:
            if isinstance(atom, dict):
                args = atom.get('args', [])
            else:
                # 如果body_atoms是字符串列表，跳过
                continue
            
            # 查找常量（长度>1的参数）
            for i, arg in enumerate(args):
                if len(arg) > 1:  # 找到常量
                    # 确定变量位置：常量的对面就是变量位置
                    variable_position = 1 - i  # 如果常量在位置i，变量在位置1-i
                    debug(f"  [DEBUG] Found constant {arg} at position {i}, variable at position {variable_position}")
                    return arg, variable_position
        
        # 如果没有找到常量，返回默认值
        debug(f"  [DEBUG] No constant found in body atoms")
        return None, 0

    def calculate_rule_support_bruteforce(self, rule_info: Dict) -> Dict:
        """
        使用暴力算法计算规则支持度
        
        Args:
            rule_info: 规则信息字典
        
        Returns:
            包含 headSize, bodySize, support, confidence 的字典
        """
        if rule_info['variable_count'] == 1:
            return self._calculate_unary_rule_bruteforce(rule_info)
        else:
            return self._calculate_binary_rule_bruteforce(rule_info)
    
    def _calculate_unary_rule_bruteforce(self, rule_info: Dict) -> Dict:
        """计算一元规则支持度 - 暴力算法"""
        head_atom = rule_info['head_atom']
        
        # 计算head实例集合
        head_instances = self._get_atom_instances_bruteforce(head_atom, rule_info['free_variables'])
        head_size = len(head_instances)
        
        # 计算body实例集合
        body_instances = self.get_unary_instances_bruteforce(rule_info)
        body_size = len(body_instances)
        
        # 计算支持度
        support_instances = head_instances.intersection(body_instances)
        support = len(support_instances)
        confidence = support / body_size if body_size > 0 else 0
        
        return {
            'headSize': head_size,
            'bodySize': body_size,
            'support': support,
            'confidence': confidence
        }
    
    def _calculate_binary_rule_bruteforce(self, rule_info: Dict) -> Dict:
        """计算二元规则支持度 - 暴力算法"""
        head_atom = rule_info['head_atom']
        
        # 计算head实例集合
        head_instances = self._get_atom_instances_bruteforce(head_atom, rule_info['free_variables'])
        head_size = len(head_instances)
        
        # 计算body实例集合
        body_instances = self.get_binary_instances_bruteforce(rule_info)
        body_size = len(body_instances)
        
        # 计算支持度
        support_instances = head_instances.intersection(body_instances)
        support = len(support_instances)
        confidence = support / body_size if body_size > 0 else 0
        
        return {
            'headSize': head_size,
            'bodySize': body_size,
            'support': support,
            'confidence': confidence
        }
    
    def _get_atom_instances_bruteforce(self, atom: Dict, free_variables: List[str]) -> Set:
        """获取原子的实例集合"""
        relation = atom['relation']
        args = atom['args']
        
        if len(free_variables) == 1:
            # 一元规则：返回变量值集合
            variable = free_variables[0]
            instances = set()
            
            for head, tail in self.kg.get_relation_pairs(relation):
                if args[0] == variable and args[1] != variable:
                    if args[1] == tail:
                        instances.add(head)
                elif args[1] == variable and args[0] != variable:
                    if args[0] == head:
                        instances.add(tail)
            
            return instances
        else:
            # 二元规则：返回 (X, Y) 对集合
            return self.kg.get_relation_pairs(relation)

    
    def find_path_instances_count(self, relation_path: List[str]) -> int:
        """
        使用逐级连接算法计算路径实例数量
        
        算法：
        1. 从右到左逐级连接关系
        2. 每次连接两个关系，产生新的中间关系并存储到r2h2t
        3. 最终返回完整路径的实例数量
        """
        if not relation_path:
            return 0
        
        if len(relation_path) == 1:
            return self.kg.get_relation_instances_count(relation_path[0])
        
        debug(f"计算路径实例: {' · '.join(relation_path)}")
        
        # 从右到左逐级连接
        current_relation = relation_path[-1]  # 最右边的关系
        
        # 从倒数第二个关系开始，向左连接
        for i in range(len(relation_path) - 2, -1, -1):
            left_relation = relation_path[i]
            current_relation = self.join_relations(left_relation, current_relation)
        
        # 返回最终关系的实例数量
        return self.kg.get_relation_instances_count(current_relation)
    
    def _calculate_unary_support(self, rule_info: Dict, head_instances: Set[Tuple[str, str]], 
                               body_instances: Set[Tuple[str, str]]) -> Set[Tuple[str, str]]:
        """计算一元规则的支持度实例"""
        # 对于一元规则，需要根据固定实体位置来匹配
        if not rule_info['is_unary']:
            return head_instances.intersection(body_instances)
        
        fixed_entity = rule_info.get('fixed_entity')
        var_position = rule_info.get('variable_position')
        
        if not fixed_entity or not var_position:
            # 如果没有固定实体信息，回退到简单交集
            return head_instances.intersection(body_instances)
        
        # 根据变量位置进行匹配
        support_instances = set()
        
        if var_position == 'tail':
            # 形如 head(X, fixed_entity)，自由变量在tail位置
            for head, tail in head_instances:
                if tail == fixed_entity:
                    # 检查身体是否包含相应的实例
                    for b_head, b_tail in body_instances:
                        if head == b_head:  # X值匹配
                            support_instances.add((head, tail))
                            break
        else:
            # 形如 head(fixed_entity, X)，自由变量在head位置
            for head, tail in head_instances:
                if head == fixed_entity:
                    for b_head, b_tail in body_instances:
                        if tail == b_tail:  # X值匹配
                            support_instances.add((head, tail))
                            break
        
        return support_instances
    
    def _find_unary_body_instances_simplified(self, rule_info: Dict, body_relations: List[str]) -> Set[Tuple[str, str]]:
        """
        处理简写格式的一元规则身体实例
        
        例如：/rel(/m/123) <= /rel1·/rel2(/m/456)
        表示：/rel(X,/m/123) <= /rel1(X,A), /rel2(A,/m/456)
        """
        if not body_relations:
            return set()
        
        # 对于简写的一元规则，需要构建复合关系并过滤固定实体
        if len(body_relations) == 1:
            # 单个关系
            relation = body_relations[0]
            # 从关系名中提取可能的固定实体
            if '(' in relation and ')' in relation:
                # 如：/rel2(/m/456)
                base_relation = relation.split('(')[0]
                fixed_entity = relation.split('(')[1].split(')')[0]
                
                # 获取该关系的所有实例，过滤包含固定实体的
                all_instances = self.kg.get_relation_pairs(base_relation)
                filtered_instances = set()
                
                if rule_info['variable_position'] == 'tail':
                    # X在tail位置，固定实体应该在head位置
                    for head, tail in all_instances:
                        if tail == fixed_entity:
                            filtered_instances.add((head, tail))
                else:
                    # X在head位置，固定实体应该在tail位置
                    for head, tail in all_instances:
                        if head == fixed_entity:
                            filtered_instances.add((head, tail))
                
                return filtered_instances
            else:
                # 没有固定实体，返回所有实例
                return self.kg.get_relation_pairs(relation)
        else:
            # 多个关系的连接：构建复合关系
            composite_relation = body_relations[0]
            for rel in body_relations[1:]:
                composite_relation = self.join_relations(composite_relation, rel)
            
            return self.kg.get_relation_pairs(composite_relation)
    
    def _find_binary_body_instances_simple(self, body_relations: List[str]) -> Set[Tuple[str, str]]:
        """
        处理二元规则的身体实例（简化版本）
        """
        if not body_relations:
            return set()
        
        if len(body_relations) == 1:
            return self.kg.get_relation_pairs(body_relations[0])
        
        # 多个关系的连接
        current_pairs = self.kg.get_relation_pairs(body_relations[0])
        
        for relation in body_relations[1:]:
            next_pairs = self.kg.get_relation_pairs(relation)
            new_pairs = set()
            
            # 简化的连接：找到能连接的对
            for (h1, t1) in current_pairs:
                for (h2, t2) in next_pairs:
                    if t1 == h2:  # 可以连接
                        new_pairs.add((h1, t2))
            
            current_pairs = new_pairs
        
        return current_pairs
    
    def _find_body_instances_by_variables(self, rule_info: Dict) -> Set[Tuple[str, str]]:
        """
        根据变量绑定信息精确计算身体实例
        
        一元规则示例：
        head(X,/m/02cg41) <= body1(X,/m/05pd94v)
        head(X,/m/05pd94v) <= body1(X,A), body2(A,/m/0m2l9)
        
        二元规则示例：
        head(X,Y) <= body1(X,A), body2(Y,A)
        head(X,Y) <= body1(X,A), body2(A,B), body3(Y,B)
        """
        body_atoms = rule_info['body_atoms']
        free_vars = rule_info['free_variables']
        
        if not body_atoms:
            return set()
        
        # 为每个身体原子获取实例
        atom_instances = {}
        for i, atom in enumerate(body_atoms):
            relation = RuleParser._extract_relation_from_atom(atom)
            variables = RuleParser._extract_variables(atom)
            atom_instances[i] = {
                'relation': relation,
                'variables': variables,
                'instances': self.kg.get_relation_pairs(relation)
            }
        
        debug(f"  身体原子数量: {len(atom_instances)}")
        
        # 生成所有可能的变量绑定组合
        result_instances = set()
        
        if len(free_vars) == 1:
            # 一元规则：只有一个自由变量
            free_var = list(free_vars)[0]
            free_var_values = self._solve_unary_rule(atom_instances, free_var)
            
            # 对于一元规则，需要根据头部的模式构造实例
            # 从规则信息中获取头部的常量
            head_variables = rule_info.get('head_variables', [])
            head_constants = []
            
            for var in head_variables:
                if var.startswith('/m/'):  # 这是一个常量实体
                    head_constants.append(var)
                elif var != free_var:  # 这也可能是常量
                    head_constants.append(var)
            
            # 构造头部实例
            if len(head_variables) == 2:
                # 双参数头部，如 head(X, constant) 或 head(constant, X)
                for value in free_var_values:
                    if head_variables[0] == free_var:
                        # 自由变量在第一位
                        if len(head_constants) > 0:
                            result_instances.add((value, head_constants[0]))
                    elif head_variables[1] == free_var:
                        # 自由变量在第二位
                        if len(head_constants) > 0:
                            result_instances.add((head_constants[0], value))
            
        else:
            # 二元规则：有两个自由变量
            result_instances = self._solve_binary_rule(atom_instances, free_vars)
        
        return result_instances
    
    def _solve_unary_rule(self, atom_instances: Dict, free_var: str) -> Set[str]:
        """
        求解一元规则
        
        例如：head(X,/m/02cg41) <= body1(X,/m/05pd94v)
        需要找到所有满足body1的X值
        
        返回：满足条件的自由变量值的集合
        """
        # 这里需要根据具体的原子模式来实现
        # 对于一元规则，我们需要找到所有可能的自由变量值
        result_values = set()
        
        if not atom_instances:
            return result_values
        
        # 从第一个原子开始，收集自由变量的可能值
        first_atom = atom_instances[0]
        first_instances = first_atom['instances']
        first_vars = first_atom['variables']
        
        # 找到自由变量在第一个原子中的位置
        free_var_position = None
        for i, var in enumerate(first_vars):
            if var == free_var:
                free_var_position = i
                break
        
        if free_var_position is not None:
            # 收集自由变量的所有可能值
            for head, tail in first_instances:
                if free_var_position == 0:
                    result_values.add(head)
                elif free_var_position == 1:
                    result_values.add(tail)
        
        # 如果有多个原子，需要进行交集操作
        for i in range(1, len(atom_instances)):
            atom = atom_instances[i]
            atom_instances_set = atom['instances']
            atom_vars = atom['variables']
            
            # 找到自由变量在当前原子中的位置
            free_var_position = None
            for j, var in enumerate(atom_vars):
                if var == free_var:
                    free_var_position = j
                    break
            
            if free_var_position is not None:
                # 收集当前原子中自由变量的可能值
                current_values = set()
                for head, tail in atom_instances_set:
                    if free_var_position == 0:
                        current_values.add(head)
                    elif free_var_position == 1:
                        current_values.add(tail)
                
                # 与之前的结果求交集
                result_values = result_values.intersection(current_values)
        
        return result_values
    
    def _solve_binary_rule(self, atom_instances: Dict, free_vars: Set[str]) -> Set[Tuple[str, str]]:
        """
        求解二元规则
        
        例如：head(X,Y) <= body1(X,A), body2(Y,A)
        需要找到所有满足连接条件的(X,Y)对
        """
        if len(atom_instances) == 1:
            # 只有一个身体原子
            atom = atom_instances[0]
            return atom['instances']
        
        # 多个身体原子：需要通过约束变量连接
        result = set()
        
        # 简化实现：假设是两个原子的连接
        if len(atom_instances) == 2:
            atom1 = atom_instances[0]
            atom2 = atom_instances[1]
            
            instances1 = atom1['instances']
            instances2 = atom2['instances']
            vars1 = atom1['variables']
            vars2 = atom2['variables']
            
            # 找到连接变量（共同的约束变量）
            common_vars = set(vars1) & set(vars2) - free_vars
            
            if common_vars:
                # 有连接变量：通过连接变量匹配
                for h1, t1 in instances1:
                    for h2, t2 in instances2:
                        # 简化的连接逻辑
                        if self._can_join(vars1, vars2, h1, t1, h2, t2, common_vars):
                            # 构造结果对（需要根据自由变量的位置确定）
                            x_val, y_val = self._extract_free_variable_values(
                                vars1, vars2, h1, t1, h2, t2, free_vars
                            )
                            if x_val and y_val:
                                result.add((x_val, y_val))
        
        return result
    
    def _can_join(self, vars1: List[str], vars2: List[str], h1: str, t1: str, 
                  h2: str, t2: str, common_vars: Set[str]) -> bool:
        """检查两个原子实例是否可以通过约束变量连接"""
        # 简化实现：检查尾部是否匹配
        return t1 == h2 or t1 == t2 or h1 == h2 or h1 == t2
    
    def _extract_free_variable_values(self, vars1: List[str], vars2: List[str], 
                                    h1: str, t1: str, h2: str, t2: str, 
                                    free_vars: Set[str]) -> Tuple[str, str]:
        """从连接的原子实例中提取自由变量的值"""
        # 简化实现：假设X在第一个原子的第一位，Y在第二个原子的第一位或第二位
        x_val = h1 if len(vars1) > 0 and vars1[0] in free_vars else None
        y_val = h2 if len(vars2) > 0 and vars2[0] in free_vars else (
            t2 if len(vars2) > 1 and vars2[1] in free_vars else None
        )
        return x_val, y_val
    
    def _find_binary_body_instances_simple(self, body_relations: List[str]) -> Set[Tuple[str, str]]:
        """
        简化的二元规则身体实例计算（用于简写格式）
        假设是链式连接模式
        """
        if not body_relations:
            return set()
        
        if len(body_relations) == 1:
            return self.kg.get_relation_pairs(body_relations[0])
        
        # 对于多个关系，进行链式连接
        current_pairs = self.kg.get_relation_pairs(body_relations[0])
        
        for relation in body_relations[1:]:
            next_pairs = self.kg.get_relation_pairs(relation)
            new_pairs = set()
            
            # 简化的连接：找到能连接的对
            for (h1, t1) in current_pairs:
                for (h2, t2) in next_pairs:
                    if t1 == h2:  # 可以连接
                        new_pairs.add((h1, t2))
            
            current_pairs = new_pairs
        
        return current_pairs
    
    def get_path_instances(self, relation_path: List[str]) -> Set[Tuple[str, str]]:
        """获取路径的实际实例集合（用于计算交集）"""
        if not relation_path:
            return set()
        
        if len(relation_path) == 1:
            return self.kg.get_relation_pairs(relation_path[0])
        
        # 构建路径关系名称，与find_path_instances_count中的逻辑一致
        current_relation = relation_path[-1]  # 最右边的关系
        
        # 从倒数第二个关系开始，向左连接（复用已存储的中间结果）
        for i in range(len(relation_path) - 2, -1, -1):
            left_relation = relation_path[i]
            current_relation = f"{left_relation}·{current_relation}"
        
        # 如果该关系已经在r2h2t中，直接获取实例
        if current_relation in self.kg.r2h2t:
            return self.kg.get_relation_pairs(current_relation)
        else:
            # 如果不存在，先计算它（这不应该发生，因为find_path_instances_count应该已经计算过了）
            self.find_path_instances_count(relation_path)
            return self.kg.get_relation_pairs(current_relation)


def load_dataset(filepath: str) -> KnowledgeGraph:
    """加载数据集到知识图谱"""
    kg = KnowledgeGraph()
    
    debug(f"正在加载数据集: {filepath}")
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"数据集文件不存在: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) != 3:
                debug(f"警告：第{line_num}行格式错误: {line}")
                continue
            
            head, relation, tail = parts
            kg.add_triple(head, relation, tail)
            
            if line_num % 50000 == 0:
                debug(f"已加载 {line_num:,} 个三元组...")
    
    debug(f"数据集加载完成:")
    debug(f"  三元组数量: {len(kg.triples):,}")
    debug(f"  实体数量: {len(kg.entities):,}")
    debug(f"  原始关系数量: {len([r for r in kg.relations if not r.startswith('INVERSE_')]):,}")
    debug(f"  总关系数量（含逆关系）: {len(kg.relations):,}")
    
    return kg


def analyze_rule_from_string(rule_str: str, kg: KnowledgeGraph) -> Dict:
    """
    从规则字符串分析规则支持度
    
    Args:
        rule_str: 规则字符串
        kg: 知识图谱
        
    Returns:
        包含两种算法结果的字典
    """
    try:
        # 解析规则
        head_relation, body_relations, variable_count, rule_info = RuleParser.parse_rule(rule_str)
        
        debug(f"\n分析规则: {rule_str}")
        debug(f"规范化规则: {rule_info.get('normalized_rule', rule_str)}")
        debug(f"规则类型: {'一元' if variable_count == 1 else '二元'}")
        debug(f"头部关系: {head_relation}")
        debug(f"身体关系: {body_relations}")
        
        # 创建计算器
        calculator = RuleSupportCalculator(kg)
        
        # 连接算法和暴力算法
        join_result = None
        bruteforce_result = None
        
        try:
            # 调用连接算法
            debug("=== 连接算法 ===")
            join_result = calculator.calculate_rule_support_join(rule_info)
            
            # 调用暴力算法
            # debug("=== 暴力算法 ===")
            # bruteforce_result = calculator.calculate_rule_support_bruteforce(rule_info)
            
        except Exception as e:
            debug(f"计算失败: {e}")
            import traceback
            traceback.print_exc()
            
            # 如果计算失败，至少返回基本的头部信息
            head_size = kg.get_relation_instances_count(head_relation)
            join_result = {'headSize': head_size, 'bodySize': 0, 'support': 0, 'confidence': 0.0}
            bruteforce_result = {'headSize': head_size, 'bodySize': 0, 'support': 0, 'confidence': 0.0}
        
        return {
            'rule': rule_str,
            'head_relation': head_relation,
            'body_relations': body_relations,
            'variable_count': variable_count,
            'join_result': join_result,
            'bruteforce_result': bruteforce_result
        }
        
    except Exception as e:
        debug(f"规则分析失败: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    # 数据集路径（相对于当前脚本的路径）
    dataset_path = "data/FB15k-237/train.txt"
    
    # 要分析的规则示例
    test_rules = [
        "/award/award_category/winners./award/award_honor/ceremony(X,/m/01xqqp) <= /award/award_category/winners./award/award_honor/ceremony(X,A), /award/award_category/winners./award/award_honor/ceremony(/m/0257w4,A)",
        # 一元规则示例（只有一个自由变量）
        "/award/award_category/winners./award/award_honor/ceremony(X,/m/02cg41) <= /award/award_category/winners./award/award_honor/ceremony(X,/m/05pd94v)",
        
        "/award/award_category/winners./award/award_honor/ceremony(X,/m/05pd94v) <= /award/award_category/winners./award/award_honor/ceremony(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(A,/m/0m2l9)",
        
        # 一元规则简写格式
        "/award/award_category/winners./award/award_honor/ceremony(/m/05pd94v) <= /award/award_category/winners./award/award_honor/ceremony·/award/award_ceremony/awards_presented./award/award_honor/award_winner(/m/0m2l9)",
        
        "INVERSE_/award/award_category/winners./award/award_honor/ceremony(/m/0gs9p) <= INVERSE_/award/award_category/winners./award/award_honor/ceremony·/award/award_category/nominees./award/award_nomination/nominated_for(/m/0j8f09z)",
        
        # 二元规则示例（有两个自由变量X,Y）
        "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)",
        
        "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/ceremony(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(A,B), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,B)",
        
        # 简写格式的二元规则（传统格式）
        "/award/award_category/winners./award/award_honor/ceremony <= /award/award_category/winners./award/award_honor/ceremony·/award/award_ceremony/awards_presented./award/award_honor/award_winner·INVERSE_/award/award_ceremony/awards_presented./award/award_honor/award_winner",

        "/award/award_category/winners./award/award_honor/ceremony(/m/0gs96,X) <= /award/award_category/winners./award/award_honor/ceremony(A,X), /award/award_category/nominees./award/award_nomination/nominated_for(A,/m/02r79_h)",
        "/award/award_category/winners./award/award_honor/ceremony(/m/0f4x7,X) <= /award/award_category/winners./award/award_honor/ceremony(A,X), /award/award_nominee/award_nominations./award/award_nomination/award(/m/02_fj,A)",
        "/award/award_category/winners./award/award_honor/ceremony(/m/01ck6v,Y) <= /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)",
        "/award/award_category/winners./award/award_honor/ceremony(X,/m/07z31v) <= /award/award_nominee/award_nominations./award/award_nomination/award(A,X)"
    ]

    test_rules = [
        "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/category_of(X,A), /time/event/instance_of_recurring_event(Y,A)",
        "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_winner/awards_won./award/award_honor/award_winner(B,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,B)",

        "/film/film/release_date_s./film/film_regional_release_date/film_release_region(X,/m/0b90_r) <= /film/film/release_date_s./film/film_regional_release_date/film_release_region(X,/m/07ylj)"
    ]

    try:
        # 加载数据集
        kg = load_dataset(dataset_path)
        
        # 分析每个规则
        results = []
        for rule_str in test_rules:
            result = analyze_rule_from_string(rule_str, kg)
            if result:
                results.append(result)
        
        # 输出综合结果
        debug(f"\n{'='*100}")
        debug("综合分析结果")
        debug(f"{'='*100}")
        
        for i, result in enumerate(results, 1):
            debug(f"\n规则 {i}: {result['rule']}")
            
            if result['join_result']:
                debug(f"  连接算法: {result['join_result']}")
            
            if result['bruteforce_result']:
                debug(f"  暴力算法: {result['bruteforce_result']}")
        
        debug(f"\n{'='*100}")
        
    except Exception as e:
        debug(f"错误: {e}")
        import traceback
        traceback.print_exc()


