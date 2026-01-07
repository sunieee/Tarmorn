"""
规则加载器模块
从规则文件中读取规则并构造H2B2Metric和HB2Combo数据结构

规则文件实际格式（完整格式）:
1. 普通二元规则:
   body_size  support  confidence  rel(X,Y) <= rel1(X,A)*rel2(A,Y)
   
2. 普通一元规则:
   body_size  support  confidence  rel(X,/m/xxx) <= rel1(X,A)*rel2(A,/m/yyy)
   
3. 组合规则 (multi-branch):
   body_size  support  confidence  rel(X,Y) <= branch1(X,A); branch2(Y,A)

转换后的简写格式：
1. 二元规则: rel <= rel1*rel2
2. 一元规则: rel(/m/xxx) <= rel1*rel2(/m/yyy)
3. 组合规则: rel <= branch1; branch2
   
格式说明：
- body_size: 规则前提出现的次数
- support: 同时满足前提和结论的次数  
- confidence: 置信度（文件中的值，但实际使用时会根据 support / (body_size + NUM_UNSEEN) 计算）
"""

import re
import sys
import os
from typing import List, Tuple, Dict, Optional, Set
from collections import defaultdict

# 添加script目录到路径以复用RuleParser
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'script'))

from structures import (
    IdManager, RelationPath, Atom, Metric,
    Rule, Combo, H2B2Rule, RuleHash2Combo
)

# 导入RuleParser用于格式转换（使用绝对导入，因为已添加到sys.path）
try:
    from analysis_rule import RuleParser
except ImportError as e:
    print(f"Warning: Could not import RuleParser from analysis_rule: {e}")
    print("Rules will be treated as already in simplified format.")
    # 定义一个简单的fallback
    class RuleParser:
        @staticmethod
        def _normalize_to_simplified(head_part: str, body_part: str) -> str:
            """简单的fallback，假设已经是简写格式"""
            return f"{head_part} <= {body_part}"

class RuleLoader:
    """
    规则加载器
    从规则文件中读取规则，转换为Atom格式并构建索引
    """
    
    def __init__(self, id_manager: IdManager):
        self.id_manager = id_manager
        self.h2b2rule: H2B2Rule = {}  # head -> body -> rule
        self.rule_hash2combo: RuleHash2Combo = {}  # rule.hash -> combo
        self.stats = {
            'total_lines': 0,
            'normal_rules': 0,
            'combo_rules': 0,
            'parse_errors': 0
        }
    
    def load_rules(self, rule_file: str, verbose: bool = False):
        """
        加载规则文件
        
        Args:
            rule_file: 规则文件路径
            verbose: 是否打印详细信息
        """
        print(f"  Loading rules from: {rule_file}")
        with open(rule_file, 'r', encoding='utf-8') as f:
            for line in f:
                self.stats['total_lines'] += 1
                
                # 每1000行打印一次进度
                if self.stats['total_lines'] % 100000 == 0:
                    print(f"    Processed {self.stats['total_lines']} lines, {self.stats['normal_rules']} normal rules, {self.stats['combo_rules']} combo rules...")
                
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                try:
                    self._parse_and_add_rule(line, verbose)
                except Exception as e:
                    self.stats['parse_errors'] += 1
                    if verbose:
                        print(f"[WARN] Failed to parse rule: {line}")
                        print(f"       Error: {e}")
        
        print(f"  Finished loading rules.")
        if verbose:
            self._print_stats()
    
    def _parse_and_add_rule(self, line: str, verbose: bool = False):
        """解析单行规则并添加到索引"""
        # 解析度量和规则部分
        # 格式: body_size support confidence head <= body
        parts = line.split(None, 3)  # 按空白分割，最多4部分
        if len(parts) < 4:
            raise ValueError(f"Invalid rule format: {line}")
        
        try:
            body_size = int(parts[0])
            support = int(parts[1])
            confidence = float(parts[2])
            rule_str = parts[3]
        except (ValueError, IndexError):
            raise ValueError(f"Invalid metrics in rule: {line}")
        
        if '<=' not in rule_str:
            raise ValueError(f"Missing '<=' in rule: {rule_str}")
        
        head_part, body_part = rule_str.split('<=', 1)
        head_part = head_part.strip()
        body_part = body_part.strip()
        
        # 检查是否是组合规则（body包含分号）
        # 组合规则需要特殊处理，因为RuleParser._normalize_to_simplified不支持分号
        is_combo = ';' in body_part
        
        # 转换为简写格式
        # 规则文件中是完整格式：rel(X,Y) <= body(X,A)*body2(A,Y)
        # 需要转换为简写格式：rel <= body*body2
        try:
            if verbose and self.stats['total_lines'] <= 3:
                print(f"    Converting rule {self.stats['total_lines']}: {head_part} <= {body_part}")
                print(f"    Is combo: {is_combo}")
            
            if is_combo:
                # 组合规则：分别转换每个分支
                branches = [b.strip() for b in body_part.split(';')]
                simplified_branches = []
                simplified_head = None
                for branch in branches:
                    branch_simplified = RuleParser._normalize_to_simplified(head_part, branch)
                    simplified_head, branch_body = branch_simplified.split('<=', 1)
                    simplified_branches.append(branch_body.strip())
                # 头部只需要转换一次（使用第一个分支）
                simplified_head = simplified_head.strip()
                simplified_body = '; '.join(simplified_branches)
            else:
                # 普通规则：直接转换
                simplified_rule = RuleParser._normalize_to_simplified(head_part, body_part)
                simplified_head, simplified_body = simplified_rule.split('<=', 1)
                simplified_head = simplified_head.strip()
                simplified_body = simplified_body.strip()
            
            if verbose and self.stats['total_lines'] <= 3:
                print(f"    Simplified to: {simplified_head} <= {simplified_body}")
        except Exception as e:
            if verbose or self.stats['parse_errors'] < 10:
                print(f"[WARN] Failed to convert to simplified format: {head_part} <= {body_part}")
                print(f"       Error: {e}, skipping this rule")
            # 跳过这个规则
            self.stats['parse_errors'] += 1
            return
        
        # 检查是否是组合规则（body包含分号）
        if ';' in simplified_body:
            self._add_combo_rule(simplified_head, simplified_body, body_size, support, confidence, verbose)
        else:
            self._add_normal_rule(simplified_head, simplified_body, body_size, support, confidence, verbose)
    
    def _add_normal_rule(self, head_str: str, body_str: str, 
                         body_size: int, support: int, confidence: float, verbose: bool = False):
        """添加普通规则"""
        head_atom = self._parse_atom(head_str)
        body_atom = self._parse_atom(body_str)
        
        metric = Metric(body_size=body_size, support=support)
        rule = Rule(head=head_atom, body=body_atom, metric=metric)
        
        if head_atom not in self.h2b2rule:
            self.h2b2rule[head_atom] = {}
        self.h2b2rule[head_atom][body_atom] = rule
        self.stats['normal_rules'] += 1
        
        if verbose:
            print(f"  Added: {rule.to_string(self.id_manager)}")
    
    def _add_combo_rule(self, head_str: str, body_str: str,
                        body_size: int, support: int, confidence: float, verbose: bool = False):
        """添加组合规则"""
        head_atom = self._parse_atom(head_str)
        
        # 解析所有分支
        branch_strs = [b.strip() for b in body_str.split(';')]
        branch_atoms = tuple(sorted(
            [self._parse_atom(b) for b in branch_strs],
            key=lambda a: (a.relation_id, a.entity_id)
        ))
        
        metric = Metric(body_size=body_size, support=support)
        combo = Combo(head=head_atom, branches=branch_atoms, metric=metric, h2b2rule=self.h2b2rule)
        
        # 找到对应的Rule并用其hash作为key
        for branch in branch_atoms:
            base_rule = Rule(head=head_atom, body=branch, metric=Metric())
            self.rule_hash2combo[hash(base_rule)] = combo
        
        self.stats['combo_rules'] += 1
        
        if verbose:
            print(f"  Added combo: {combo.to_string(self.id_manager)}")
    
    def _parse_atom(self, atom_str: str) -> Atom:
        """
        解析原子字符串为Atom对象
        
        支持的格式:
        - 二元: "rel" 或 "rel1*rel2" -> (relation_path_id, Y_id)
        - 一元: "rel(/m/xxx)" -> (relation_id, entity_id)
        - 存在性: "rel(*)" -> (relation_id, 0)
        - 自环: "rel(X)" -> (relation_id, X_id)
        """
        atom_str = atom_str.strip()
        
        # 检查是否有括号（一元规则）
        if '(' in atom_str and ')' in atom_str:
            paren_start = atom_str.rfind('(')
            paren_end = atom_str.rfind(')')
            
            relation_path_str = atom_str[:paren_start].strip()
            entity_str = atom_str[paren_start+1:paren_end].strip()
            
            # 解析关系路径
            relation_id = self._parse_relation_path(relation_path_str)
            
            # 解析实体
            if entity_str == '*':
                entity_id = 0  # 存在性
            elif entity_str == 'X':
                entity_id = self.id_manager.get_x_id()  # 自环
            elif entity_str == 'Y':
                entity_id = self.id_manager.get_y_id()  # 不应该出现，但保险起见
            else:
                entity_id = self.id_manager.get_entity_id(entity_str)
            
            return Atom(relation_id=relation_id, entity_id=entity_id)
        else:
            # 二元规则：没有括号
            relation_id = self._parse_relation_path(atom_str)
            entity_id = self.id_manager.get_y_id()  # Y表示二元
            return Atom(relation_id=relation_id, entity_id=entity_id)
    
    def _parse_relation_path(self, path_str: str) -> int:
        """
        解析关系路径字符串为编码的ID
        
        Args:
            path_str: 关系路径字符串，如 "rel1*rel2*rel3"
            
        Returns:
            编码的关系路径ID
        """
        if not path_str:
            raise ValueError("Empty relation path")
        
        # 按*分割关系
        if '*' in path_str:
            relation_strs = path_str.split('*')
        else:
            relation_strs = [path_str]
        
        # 获取每个关系的ID
        relation_ids = []
        for rel_str in relation_strs:
            rel_str = rel_str.strip()
            if not rel_str:
                continue
            relation_ids.append(self.id_manager.get_relation_id(rel_str))
        
        if len(relation_ids) == 0:
            raise ValueError(f"No valid relations in path: {path_str}")
        
        if len(relation_ids) == 1:
            return relation_ids[0]
        
        # 编码为关系路径
        return RelationPath.encode(relation_ids)
    
    def _print_stats(self):
        """打印统计信息"""
        print("\n=== Rule Loading Statistics ===")
        print(f"Total lines: {self.stats['total_lines']}")
        print(f"Normal rules: {self.stats['normal_rules']}")
        print(f"Combo rules: {self.stats['combo_rules']}")
        print(f"Parse errors: {self.stats['parse_errors']}")
        print(f"H2B2Rule heads: {len(self.h2b2rule)}")
        print(f"RuleHash2Combo size: {len(self.rule_hash2combo)}")
        print("================================\n")


class KnowledgeGraphLoader:
    """
    知识图谱加载器
    从三元组文件加载数据并建立索引
    """
    
    def __init__(self, id_manager: IdManager):
        self.id_manager = id_manager
        # r2h2t索引: relation_id -> {head_id: set of tail_ids}
        # 不使用lambda以支持pickle（多进程需要）
        self.r2h2t: Dict[int, Dict[int, Set[int]]] = {}
        # 三元组集合
        self.triples: Set[Tuple[int, int, int]] = set()
        self.stats = {
            'total_triples': 0,
            'entities': 0,
            'relations': 0
        }
    
    def _get_or_create_h2t(self, rel_id: int) -> Dict[int, Set[int]]:
        """获取或创建head到tail的映射"""
        if rel_id not in self.r2h2t:
            self.r2h2t[rel_id] = {}
        return self.r2h2t[rel_id]
    
    def _add_tail(self, rel_id: int, head_id: int, tail_id: int):
        """添加一个tail到索引"""
        h2t = self._get_or_create_h2t(rel_id)
        if head_id not in h2t:
            h2t[head_id] = set()
        h2t[head_id].add(tail_id)
    
    def load_triples(self, triple_file: str, verbose: bool = False):
        """
        加载三元组文件
        
        Args:
            triple_file: 三元组文件路径 (格式: head TAB relation TAB tail)
            verbose: 是否打印详细信息
        """
        print(f"  Loading triples from: {triple_file}")
        with open(triple_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # 每10000行打印一次进度
                if self.stats['total_triples'] > 0 and self.stats['total_triples'] % 100000 == 0:
                    print(f"    Loaded {self.stats['total_triples']} triples...")
                
                parts = line.split('\t')
                if len(parts) != 3:
                    if verbose:
                        print(f"[WARN] Invalid triple format: {line}")
                    continue
                
                head_str, rel_str, tail_str = parts
                
                # 获取ID
                head_id = self.id_manager.get_entity_id(head_str)
                rel_id = self.id_manager.get_relation_id(rel_str)
                tail_id = self.id_manager.get_entity_id(tail_str)
                
                # 添加三元组
                self.triples.add((head_id, rel_id, tail_id))
                
                # 建立r2h2t索引
                self._add_tail(rel_id, head_id, tail_id)
                
                self.stats['total_triples'] += 1
        
        print(f"  Finished loading {self.stats['total_triples']} triples.")
        print(f"  Adding inverse relations...")
        # 添加逆关系
        self.id_manager.add_inverse_relations()
        self._add_inverse_index()
        print(f"  Inverse relations added.")
        
        self.stats['entities'] = len(self.id_manager.id2entity) - 27  # 减去A-Z和*
        self.stats['relations'] = self.id_manager.original_relation_count
        
        if verbose:
            self._print_stats()
    
    def _add_inverse_index(self):
        """为所有关系添加逆关系索引"""
        inverse_entries = []
        
        for rel_id, h2t in self.r2h2t.items():
            if rel_id <= self.id_manager.original_relation_count:
                inv_rel_id = self.id_manager.get_inverse_relation(rel_id)
                for head_id, tail_ids in h2t.items():
                    for tail_id in tail_ids:
                        inverse_entries.append((inv_rel_id, tail_id, head_id))
        
        for inv_rel_id, head_id, tail_id in inverse_entries:
            self._add_tail(inv_rel_id, head_id, tail_id)
    
    def has_triple(self, head_id: int, rel_id: int, tail_id: int) -> bool:
        """检查三元组是否存在"""
        return tail_id in self.r2h2t.get(rel_id, {}).get(head_id, set())
    
    def get_tails(self, head_id: int, rel_id: int) -> Set[int]:
        """获取给定头部和关系的所有尾部"""
        return self.r2h2t.get(rel_id, {}).get(head_id, set())
    
    def get_heads(self, rel_id: int, tail_id: int) -> Set[int]:
        """获取给定关系和尾部的所有头部"""
        inv_rel_id = self.id_manager.get_inverse_relation(rel_id)
        return self.r2h2t.get(inv_rel_id, {}).get(tail_id, set())
    
    def _print_stats(self):
        """打印统计信息"""
        print("\n=== Knowledge Graph Statistics ===")
        print(f"Total triples: {self.stats['total_triples']}")
        print(f"Entities: {self.stats['entities']}")
        print(f"Relations: {self.stats['relations']} (+ {self.stats['relations']} inverse)")
        print("==================================\n")


def load_data_and_rules(train_file: str, rule_file: str, 
                        verbose: bool = False) -> Tuple[IdManager, KnowledgeGraphLoader, RuleLoader]:
    """
    加载知识图谱和规则的便捷函数
    
    Args:
        train_file: 训练三元组文件
        rule_file: 规则文件
        verbose: 是否打印详细信息
        
    Returns:
        (id_manager, kg_loader, rule_loader)
    """
    print("Creating IdManager...")
    id_manager = IdManager()
    
    # 先加载知识图谱（建立entity和relation的ID映射）
    print("Loading knowledge graph...")
    kg_loader = KnowledgeGraphLoader(id_manager)
    kg_loader.load_triples(train_file, verbose)
    
    # 再加载规则（使用已建立的ID映射）
    print("Loading rules...")
    rule_loader = RuleLoader(id_manager)
    rule_loader.load_rules(rule_file, verbose)
    
    print("Data and rules loaded successfully!")
    return id_manager, kg_loader, rule_loader


if __name__ == '__main__':
    # 简单测试
    import argparse
    
    parser = argparse.ArgumentParser(description='Rule Loader Test')
    parser.add_argument('--train', type=str, required=True, help='Training triples file')
    parser.add_argument('--rules', type=str, required=True, help='Rules file')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    id_manager, kg_loader, rule_loader = load_data_and_rules(
        args.train, args.rules, args.verbose
    )
    
    print(f"\nLoaded {rule_loader.stats['normal_rules']} normal rules")
    print(f"Loaded {rule_loader.stats['combo_rules']} combo rules")
    
    # 打印前5条规则
    print("\nSample normal rules:")
    count = 0
    for head, b2rule in rule_loader.h2b2rule.items():
        for body, rule in b2rule.items():
            if count >= 5:
                break
            print(f"  {rule.to_string(id_manager)}")
            count += 1
        if count >= 5:
            break
