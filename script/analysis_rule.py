#!/usr/bin/env python3
"""
规则支持度计算器
计算给定规则的 headSize, bodySize 和 support

规则：
/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month <= 
/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month·INVERSE_/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month·/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month

这个规则等价于：month(X,Y) <= month(X,A), month(B,A), month(B,Y)
"""

import os
from collections import defaultdict, Counter
from typing import Set, Tuple, Dict, List


class KnowledgeGraph:
    """知识图谱类，用于存储和查询三元组"""
    
    def __init__(self):
        # 正向关系索引: (head, relation) -> set of tails
        self.forward_index = defaultdict(set)
        # 反向关系索引: (tail, relation) -> set of heads  
        self.backward_index = defaultdict(set)
        # 所有三元组
        self.triples = set()
        # 所有实体
        self.entities = set()
        # 所有关系
        self.relations = set()
    
    def add_triple(self, head: str, relation: str, tail: str):
        """添加三元组到知识图谱"""
        self.triples.add((head, relation, tail))
        self.entities.add(head)
        self.entities.add(tail)
        self.relations.add(relation)
        
        # 建立索引
        self.forward_index[(head, relation)].add(tail)
        self.backward_index[(tail, relation)].add(head)
    
    def get_forward_neighbors(self, entity: str, relation: str) -> Set[str]:
        """获取实体在给定关系下的正向邻居"""
        return self.forward_index.get((entity, relation), set())
    
    def get_backward_neighbors(self, entity: str, relation: str) -> Set[str]:
        """获取实体在给定关系下的反向邻居"""
        return self.backward_index.get((entity, relation), set())
    
    def get_relation_pairs(self, relation: str) -> Set[Tuple[str, str]]:
        """获取某个关系的所有(head, tail)对"""
        pairs = set()
        for (h, r), tails in self.forward_index.items():
            if r == relation:
                for t in tails:
                    pairs.add((h, t))
        return pairs


class RuleSupportCalculator:
    """规则支持度计算器"""
    
    def __init__(self, kg: KnowledgeGraph):
        self.kg = kg
    
    def parse_relation_path(self, path: str) -> List[Tuple[str, bool]]:
        """
        解析关系路径
        返回: [(relation, is_inverse), ...]
        """
        relations = []
        parts = path.split('·')
        
        for part in parts:
            part = part.strip()
            if part.startswith('INVERSE_'):
                relation = part[8:]  # 去掉 'INVERSE_' 前缀
                is_inverse = True
            else:
                relation = part
                is_inverse = False
            relations.append((relation, is_inverse))
        
        return relations
    
    def find_path_instances(self, relation_path: List[Tuple[str, bool]]) -> Set[Tuple[str, str]]:
        """
        找到满足关系路径的所有(start_entity, end_entity)对
        
        对于路径: relation1 · INVERSE_relation2 · relation3
        等价于: X --relation1--> A, B --relation2--> A, B --relation3--> Y
        最终返回所有(X, Y)对
        """
        if not relation_path:
            return set()
        
        # 获取第一个关系的所有实例
        first_relation, first_is_inverse = relation_path[0]
        
        if first_is_inverse:
            # 如果是逆关系，获取所有(tail, head)对，但返回为(head, tail)
            current_pairs = set()
            for (h, r), tails in self.kg.forward_index.items():
                if r == first_relation:
                    for t in tails:
                        current_pairs.add((t, h))  # 逆转
        else:
            current_pairs = self.kg.get_relation_pairs(first_relation)
        
        # 逐步扩展路径
        for relation, is_inverse in relation_path[1:]:
            new_pairs = set()
            
            for start, intermediate in current_pairs:
                if is_inverse:
                    # 逆关系：从 intermediate 出发，找到有关系指向intermediate的实体
                    next_entities = self.kg.get_backward_neighbors(intermediate, relation)
                else:
                    # 正向关系：从 intermediate 出发
                    next_entities = self.kg.get_forward_neighbors(intermediate, relation)
                
                for next_entity in next_entities:
                    new_pairs.add((start, next_entity))
            
            current_pairs = new_pairs
        
        return current_pairs
    
    def calculate_rule_support(self, head_relation: str, body_path: str) -> Dict[str, int]:
        """
        计算规则的支持度
        
        Args:
            head_relation: 头部关系
            body_path: 身体关系路径，用·连接
        
        Returns:
            包含 headSize, bodySize, support 的字典
        """
        print(f"计算规则: {head_relation} <= {body_path}")
        
        # 解析身体关系路径
        body_relations = self.parse_relation_path(body_path)
        print(f"解析的身体关系路径: {body_relations}")
        
        # 获取头部关系的所有实例
        head_instances = self.kg.get_relation_pairs(head_relation)
        head_size = len(head_instances)
        print(f"头部关系 {head_relation} 的实例数: {head_size}")
        
        # 输出头部关系的前几个实例
        if head_size > 0:
            print("头部关系实例样例（前5个）:")
            for i, (h, t) in enumerate(list(head_instances)[:5]):
                print(f"  {h} -> {t}")
        
        # 获取身体路径的所有实例
        body_instances = self.find_path_instances(body_relations)
        body_size = len(body_instances)
        print(f"身体路径的实例数: {body_size}")
        
        # 输出身体路径的前几个实例
        if body_size > 0:
            print("身体路径实例样例（前5个）:")
            for i, (h, t) in enumerate(list(body_instances)[:5]):
                print(f"  {h} -> {t}")
        
        # 计算支持度（头部和身体的交集）
        support_instances = head_instances.intersection(body_instances)
        support = len(support_instances)
        print(f"支持度实例数: {support}")
        
        if support > 0:
            print(f"支持度实例样例（前5个）:")
            for i, (h, t) in enumerate(list(support_instances)[:5]):
                print(f"  {h} -> {t}")
        
        return {
            'headSize': head_size,
            'bodySize': body_size, 
            'support': support,
            'confidence': support / body_size if body_size > 0 else 0
        }


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
                print(f"已加载 {line_num} 个三元组...")
    
    print(f"数据集加载完成:")
    print(f"  三元组数量: {len(kg.triples)}")
    print(f"  实体数量: {len(kg.entities)}")
    print(f"  关系数量: {len(kg.relations)}")
    
    return kg


def main():
    """主函数"""
    # 数据集路径（相对于当前脚本的路径）
    dataset_path = "../data/FB15k-237/train.txt"
    
    # 要分析的规则
    head_relation = "/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month"
    body_path = "/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month·INVERSE_/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month·/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month"
    
    try:
        # 加载数据集
        kg = load_dataset(dataset_path)
        
        # 检查目标关系是否存在
        target_relation = head_relation
        if target_relation in kg.relations:
            print(f"\n✓ 目标关系 '{target_relation}' 在数据集中存在")
        else:
            print(f"\n✗ 目标关系 '{target_relation}' 在数据集中不存在")
            print("数据集中的相关关系（包含travel关键词）:")
            travel_relations = [r for r in kg.relations if 'travel' in r.lower()]
            for rel in sorted(travel_relations)[:10]:
                print(f"  {rel}")
            if len(travel_relations) > 10:
                print(f"  ... 还有 {len(travel_relations) - 10} 个相关关系")
        
        # 创建规则支持度计算器
        calculator = RuleSupportCalculator(kg)
        
        # 计算规则支持度
        print("\n" + "="*80)
        result = calculator.calculate_rule_support(head_relation, body_path)
        
        # 输出结果
        print("\n" + "="*80)
        print("规则支持度计算结果:")
        print("="*80)
        print(f"规则: {head_relation}")
        print(f"  <= {body_path}")
        print(result)
        print("="*80)
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
