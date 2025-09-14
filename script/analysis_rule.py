#!/usr/bin/env python3
"""
规则支持度计算器
计算给定规则的 headSize, bodySize 和 support

规则：
/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month <= 
/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month·INVERSE_/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month·/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month

这个规则等价于：month(X,Y) <= month(X,A), month(B,A), month(B,Y)

算法特点：
1. 基于r2h2t索引结构，提供高效的关系查询
2. 自动建立inverse关系索引
3. 使用逐级连接算法计算复合关系路径
4. 通过连接节点优化，避免不必要的笛卡尔积计算
5. 将中间结果存储到r2h2t索引中，支持复用
"""

import os
import json
from collections import defaultdict, Counter
from typing import Set, Tuple, Dict, List


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
    
    def add_triple(self, head: str, relation: str, tail: str):
        """添加三元组到知识图谱"""
        self.triples.add((head, relation, tail))
        self.entities.add(head)
        self.entities.add(tail)
        self.relations.add(relation)
        
        # 建立r2h2t索引
        self.r2h2t[relation][head].add(tail)
        
        # 建立inverse关系索引
        inverse_relation = f"INVERSE_{relation}"
        self.relations.add(inverse_relation)
        self.r2h2t[inverse_relation][tail].add(head)
    
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
        """获取关系的逆关系"""
        if relation.startswith("INVERSE_"):
            # 如果已经是逆关系，返回原关系
            return relation[8:]
        else:
            # 如果是正向关系，返回逆关系
            return f"INVERSE_{relation}"
    
    def save_r2h2t_to_json(self, filepath: str):
        """将r2h2t索引保存到JSON文件"""
        print(f"正在保存r2h2t索引到文件: {filepath}")
        
        # 转换r2h2t为可序列化的格式
        serializable_r2h2t = {}
        for relation, h2t_dict in self.r2h2t.items():
            serializable_r2h2t[relation] = {}
            for head, tails in h2t_dict.items():
                serializable_r2h2t[relation][head] = list(tails)
        
        # 保存到JSON文件
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(serializable_r2h2t, f, ensure_ascii=False, indent=2)
        
        print(f"r2h2t索引已保存，包含 {len(serializable_r2h2t)} 个关系")


class RuleSupportCalculator:
    """规则支持度计算器，基于r2h2t索引和逐级连接算法"""
    
    def __init__(self, kg: KnowledgeGraph):
        self.kg = kg
    
    def parse_relation_path(self, path: str) -> List[str]:
        """
        解析关系路径
        返回: [relation1, relation2, relation3, ...]
        注意：INVERSE_前缀已经在path中，不需要额外处理
        """
        relations = []
        parts = path.split('·')
        
        for part in parts:
            part = part.strip()
            relations.append(part)
        
        return relations
    
    def join_relations(self, r1: str, r2: str) -> str:
        """
        连接两个关系：r1 · r2
        返回新关系的名称，并将结果存储到kg.r2h2t中
        
        高效算法：
        1. 找到r1的所有tail和r2的所有head的交集（连接节点）
        2. 对每个连接节点，计算其对应的head和tail的笛卡尔积
        """
        new_relation_name = f"{r1}·{r2}"
        
        # 如果已经计算过，直接返回
        if new_relation_name in self.kg.r2h2t:
            return new_relation_name
        
        print(f"  连接 {r1} · {r2}")
        
        # 获取r1和r2的r2h2t映射
        r1_h2t = self.kg.r2h2t.get(r1, {})
        r2_h2t = self.kg.r2h2t.get(r2, {})
        
        # 高效获取连接节点：r1的所有tail ∩ r2的所有head
        # 使用逆关系获取r1的所有tail
        inverse_r1 = self.kg.get_inverse_relation(r1)
        r1_tails = set(self.kg.r2h2t.get(inverse_r1, {}).keys())
        
        r2_heads = set(r2_h2t.keys())
        connection_nodes = r1_tails.intersection(r2_heads)
        
        print(f"    找到 {len(connection_nodes)} 个连接节点")
        
        # 对每个连接节点计算笛卡尔积
        result = defaultdict(set)
        for node in connection_nodes:
            # 高效获取所有指向该节点的r1的head：通过逆关系索引直接获取
            inverse_r1 = self.kg.get_inverse_relation(r1)
            r1_heads_to_node = self.kg.r2h2t.get(inverse_r1, {}).get(node, set())
            
            # 找到从该节点出发的r2的tail
            r2_tails_from_node = r2_h2t.get(node, set())
            
            # 笛卡尔积
            for h1 in r1_heads_to_node:
                for t2 in r2_tails_from_node:
                    if h1 != t2:  # 避免自环
                        result[h1].add(t2)
        
        # 将结果存储到kg的r2h2t中
        self.kg.r2h2t[new_relation_name] = result
        self.kg.relations.add(new_relation_name)
        
        instances_count = sum(len(tails) for tails in result.values())
        print(f"    生成 {instances_count} 个实例")
        
        return new_relation_name
    
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
        
        print(f"计算路径实例: {' · '.join(relation_path)}")
        
        # 从右到左逐级连接
        current_relation = relation_path[-1]  # 最右边的关系
        
        # 从倒数第二个关系开始，向左连接
        for i in range(len(relation_path) - 2, -1, -1):
            left_relation = relation_path[i]
            current_relation = self.join_relations(left_relation, current_relation)
        
        # 返回最终关系的实例数量
        return self.kg.get_relation_instances_count(current_relation)
    
    def calculate_rule_support(self, head_relation: str, body_path: str) -> Dict[str, int]:
        """
        计算规则的支持度
        
        Args:
            head_relation: 头部关系
            body_path: 身体关系路径，用·连接
        
        Returns:
            包含 headSize, bodySize, support 的字典
        """
        print(f"\n计算规则: {head_relation} <= {body_path}")
        
        # 解析身体关系路径
        body_relations = self.parse_relation_path(body_path)
        print(f"解析的身体关系路径: {body_relations}")
        
        # 获取头部关系的实例数
        head_size = self.kg.get_relation_instances_count(head_relation)
        print(f"头部关系实例数: {head_size}")
        
        # 计算身体路径的实例数
        body_size = self.find_path_instances_count(body_relations)
        print(f"身体路径实例数: {body_size}")
        
        # 计算支持度（头部和身体的交集）
        # 获取头部关系的实际实例
        head_instances = self.kg.get_relation_pairs(head_relation)
        
        # 获取身体路径的实际实例
        body_instances = self.get_path_instances(body_relations)
        
        # 计算交集
        support_instances = head_instances.intersection(body_instances)
        support = len(support_instances)
        print(f"支持度实例数: {support}")
        
        return {
            'headSize': head_size,
            'bodySize': body_size, 
            'support': support,
            'confidence': support / body_size if body_size > 0 else 0
        }
    
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
    print(f"  实体数量: {len(kg.entities):,}")
    print(f"  原始关系数量: {len([r for r in kg.relations if not r.startswith('INVERSE_')]):,}")
    print(f"  总关系数量（含逆关系）: {len(kg.relations):,}")
    
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
        
        # 保存r2h2t索引到JSON文件
        r2h2t_output_path = "r2h2t_index.json"
        kg.save_r2h2t_to_json(r2h2t_output_path)
        
        # 输出一些统计信息
        print(f"\n统计信息:")
        print(f"  原始关系数量: {len([r for r in kg.relations if not r.startswith('INVERSE_') and '·' not in r])}")
        print(f"  逆关系数量: {len([r for r in kg.relations if r.startswith('INVERSE_')])}")
        print(f"  复合关系数量: {len([r for r in kg.relations if '·' in r])}")
        print(f"  总关系数量: {len(kg.relations)}")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
