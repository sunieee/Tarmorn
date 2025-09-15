#!/usr/bin/env python3
"""
简化版规则测试脚本 - 用于调试
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from analysis_rule import *

def test_simple_rule():
    """测试简单规则"""
    # 数据集路径
    dataset_path = "../data/FB15k-237/train.txt"
    
    try:
        # 加载数据集
        print("加载数据集...")
        kg = load_dataset(dataset_path)
        
        # 首先检查一些基本的关系是否存在
        test_relation = "/award/award_category/winners./award/award_honor/ceremony"
        print(f"\n检查关系存在性:")
        print(f"  {test_relation} 存在: {test_relation in kg.relations}")
        print(f"  该关系的实例数: {kg.get_relation_instances_count(test_relation)}")
        
        # 查看前几个实例
        instances = kg.get_relation_pairs(test_relation)
        print(f"  前5个实例:")
        for i, (h, t) in enumerate(list(instances)[:5]):
            print(f"    {h} -> {t}")
        
        # 测试简单的二元规则
        # simple_rule = "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)"
        simple_rule = "/award/award_category/winners./award/award_honor/ceremony  <= /award/award_category/winners./award/award_honor/ceremony·/award/award_ceremony/awards_presented./award/award_honor/award_winner·INVERSE_/award/award_ceremony/awards_presented./award/award_honor/award_winner"
        print(f"\n测试简单规则: {simple_rule}")
        
        # 解析规则
        head_relation, body_relations, variable_count, rule_info = RuleParser.parse_rule(simple_rule)
        print(f"解析结果:")
        print(f"  head_relation: {head_relation}")
        print(f"  body_relations: {body_relations}")
        print(f"  variable_count: {variable_count}")
        print(f"  rule_info: {rule_info}")
        
        # 创建计算器
        calculator = RuleSupportCalculator(kg)
        
        # 计算连接算法结果
        if variable_count == 2:
            print(f"\n计算二元规则（连接算法）:")
            head_instances = kg.get_relation_pairs(head_relation)
            body_instances = calculator.get_binary_instances_join(body_relations)
            
            head_size = len(head_instances)
            body_size = len(body_instances)
            support = len(head_instances.intersection(body_instances))
            confidence = support / body_size if body_size > 0 else 0
            
            print(f"  Head实例数: {head_size}")
            print(f"  Body实例数: {body_size}")
            print(f"  Support: {support}")
            print(f"  Confidence: {confidence}")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simple_rule()