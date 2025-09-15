#!/usr/bin/env python3

from analysis_rule import *

def test_inverse_head():
    """测试INVERSE关系的头部实例计算"""
    
    # 测试规则
    rule_str = "INVERSE_/award/award_category/winners./award/award_honor/ceremony(/m/0gs9p) <= INVERSE_/award/award_category/winners./award/award_honor/ceremony·/award/award_category/nominees./award/award_nomination/nominated_for(/m/0j8f09z)"
    
    # 加载数据集
    try:
        dataset_path = "../data/FB15k-237/train.txt"
        kg = load_dataset(dataset_path)
        
        # 解析规则
        head_relation, body_relations, variable_count, rule_info = RuleParser.parse_rule(rule_str)
        
        print(f"规则: {rule_str}")
        print(f"头部关系: {head_relation}")
        print(f"变量数量: {variable_count}")
        print(f"是否一元: {rule_info.get('is_unary', False)}")
        print(f"固定实体: {rule_info.get('fixed_entity')}")
        print(f"变量位置: {rule_info.get('variable_position')}")
        print(f"头部原子: {rule_info.get('head_atom')}")
        
        # 创建计算器
        calculator = RuleSupportCalculator(kg)
        
        # 手动检查头部实例
        head_atom = rule_info['head_atom']
        head_relation = head_atom['relation']
        head_args = head_atom['args']
        
        print(f"\n手动检查:")
        print(f"头部关系: {head_relation}")
        print(f"头部参数: {head_args}")
        
        # 检查r2h2t中是否有这个关系
        if head_relation in kg.r2h2t:
            print(f"关系 {head_relation} 存在于r2h2t中")
            constant = head_args[0]  # /m/0gs9p
            if constant in kg.r2h2t[head_relation]:
                instances = kg.r2h2t[head_relation][constant]
                print(f"常量 {constant} 的实例数量: {len(instances)}")
                print(f"前5个实例: {list(instances)[:5]}")
            else:
                print(f"常量 {constant} 不存在于 {head_relation} 中")
                # 列出该关系的所有keys
                print(f"该关系的前10个keys: {list(kg.r2h2t[head_relation].keys())[:10]}")
        else:
            print(f"关系 {head_relation} 不存在于r2h2t中")
            # 检查原始关系
            original_relation = head_relation[8:]  # 去掉INVERSE_
            if original_relation in kg.r2h2t:
                print(f"原始关系 {original_relation} 存在")
            else:
                print(f"原始关系 {original_relation} 也不存在")
        
        # 使用计算器方法
        print(f"\n使用计算器方法:")
        head_instances = calculator._get_head_instances(rule_info)
        print(f"头部实例数量: {len(head_instances)}")
        if head_instances:
            print(f"前5个实例: {list(head_instances)[:5]}")
        
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_inverse_head()