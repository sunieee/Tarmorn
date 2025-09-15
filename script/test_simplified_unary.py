#!/usr/bin/env python3

from analysis_rule import RuleParser

def test_simplified_unary_rule():
    """测试简写格式的一元规则解析"""
    
    # 测试用例
    test_rules = [
        # 简写格式的一元规则（用户提到的例子）
        "/award/award_category/winners./award/award_honor/ceremony(/m/05pd94v) <= /award/award_category/winners./award/award_honor/ceremony·/award/award_ceremony/awards_presented./award/award_honor/award_winner(/m/0m2l9)",
        
        # 完整格式的一元规则（对比）
        "/award/award_category/winners./award/award_honor/ceremony(X,/m/05pd94v) <= /award/award_category/winners./award/award_honor/ceremony(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(A,/m/0m2l9)",
        
        # 简写格式的二元规则
        "/award/award_category/winners./award/award_honor/ceremony <= /award/award_category/winners./award/award_honor/ceremony·/award/award_ceremony/awards_presented./award/award_honor/award_winner"
    ]
    
    for i, rule_str in enumerate(test_rules, 1):
        print(f"\n=== 测试规则 {i} ===")
        print(f"规则: {rule_str}")
        
        try:
            head_relation, body_relations, variable_count, rule_info = RuleParser.parse_rule(rule_str)
            
            print(f"解析结果:")
            print(f"  简写格式: {rule_info.get('is_simplified', False)}")
            print(f"  头部关系: {head_relation}")
            print(f"  身体关系: {body_relations}")
            print(f"  变量数量: {variable_count}")
            print(f"  是否一元: {rule_info.get('is_unary', False)}")
            print(f"  固定实体: {rule_info.get('fixed_entity')}")
            print(f"  body常量: {rule_info.get('body_constant')}")
            print(f"  自由变量: {rule_info.get('free_variables', [])}")
            print(f"  头部原子: {rule_info.get('head_atom', {})}")
            
        except Exception as e:
            print(f"解析失败: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_simplified_unary_rule()