#!/usr/bin/env python3
"""
测试规则规范化和处理
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from analysis_rule import RuleParser

def test_rule_normalization():
    """测试规则规范化功能"""
    
    print("测试规则规范化功能")
    print("=" * 60)
    
    test_cases = [
        {
            'name': '一元规则案例1',
            'rule': '/award/award_category/winners./award/award_honor/ceremony(/m/0gs96,X) <= /award/award_category/winners./award/award_honor/ceremony(A,X), /award/award_category/nominees./award/award_nomination/nominated_for(A,/m/02r79_h)',
            'expected_type': 'unary',
            'expected_normalized': 'INVERSE_/award/award_category/winners./award/award_honor/ceremony(/m/0gs96) <= INVERSE_/award/award_category/winners./award/award_honor/ceremony·/award/award_category/nominees./award/award_nomination/nominated_for(/m/02r79_h)'
        },
        {
            'name': '一元规则案例2', 
            'rule': '/award/award_category/winners./award/award_honor/ceremony(X,/m/01xqqp) <= /award/award_category/winners./award/award_honor/ceremony(X,A), /award/award_category/winners./award/award_honor/ceremony(/m/0257w4,A)',
            'expected_type': 'unary',
            'expected_normalized': '/award/award_category/winners./award/award_honor/ceremony(/m/01xqqp) <= /award/award_category/winners./award/award_honor/ceremony·INVERSE_/award/award_category/winners./award/award_honor/ceremony(/m/0257w4)'
        }
    ]
    
    for case in test_cases:
        print(f"\n{case['name']}")
        print(f"原始规则: {case['rule']}")
        
        try:
            # 解析规则
            head_relation, body_relations, variable_count, rule_info = RuleParser.parse_rule(case['rule'])
            
            print(f"规范化规则: {rule_info.get('normalized_rule', 'N/A')}")
            print(f"规则类型: {'一元' if variable_count == 1 else '二元'}")
            print(f"头部关系: {head_relation}")
            print(f"身体关系: {body_relations}")
            
            # 检查关键信息
            if variable_count == 1:
                print(f"头部常量: {rule_info.get('head_constant')}")
                print(f"头部变量位置: {rule_info.get('head_variable_position')}")
                print(f"身体常量: {rule_info.get('body_constant')}")
            
            print("✓ 解析成功")
            
        except Exception as e:
            print(f"✗ 解析失败: {str(e)}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_rule_normalization()