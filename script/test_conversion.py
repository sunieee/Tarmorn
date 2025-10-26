#!/usr/bin/env python3
"""
测试规则转换
"""

from analysis_rule import RuleParser

# 测试用例
test_cases = [
    # 一元规则 - 有中间变量，无body常量
    {
        'input': '/tv/tv_program/languages(X,/m/02h40lc) <= /tv/tv_writer/tv_programs./tv/tv_program_writer_relationship/tv_program(A,X)',
        'expected': '/tv/tv_program/languages(/m/02h40lc) <= INVERSE_/tv/tv_writer/tv_programs./tv/tv_program_writer_relationship/tv_program(·)'
    },
    # 二元规则 - 单个原子
    {
        'input': '/education/university/local_tuition./measurement_unit/dated_money_value/currency(X,Y) <= /organization/endowed_organization/endowment./measurement_unit/dated_money_value/currency(X,Y)',
        'expected': '/education/university/local_tuition./measurement_unit/dated_money_value/currency <= /organization/endowed_organization/endowment./measurement_unit/dated_money_value/currency'
    },
    # 二元规则 - 多个原子 (L3)
    {
        'input': '/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)',
        'expected': '/award/award_category/winners./award/award_honor/ceremony <= /award/award_category/winners./award/award_honor/award_winner·INVERSE_/award/award_ceremony/awards_presented./award/award_honor/award_winner'
    }
]

print("测试规则转换：\n")
for i, test in enumerate(test_cases, 1):
    input_rule = test['input']
    expected = test['expected']
    
    # 执行转换
    head_part, body_part = input_rule.split('<=', 1)
    head_part = head_part.strip()
    body_part = body_part.strip()
    
    try:
        result = RuleParser._normalize_to_simplified(head_part, body_part)
        # 美化：添加空格
        result_beautified = result.replace('·', ' · ')
        expected_beautified = expected.replace('·', ' · ')
        
        # 检查结果
        match = result == expected
        print(f"测试 {i}: {'✓ 通过' if match else '✗ 失败'}")
        print(f"  输入:   {input_rule}")
        print(f"  期望:   {expected_beautified}")
        print(f"  结果:   {result_beautified}")
        if not match:
            print(f"  差异:   期望和结果不匹配")
        print()
        
    except Exception as e:
        print(f"测试 {i}: ✗ 错误")
        print(f"  输入:   {input_rule}")
        print(f"  错误:   {e}")
        print()
