#!/usr/bin/env python3
"""
调试规则计算差异
"""
import sys
sys.path.insert(0, '.')

# 启用DEBUG模式
import analysis_rule
analysis_rule.DEBUG = True

from analysis_rule import load_dataset, analyze_rule_from_string

# 测试规则
rule = "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_winner/awards_won./award/award_honor/award_winner(B,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,B)"

print("="*80)
print("测试1: 第一次调用 analyze_rule_from_string (DEBUG模式已启用)")
print("="*80)

# 加载知识图谱
kg = load_dataset('../data/FB15k-237/train.txt')
print(f"知识图谱加载完成: {len(kg.triples)} 条三元组\n")

# 第一次分析
result1 = analyze_rule_from_string(rule, kg)
if result1 and result1['join_result']:
    print(f"第一次结果: {result1['join_result']}")
else:
    print("第一次结果: None")

print("\n" + "="*80)
print("测试2: 第二次调用 analyze_rule_from_string (使用同一个kg对象)")
print("="*80)

# 第二次分析（使用同一个kg对象）
result2 = analyze_rule_from_string(rule, kg)
if result2 and result2['join_result']:
    print(f"第二次结果: {result2['join_result']}")
else:
    print("第二次结果: None")

print("\n" + "="*80)
print("测试3: 第三次调用 analyze_rule_from_string (重新加载kg)")
print("="*80)

# 重新加载知识图谱
kg3 = load_dataset('../data/FB15k-237/train.txt')
print(f"知识图谱重新加载完成: {len(kg3.triples)} 条三元组\n")

# 第三次分析（使用新的kg对象）
result3 = analyze_rule_from_string(rule, kg3)
if result3 and result3['join_result']:
    print(f"第三次结果: {result3['join_result']}")
else:
    print("第三次结果: None")

print("\n" + "="*80)
print("结果比较")
print("="*80)

if result1 and result2 and result3:
    print(f"第一次 vs 第二次: {result1['join_result'] == result2['join_result']}")
    print(f"第一次 vs 第三次: {result1['join_result'] == result3['join_result']}")
    print(f"第二次 vs 第三次: {result2['join_result'] == result3['join_result']}")
