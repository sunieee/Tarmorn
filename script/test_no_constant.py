#!/usr/bin/env python3
"""
测试无常量一元规则处理
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_no_constant_logic():
    """测试无常量一元规则的处理逻辑"""
    
    print("测试无常量一元规则的处理逻辑")
    print("=" * 50)
    
    # 模拟两种情况
    test_cases = [
        {
            'name': '规则4: ceremony(/m/01ck6v,Y) <= award_winner(Y,A)',
            'head_var': 'Y',
            'head_position': 1,  # Y在头部的第二个位置
            'body_first_atom': {'relation': 'award_winner', 'args': ['Y', 'A']},
            'body_var_position': 0,  # Y在body第一个原子的第一个位置
            'expected': 'get all heads from award_winner'
        },
        {
            'name': '规则5: ceremony(X,/m/07z31v) <= award(A,X)',
            'head_var': 'X', 
            'head_position': 0,  # X在头部的第一个位置
            'body_first_atom': {'relation': 'award', 'args': ['A', 'X']},
            'body_var_position': 1,  # X在body第一个原子的第二个位置
            'expected': 'get all heads from INVERSE_award'
        }
    ]
    
    for case in test_cases:
        print(f"\n{case['name']}")
        print(f"头部变量 {case['head_var']} 在头部位置: {case['head_position']}")
        print(f"该变量在body第一个原子中的位置: {case['body_var_position']}")
        
        if case['body_var_position'] == 0:
            print(f"结论: 变量在第一个位置 -> 获取 {case['body_first_atom']['relation']} 的所有头部实体")
        else:
            print(f"结论: 变量在第二个位置 -> 获取 INVERSE_{case['body_first_atom']['relation']} 的所有头部实体")
        
        print(f"预期: {case['expected']}")

if __name__ == "__main__":
    test_no_constant_logic()