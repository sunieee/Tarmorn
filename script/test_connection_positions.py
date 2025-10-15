#!/usr/bin/env python3
"""
测试连接变量位置的处理逻辑
"""

def test_connection_positions():
    """测试不同连接变量位置的处理"""
    
    print("测试连接变量位置的处理逻辑")
    print("=" * 50)
    
    test_cases = [
        {
            'name': '情况1: rel1(A,X), rel2(A,/m/const) - A连接，X自由',
            'atoms': [
                {'relation': 'rel1', 'args': ['A', 'X']},  # A在位置0，X在位置1
                {'relation': 'rel2', 'args': ['A', '/m/const']}  # A在位置0，常量在位置1
            ],
            'connecting_var': 'A',
            'free_var': 'X',
            'expected': 'INVERSE_rel1 ∘ rel2',
            'explanation': 'X在tail位置，需要INVERSE_rel1；A在rel2的head位置，正向连接'
        },
        {
            'name': '情况2: rel1(X,A), rel2(/m/const,A) - A连接，X自由',
            'atoms': [
                {'relation': 'rel1', 'args': ['X', 'A']},  # X在位置0，A在位置1
                {'relation': 'rel2', 'args': ['/m/const', 'A']}  # 常量在位置0，A在位置1
            ],
            'connecting_var': 'A',
            'free_var': 'X',
            'expected': 'rel1 ∘ INVERSE_rel2',
            'explanation': 'X在head位置，正向；A在rel2的tail位置，需要INVERSE_rel2'
        },
        {
            'name': '情况3: rel1(A,X), rel2(/m/const,A) - A连接，X自由',
            'atoms': [
                {'relation': 'rel1', 'args': ['A', 'X']},  # A在位置0，X在位置1
                {'relation': 'rel2', 'args': ['/m/const', 'A']}  # 常量在位置0，A在位置1
            ],
            'connecting_var': 'A',
            'free_var': 'X',
            'expected': 'INVERSE_rel1 ∘ INVERSE_rel2',
            'explanation': 'X在tail位置，需要INVERSE_rel1；A在rel2的tail位置，需要INVERSE_rel2'
        },
        {
            'name': '情况4: rel1(X,A), rel2(A,/m/const) - A连接，X自由',
            'atoms': [
                {'relation': 'rel1', 'args': ['X', 'A']},  # X在位置0，A在位置1
                {'relation': 'rel2', 'args': ['A', '/m/const']}  # A在位置0，常量在位置1
            ],
            'connecting_var': 'A',
            'free_var': 'X',
            'expected': 'rel1 ∘ rel2',
            'explanation': 'X在head位置，正向；A在rel2的head位置，正向连接'
        }
    ]
    
    for case in test_cases:
        print(f"\n{case['name']}")
        print(case['explanation'])
        print("原子:")
        for i, atom in enumerate(case['atoms']):
            print(f"  {i+1}: {atom['relation']}({', '.join(atom['args'])})")
        
        # 找到连接变量的位置
        connecting_var = case['connecting_var']
        free_var = case['free_var']
        
        # 找到自由变量在第一个原子中的位置
        free_var_pos = -1
        for j, arg in enumerate(case['atoms'][0]['args']):
            if arg == free_var:
                free_var_pos = j
                break
        
        # 找到连接变量在每个原子中的位置
        connecting_positions = []
        for atom in case['atoms']:
            for j, arg in enumerate(atom['args']):
                if arg == connecting_var:
                    connecting_positions.append(j)
                    break
        
        print(f"自由变量 {free_var} 在第一个原子中的位置: {free_var_pos}")
        print(f"连接变量 {connecting_var} 的位置: {connecting_positions}")
        print(f"预期连接方式: {case['expected']}")
        
        # 应用新的连接逻辑
        result_relation = case['atoms'][0]['relation']
        
        # 如果自由变量在第一个原子的tail位置，取逆
        if free_var_pos == 1:
            result_relation = f"INVERSE_{result_relation}"
        
        # 连接第二个原子
        second_relation = case['atoms'][1]['relation']
        if len(connecting_positions) >= 2 and connecting_positions[1] == 1:
            second_relation = f"INVERSE_{second_relation}"
        
        actual_result = f"{result_relation} ∘ {second_relation}"
        print(f"实际连接方式: {actual_result}")
        
        if actual_result == case['expected']:
            print("✓ 正确")
        else:
            print("✗ 错误")

if __name__ == "__main__":
    test_connection_positions()