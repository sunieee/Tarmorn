#!/usr/bin/env python3
"""
简单测试连接变量分析逻辑
"""

def test_connection_logic():
    """测试连接变量分析逻辑"""
    
    # 模拟body_atoms结构
    body_atoms = [
        {'relation': '/award/award_category/winners./award/award_honor/ceremony', 'args': ['X', 'A']},
        {'relation': '/award/award_category/winners./award/award_honor/ceremony', 'args': ['/m/0257w4', 'A']}
    ]
    
    print("Body atoms:")
    for i, atom in enumerate(body_atoms):
        print(f"  {i}: {atom['relation']}({', '.join(atom['args'])})")
    
    # 找到连接变量
    var_counts = {}
    for atom in body_atoms:
        args = atom.get('args', [])
        for arg in args:
            if len(arg) == 1:  # 变量（单字符）
                var_counts[arg] = var_counts.get(arg, 0) + 1
    
    print(f"\nVariable counts: {var_counts}")
    
    # 找到真正的连接变量（出现次数 >= 2）
    connecting_vars = [var for var, count in var_counts.items() if count >= 2]
    print(f"Connecting variables: {connecting_vars}")
    
    if connecting_vars:
        connecting_var = connecting_vars[0]
        print(f"Using connecting variable: {connecting_var}")
        
        # 找到这个变量在每个原子中的位置
        var_positions = []
        for atom in body_atoms:
            args = atom.get('args', [])
            position = -1
            for i, arg in enumerate(args):
                if arg == connecting_var:
                    position = i
                    break
            var_positions.append(position)
        
        print(f"Variable {connecting_var} positions: {var_positions}")
        
        # 判断连接方式
        if len(var_positions) >= 2:
            if var_positions[0] == 1 and var_positions[1] == 1:
                print("Both variables at position 1 (tail) -> need INVERSE for second relation")
                print("Connection should be: rel1 ∘ INVERSE_rel2")
            else:
                print("Normal connection: rel1 ∘ rel2")

if __name__ == "__main__":
    test_connection_logic()