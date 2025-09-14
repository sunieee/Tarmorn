#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
简化版规则支持度计算器
"""

import os
from collections import defaultdict

# 数据集路径
dataset_path = "../data/FB15k-237/train.txt"

# 目标关系
target_relation = "/travel/travel_destination/climate./travel/travel_destination_monthly_climate/month"

print("开始分析...")
print(f"数据集路径: {dataset_path}")
print(f"目标关系: {target_relation}")

# 检查文件是否存在
if not os.path.exists(dataset_path):
    print(f"错误：文件不存在 {dataset_path}")
    exit(1)

print("文件存在，开始加载...")

# 存储关系实例
relation_instances = defaultdict(list)

# 读取数据
line_count = 0
target_count = 0

try:
    with open(dataset_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_count += 1
            line = line.strip()
            if not line:
                continue
            
            parts = line.split('\t')
            if len(parts) != 3:
                continue
            
            head, relation, tail = parts
            
            # 只保存目标关系的实例
            if relation == target_relation:
                relation_instances[relation].append((head, tail))
                target_count += 1
            
            if line_count % 100000 == 0:
                print(f"已处理 {line_count} 行，找到目标关系 {target_count} 个")

    print(f"数据加载完成：")
    print(f"总行数: {line_count}")
    print(f"目标关系实例数: {target_count}")

    # 计算规则支持度
    head_instances = set(relation_instances[target_relation])
    head_size = len(head_instances)
    
    print(f"\nheadSize: {head_size}")
    
    # 对于这个特定规则，body部分相当于寻找路径：
    # X --rel--> A, B --rel--> A, B --rel--> Y
    # 这等价于寻找所有(X,Y)对，使得存在A,B满足上述条件
    
    # 建立索引
    forward_index = defaultdict(set)  # entity -> set of targets
    backward_index = defaultdict(set)  # entity -> set of sources
    
    for head, tail in relation_instances[target_relation]:
        forward_index[head].add(tail)
        backward_index[tail].add(head)
    
    # 计算body实例：对于路径 rel · INVERSE_rel · rel
    body_instances = set()
    
    for x in forward_index:  # X
        for a in forward_index[x]:  # X --rel--> A
            for b in backward_index[a]:  # B --rel--> A (INVERSE)
                for y in forward_index[b]:  # B --rel--> Y
                    body_instances.add((x, y))
    
    body_size = len(body_instances)
    print(f"bodySize: {body_size}")
    
    # 计算支持度
    support_instances = head_instances.intersection(body_instances)
    support = len(support_instances)
    print(f"support: {support}")
    
    # 计算置信度
    confidence = support / body_size if body_size > 0 else 0
    print(f"confidence: {confidence:.4f}")
    
    print("\n规则分析完成！")
    print("="*50)
    print(f"headSize: {head_size}")
    print(f"bodySize: {body_size}")
    print(f"support: {support}")
    print(f"confidence: {confidence:.4f}")
    print("="*50)

except Exception as e:
    print(f"发生错误: {e}")
    import traceback
    traceback.print_exc()