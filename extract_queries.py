#!/usr/bin/env python3
"""
从out/FB15k-237目录下所有log文件中提取以{"query":开头的行，
去重后保存为JSON格式。
"""

import json
import os
from pathlib import Path


def extract_queries_from_logs(log_dir, output_file):
    """
    从指定目录的所有log文件中提取query行，去重后保存到JSON文件
    
    Args:
        log_dir: log文件所在目录
        output_file: 输出的JSON文件路径
    """
    log_dir = Path(log_dir)
    seen_queries = set()  # 用于去重
    results = []
    
    # 获取所有log文件
    log_files = sorted(log_dir.glob("*.log"))
    print(f"找到 {len(log_files)} 个log文件")
    
    # 遍历所有log文件
    for log_file in log_files:
        print(f"处理: {log_file.name}")
        processed_count = 0
        
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # 检查是否以{"query":开头
                    if line.startswith('{"query":'):
                        try:
                            # 解析JSON以验证格式
                            data = json.loads(line)
                            
                            # 提取query字段用于去重
                            query = data.get("query", "")
                            
                            # 如果是新的query，添加到结果中
                            if query and query not in seen_queries:
                                seen_queries.add(query)
                                results.append(data)
                                processed_count += 1
                        except json.JSONDecodeError as e:
                            print(f"  警告: {log_file.name} 第 {line_num} 行JSON解析失败: {e}")
                            continue
        except Exception as e:
            print(f"  错误: 读取文件 {log_file.name} 失败: {e}")
            continue
        
        print(f"  从该文件提取了 {processed_count} 条唯一记录")
    
    # 保存到输出文件
    print(f"\n总共提取了 {len(results)} 条唯一记录")
    print(f"保存到: {output_file}")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('[\n')
        for i, record in enumerate(results):
            json_str = json.dumps(record, ensure_ascii=False)
            if i < len(results) - 1:
                f.write(f'{json_str},\n')
            else:
                f.write(f'{json_str}\n')
        f.write(']\n')
    
    print("完成！")


if __name__ == "__main__":
    log_directory = "out/FB15k-237"
    output_path = "out/FB15k-237/example.json"
    
    extract_queries_from_logs(log_directory, output_path)
