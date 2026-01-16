#!/bin/bash

# 定义阈值
threshold=3

# 遍历 out/ 目录下的所有数据集目录
for dataset_dir in out/*/; do
    # 移除末尾的斜杠，获取数据集名称
    dataset=$(basename "$dataset_dir")
    
    # 进入数据集目录
    cd "$dataset_dir" || continue
    rm -rf rules-*-5
    rm -rf rules-*-3
    
    # 查找所有 rules-* 文件（排除已经是 rules-*-5 的文件）
    for input_file in rules-*; do
        # 跳过不存在的文件（通配符未匹配时）
        [ ! -f "$input_file" ] && continue
        
        # 定义输出文件名
        output_file="${input_file}-${threshold}"
        
        # 统计原始文件行数
        original_lines=$(wc -l < "$input_file" 2>/dev/null || echo 0)
        
        # 使用awk筛选第二列大于等于threshold的行
        awk -v threshold="$threshold" '$2 >= threshold' "$input_file" > "$output_file"
        
        # 统计输出文件行数
        filtered_lines=$(wc -l < "$output_file" 2>/dev/null || echo 0)
        
        # 计算行数差别
        diff=$((original_lines - filtered_lines))
        
        # 打印结果
        echo "[$dataset] $input_file -> $output_file"
        echo "  原始行数: $original_lines, 过滤后行数: $filtered_lines, 减少: $diff 行"
    done
    
    # 返回原目录
    cd - > /dev/null || exit
done
