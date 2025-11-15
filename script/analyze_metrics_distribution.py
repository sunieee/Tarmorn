import json
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import os

def is_unary_rule(rule_str):
    """
    判断规则是否为一元规则
    一元规则包含常量，例如 (X,/m/04dn09n) 或 (X,·)
    """
    return '(' in rule_str and ('·)' in rule_str or '/m/' in rule_str.split('(')[-1])

def is_binary_rule(rule_str):
    """
    判断规则是否为二元规则
    二元规则只包含两个变量，例如 (X,Y)
    """
    if '(' not in rule_str:
        return False
    args = rule_str.split('(')[-1].rstrip(')')
    # 简单判断：如果不包含常量标记，则为二元规则
    return '·' not in args and '/m/' not in args

def analyze_file(file_path):
    """分析JSON文件中的规则分布"""
    print(f"正在加载文件: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 存储一元和二元规则的指标
    unary_metrics = {
        'support': [],
        'headSize': [],
        'bodySize': [],
        'confidence': []
    }
    
    binary_metrics = {
        'support': [],
        'headSize': [],
        'bodySize': [],
        'confidence': []
    }
    
    # 统计规则数量
    unary_count = 0
    binary_count = 0
    other_count = 0
    
    # 遍历所有规则
    for head_rule, body_rules in data.items():
        for body_rule, metrics in body_rules.items():
            # 判断是一元规则还是二元规则
            # 这里我们检查body_rule（因为它是规则的主体部分）
            if is_unary_rule(body_rule):
                unary_count += 1
                unary_metrics['support'].append(metrics.get('support', 0))
                unary_metrics['headSize'].append(metrics.get('headSize', 0))
                unary_metrics['bodySize'].append(metrics.get('bodySize', 0))
                unary_metrics['confidence'].append(metrics.get('confidence', 0))
            elif is_binary_rule(body_rule):
                binary_count += 1
                binary_metrics['support'].append(metrics.get('support', 0))
                binary_metrics['headSize'].append(metrics.get('headSize', 0))
                binary_metrics['bodySize'].append(metrics.get('bodySize', 0))
                binary_metrics['confidence'].append(metrics.get('confidence', 0))
            else:
                other_count += 1
    
    print(f"\n规则统计:")
    print(f"  一元规则数量: {unary_count}")
    print(f"  二元规则数量: {binary_count}")
    print(f"  其他规则数量: {other_count}")
    print(f"  总规则数量: {unary_count + binary_count + other_count}")
    
    return unary_metrics, binary_metrics

def print_statistics(metrics, rule_type):
    """打印统计信息"""
    print(f"\n{rule_type}规则统计信息:")
    for metric_name, values in metrics.items():
        if len(values) > 0:
            print(f"  {metric_name}:")
            print(f"    最小值: {np.min(values):.2f}")
            print(f"    最大值: {np.max(values):.2f}")
            print(f"    平均值: {np.mean(values):.2f}")
            print(f"    中位数: {np.median(values):.2f}")
            print(f"    标准差: {np.std(values):.2f}")

def plot_distributions(unary_metrics, binary_metrics, output_dir):
    """绘制分布图"""
    metrics_to_plot = ['support', 'headSize', 'bodySize', 'confidence']
    
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    
    # 创建2x2的子图
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('规则指标分布对比 (一元规则 vs 二元规则)', fontsize=16, fontweight='bold')
    
    for idx, metric in enumerate(metrics_to_plot):
        row = idx // 2
        col = idx % 2
        ax = axes[row, col]
        
        unary_data = unary_metrics[metric]
        binary_data = binary_metrics[metric]
        
        # 计算合适的bins
        if len(unary_data) > 0 and len(binary_data) > 0:
            all_data = unary_data + binary_data
            bins = np.linspace(min(all_data), max(all_data), 50)
            
            # 绘制直方图
            ax.hist(unary_data, bins=bins, alpha=0.6, label=f'Unary Rules (n={len(unary_data)})', 
                   color='blue', edgecolor='black')
            ax.hist(binary_data, bins=bins, alpha=0.6, label=f'Binary Rules (n={len(binary_data)})', 
                   color='red', edgecolor='black')
            
            # 添加平均值线
            if len(unary_data) > 0:
                ax.axvline(np.mean(unary_data), color='blue', linestyle='--', 
                          linewidth=2, label=f'Unary Mean: {np.mean(unary_data):.2f}')
            if len(binary_data) > 0:
                ax.axvline(np.mean(binary_data), color='red', linestyle='--', 
                          linewidth=2, label=f'Binary Mean: {np.mean(binary_data):.2f}')
            
            ax.set_xlabel(metric.capitalize(), fontsize=12)
            ax.set_ylabel('Frequency', fontsize=12)
            ax.set_title(f'{metric.capitalize()} Distribution', fontsize=14, fontweight='bold')
            ax.legend(loc='best')
            ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # 保存图片
    output_file = os.path.join(output_dir, 'metrics_distribution.png')
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n分布图已保存到: {output_file}")
    
    # 显示图片
    plt.show()
    
    # 创建单独的箱线图
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('规则指标箱线图对比 (一元规则 vs 二元规则)', fontsize=16, fontweight='bold')
    
    for idx, metric in enumerate(metrics_to_plot):
        row = idx // 2
        col = idx % 2
        ax = axes[row, col]
        
        unary_data = unary_metrics[metric]
        binary_data = binary_metrics[metric]
        
        # 绘制箱线图
        data_to_plot = []
        labels = []
        if len(unary_data) > 0:
            data_to_plot.append(unary_data)
            labels.append('Unary')
        if len(binary_data) > 0:
            data_to_plot.append(binary_data)
            labels.append('Binary')
        
        if data_to_plot:
            bp = ax.boxplot(data_to_plot, labels=labels, patch_artist=True, 
                           notch=True, showmeans=True)
            
            # 设置颜色
            colors = ['lightblue', 'lightcoral']
            for patch, color in zip(bp['boxes'], colors[:len(data_to_plot)]):
                patch.set_facecolor(color)
            
            ax.set_ylabel(metric.capitalize(), fontsize=12)
            ax.set_title(f'{metric.capitalize()} Box Plot', fontsize=14, fontweight='bold')
            ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    
    # 保存箱线图
    output_file_box = os.path.join(output_dir, 'metrics_boxplot.png')
    plt.savefig(output_file_box, dpi=300, bbox_inches='tight')
    print(f"箱线图已保存到: {output_file_box}")
    
    plt.show()

def main():
    # 文件路径
    file_path = r"c:\Users\sy650\IdeaProjects\Tarmorn\out\FB15k-237\atom2formula2metric.json"
    
    # 输出目录
    output_dir = os.path.dirname(file_path)
    
    # 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"错误: 文件不存在 - {file_path}")
        return
    
    # 分析文件
    unary_metrics, binary_metrics = analyze_file(file_path)
    
    # 打印统计信息
    print_statistics(unary_metrics, "一元")
    print_statistics(binary_metrics, "二元")
    
    # 绘制分布图
    plot_distributions(unary_metrics, binary_metrics, output_dir)

if __name__ == "__main__":
    main()
