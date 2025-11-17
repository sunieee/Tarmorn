#!/usr/bin/env python3
"""
Analyze and visualize split methods comparison.
Reads eval.log and merge_rules.log from each split directory and generates a heatmap.
"""

import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# 设置字体：Times New Roman用于英文和数字，SimHei用于中文
plt.rcParams['font.serif'] = ['Times New Roman', 'DejaVu Serif', 'serif']
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial', 'DejaVu Sans', 'sans-serif']
plt.rcParams['font.family'] = 'serif'
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['mathtext.fontset'] = 'custom'
plt.rcParams['mathtext.rm'] = 'Times New Roman'
plt.rcParams['mathtext.it'] = 'Times New Roman:italic'
plt.rcParams['mathtext.bf'] = 'Times New Roman:bold'


def extract_eval_metrics(eval_log_path: Path) -> dict:
    """
    Extract metrics from eval.log file.
    Expected format:
    Loaded and indexed 386182 rules.
    MRR 0.277908, hits@1 0.198378, hits@3 0.308145
    """
    if not eval_log_path.exists():
        return {}
    
    metrics = {}
    
    try:
        with open(eval_log_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract number of rules loaded
        rules_match = re.search(r'Loaded and indexed (\d+) rules', content)
        if rules_match:
            metrics['loaded_rules'] = int(rules_match.group(1))
        
        # Extract MRR, hits@1, hits@3
        mrr_match = re.search(r'MRR\s+([\d.]+)', content)
        hits1_match = re.search(r'hits@1\s+([\d.]+)', content)
        hits3_match = re.search(r'hits@3\s+([\d.]+)', content)
        
        if mrr_match:
            metrics['MRR'] = float(mrr_match.group(1))
        if hits1_match:
            metrics['hits@1'] = float(hits1_match.group(1))
        if hits3_match:
            metrics['hits@3'] = float(hits3_match.group(1))
            
    except Exception as e:
        print(f"Error reading {eval_log_path}: {e}")
    
    return metrics


def extract_merge_metrics(merge_log_path: Path) -> dict:
    """
    Extract metrics from merge_rules.log file.
    Expected format:
    Total rules: 499023
    """
    if not merge_log_path.exists():
        return {}
    
    metrics = {}
    
    try:
        with open(merge_log_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract total rules
        rules_match = re.search(r'Total rules:\s*(\d+)', content)
        if rules_match:
            metrics['total_rules'] = int(rules_match.group(1))
            
    except Exception as e:
        print(f"Error reading {merge_log_path}: {e}")
    
    return metrics


def get_split_order():
    """Define the order of split methods from simple to complex."""
    return [
        'random_nonoverlap',
        'random_multi_k2',
        'random_multi_k3',
        'edge_cut',
        'vertex_cut',
        'louvain',
        'hub_replication_t50',
        'bfs_expansion_r2',
        'relation_centric'
    ]


def read_split_metrics(base_dir: str, split_names: list) -> pd.DataFrame:
    """Read metrics from all split directories."""
    data = []
    
    base_path = Path(base_dir)
    if not base_path.exists():
        print(f"Error: Directory {base_dir} does not exist")
        return pd.DataFrame()
    
    for split_name in split_names:
        split_dir = base_path / split_name
        if not split_dir.is_dir():
            print(f"Warning: Directory {split_dir} not found")
            continue
        
        eval_log = split_dir / 'eval.log'
        merge_log = split_dir / 'merge_rules.log'
        
        # Extract metrics from both log files
        eval_metrics = extract_eval_metrics(eval_log)
        merge_metrics = extract_merge_metrics(merge_log)
        
        if not eval_metrics and not merge_metrics:
            print(f"Warning: No metrics found for {split_name}")
            continue
        
        # Combine metrics
        row = {
            'split': split_name,
            'total_rules': merge_metrics.get('total_rules', 0),
            'loaded_rules': eval_metrics.get('loaded_rules', 0),
            'MRR': eval_metrics.get('MRR', 0.0),
            'hits@1': eval_metrics.get('hits@1', 0.0),
            'hits@3': eval_metrics.get('hits@3', 0.0),
        }
        data.append(row)
        print(f"Loaded: {split_name} - Total rules: {row['total_rules']}, MRR: {row['MRR']:.4f}")
    
    if not data:
        print("No valid metrics found!")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    
    # Sort by predefined order
    order_dict = {name: idx for idx, name in enumerate(get_split_order())}
    df['order'] = df['split'].map(order_dict)
    df = df.sort_values('order').drop('order', axis=1).reset_index(drop=True)
    
    return df


def plot_heatmap(df: pd.DataFrame, output_path: str):
    """Generate heatmap visualization for all metrics."""
    
    if df.empty:
        print("No data to plot!")
        return
    
    # Define metrics to plot
    metrics = ['total_rules', 'loaded_rules', 'MRR', 'hits@1', 'hits@3']
    metric_labels = ['Total Rules', 'Loaded Rules', 'MRR', 'Hits@1', 'Hits@3']
    
    # Get split names
    splits = df['split'].tolist()
    
    # Create figure with subplots for each metric
    fig, axes = plt.subplots(1, len(metrics), figsize=(8, 4))
    
    # Ensure axes is always a list
    if len(metrics) == 1:
        axes = [axes]
    
    for idx, (metric, label) in enumerate(zip(metrics, metric_labels)):
        ax = axes[idx]
        
        # Prepare data for this metric
        values = df[metric].values.reshape(-1, 1)
        
        # Normalize to [0, 1] for coloring
        vmin, vmax = values.min(), values.max()
        if vmax > vmin:
            normalized = (values - vmin) / (vmax - vmin)
        else:
            normalized = np.ones_like(values) * 0.5
        
        # Determine colormap: Blues for count metrics, YlOrRd for performance metrics
        if metric in ['total_rules', 'loaded_rules']:
            cmap = 'Blues'
        else:
            cmap = 'YlOrRd'
        
        # Create heatmap
        im = ax.imshow(normalized, cmap=cmap, aspect='auto', vmin=0, vmax=1)
        
        # Set title
        ax.set_title(label, fontsize=10, fontweight='bold', pad=10)
        
        # Configure y-axis: only show labels on the first subplot
        if idx == 0:
            ax.set_yticks(range(len(splits)))
            ax.set_yticklabels(splits, fontsize=9)
        else:
            ax.set_yticks([])
        
        # Remove x-axis
        ax.set_xticks([])
        
        # Add value annotations
        for i in range(len(df)):
            value = values[i, 0]
            if metric in ['total_rules', 'loaded_rules']:
                text = f'{int(value):,}'
            else:
                text = f'{value:.4f}'
            
            # Choose text color based on background
            text_color = 'white' if normalized[i, 0] > 0.5 else 'black'
            
            ax.text(0, i, text, ha='center', va='center', 
                   color=text_color, fontsize=9, fontweight='bold')
        
        # Remove spines
        for spine in ax.spines.values():
            spine.set_visible(False)
        
        # Add colorbar at the bottom (without labels)
        cbar = plt.colorbar(im, ax=ax, orientation='horizontal', 
                           pad=0.02, fraction=0.05, aspect=10)
        cbar.set_ticks([])
    
    plt.suptitle('Split Methods Comparison', 
                fontsize=14, fontweight='bold', y=0.98)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nHeatmap saved to: {output_path}")
    plt.close()


def generate_summary_table(df: pd.DataFrame, output_path: str):
    """Generate a text summary table."""
    
    if df.empty:
        return
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("="*100 + "\n")
        f.write("SPLIT METHODS COMPARISON SUMMARY\n")
        f.write("="*100 + "\n\n")
        
        # Format and write table
        f.write(f"{'Split Method':<30} {'Total Rules':>12} {'Loaded Rules':>13} "
                f"{'MRR':>10} {'Hits@1':>10} {'Hits@3':>10}\n")
        f.write("-"*100 + "\n")
        
        for _, row in df.iterrows():
            f.write(f"{row['split']:<30} "
                   f"{int(row['total_rules']):>12,} "
                   f"{int(row['loaded_rules']):>13,} "
                   f"{row['MRR']:>10.6f} "
                   f"{row['hits@1']:>10.6f} "
                   f"{row['hits@3']:>10.6f}\n")
        
        f.write("="*100 + "\n\n")
        
        # Add statistics
        f.write("STATISTICS:\n")
        f.write("-"*100 + "\n")
        f.write(f"Number of split methods analyzed: {len(df)}\n")
        f.write(f"Best MRR: {df['split'].iloc[df['MRR'].idxmax()]} ({df['MRR'].max():.6f})\n")
        f.write(f"Best Hits@1: {df['split'].iloc[df['hits@1'].idxmax()]} ({df['hits@1'].max():.6f})\n")
        f.write(f"Best Hits@3: {df['split'].iloc[df['hits@3'].idxmax()]} ({df['hits@3'].max():.6f})\n")
        f.write(f"Most total rules: {df['split'].iloc[df['total_rules'].idxmax()]} ({int(df['total_rules'].max()):,})\n")
        f.write(f"Most loaded rules: {df['split'].iloc[df['loaded_rules'].idxmax()]} ({int(df['loaded_rules'].max()):,})\n")
    
    print(f"Summary table saved to: {output_path}")


def main():
    # Configuration
    base_dir = r'.\out\FB15k-237\SUPP10'
    split_names = [
        'hub_replication_t50',
        'louvain',
        'bfs_expansion_r2',
        'edge_cut',
        'random_multi_k2',
        'random_multi_k3',
        'random_nonoverlap',
        'relation_centric',
        'vertex_cut'
    ]
    
    output_heatmap = os.path.join(base_dir, 'splits_comparison_heatmap.png')
    output_summary = os.path.join(base_dir, 'splits_comparison_summary.txt')
    
    print("Reading split metrics...")
    df = read_split_metrics(base_dir, split_names)
    
    if df.empty:
        print("No metrics found. Please check log files exist.")
        return
    
    print(f"\nFound metrics for {len(df)} split methods")
    
    print("\nGenerating visualizations...")
    plot_heatmap(df, output_heatmap)
    generate_summary_table(df, output_summary)
    
    print("\n" + "="*60)
    print("Analysis complete!")
    print(f"  Heatmap: {output_heatmap}")
    print(f"  Summary: {output_summary}")
    print("="*60)
    
    # Print preview
    print("\nQuick Preview:")
    print("-"*60)
    print(df.to_string(index=False))


if __name__ == '__main__':
    main()
