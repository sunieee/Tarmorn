#!/usr/bin/env python3
"""
Analyze and visualize partition strategies comparison.
Reads all metrics.json from out/FB15k-237/* and generates a heatmap table.
"""

import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# 设置字体：Times New Roman用于英文和数字，SimHei用于中文
plt.rcParams['font.serif'] = ['Times New Roman', 'DejaVu Serif', 'serif']
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial', 'DejaVu Sans', 'sans-serif']
plt.rcParams['font.family'] = 'serif'  # 改为serif以使用Times New Roman
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['mathtext.fontset'] = 'custom'
plt.rcParams['mathtext.rm'] = 'Times New Roman'
plt.rcParams['mathtext.it'] = 'Times New Roman:italic'
plt.rcParams['mathtext.bf'] = 'Times New Roman:bold'

def read_metrics(base_dir: str) -> pd.DataFrame:
    """Read all metrics.json files from subdirectories."""
    data = []
    
    base_path = Path(base_dir)
    if not base_path.exists():
        print(f"Error: Directory {base_dir} does not exist")
        return pd.DataFrame()
    
    for strategy_dir in sorted(base_path.iterdir()):
        if not strategy_dir.is_dir():
            continue
        
        metrics_file = strategy_dir / 'metrics.json'
        if not metrics_file.exists():
            print(f"Warning: No metrics.json found in {strategy_dir}")
            continue
        
        try:
            with open(metrics_file, 'r', encoding='utf-8') as f:
                metrics = json.load(f)
            # Extract retention rates
            details = metrics.get('details', {})
            pt_retention = details.get('PT', {}).get('kept', 0) / max(details.get('PT', {}).get('total', 1), 1)
            c2_retention = details.get('CP_len2', {}).get('kept', 0) / max(details.get('CP_len2', {}).get('total', 1), 1)
            c3_retention = details.get('CP_len3', {}).get('kept', 0) / max(details.get('CP_len3', {}).get('total', 1), 1)
            c4_retention = details.get('CP_len4', {}).get('kept', 0) / max(details.get('CP_len4', {}).get('total', 1), 1)
            
            row = {
                'strategy': metrics.get('strategy', strategy_dir.name),
                'partitions': metrics.get('partitions', 0),
                'edges_total': metrics.get('edges_total', 0),
                'nodes_total': metrics.get('nodes_total', 0),
                'replication_factor': metrics.get('replication_factor', 0),
                'retention_overall': metrics.get('retention_overall', 0),
                'retention_PT': pt_retention,
                'retention_C2': c2_retention,
                'retention_C3': c3_retention,
                'retention_C4': c4_retention,
            }
            data.append(row)
            print(f"Loaded: {strategy_dir.name}")
            
        except Exception as e:
            print(f"Error reading {metrics_file}: {e}")
            continue
    
    if not data:
        print("No valid metrics found!")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)
    df = df.sort_values('strategy')
    return df


def calculate_total_unique_nodes(partitions_dir: Path) -> int:
    """Calculate total unique nodes across all partition files."""
    if not partitions_dir.exists():
        return 0
    
    all_nodes = set()
    
    for part_file in sorted(partitions_dir.glob('part_*.tsv')):
        try:
            with open(part_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split('\t')
                    if len(parts) >= 3:
                        h, r, t = parts[0], parts[1], parts[2]
                        all_nodes.add(h)
                        all_nodes.add(t)
        except Exception as e:
            print(f"  Warning: Could not read {part_file}: {e}")
            continue
    
    return len(all_nodes)


def plot_heatmap(df: pd.DataFrame, output_path: str):
    """Generate heatmap table with individual column normalization."""
    
    if df.empty:
        print("No data to plot!")
        return
    
    # Prepare data for plotting
    strategies = df['strategy'].tolist()
    
    # Columns to plot (excluding strategy name)
    metric_cols = [
        'partitions', 'edges_total', 'nodes_total', 'replication_factor',
        'retention_overall', 'retention_PT', 'retention_C2', 'retention_C3', 'retention_C4'
    ]
    
    # Nicer column names for display
    col_labels = [
        'Partitions', 'Edges Total', 'Nodes Total', 'Replication Factor',
        'Overall Retention', 'PT Retention', 'C2 Retention', 'C3 Retention', 'C4 Retention'
    ]
    
    # Create figure with subplots for each column
    n_cols = len(metric_cols)
    fig, axes = plt.subplots(1, n_cols, figsize=(12, len(strategies) * 0.3 + 2), 
                            gridspec_kw={'wspace': 0.05})
    
    if n_cols == 1:
        axes = [axes]
    
    # Plot each column as a separate heatmap
    for idx, (col, label) in enumerate(zip(metric_cols, col_labels)):
        ax = axes[idx]
        
        # Get column data
        values = df[col].values.reshape(-1, 1)
        
        # Normalize to [0, 1] for coloring
        vmin, vmax = values.min(), values.max()
        if vmax > vmin:
            normalized = (values - vmin) / (vmax - vmin)
        else:
            normalized = np.ones_like(values) * 0.5
        
        # Create heatmap
        cmap = 'YlOrRd' if 'retention' in col else 'Blues'
        im = ax.imshow(normalized, cmap=cmap, aspect='auto', vmin=0, vmax=1)
        
        # Add text annotations with actual values
        for i, val in enumerate(values):
            # Format numbers appropriately
            if col == 'replication_factor' or 'retention' in col:
                text = f'{val[0]:.3f}'
            elif col in ['edges_total', 'nodes_total']:
                text = f'{int(val[0]):,}'
            else:
                text = f'{int(val[0])}'
            
            # Choose text color based on background
            text_color = 'white' if normalized[i, 0] > 0.5 else 'black'
            ax.text(0, i, text, ha='center', va='center', 
                   color=text_color, fontsize=9, fontweight='bold')
        
        # Set title
        ax.set_title(label, fontsize=10, fontweight='bold', pad=10)
        
        # Configure axes
        ax.set_xticks([])
        if idx == 0:
            ax.set_yticks(range(len(strategies)))
            ax.set_yticklabels(strategies, fontsize=9)
        else:
            ax.set_yticks([])
        
        # Remove spines
        for spine in ax.spines.values():
            spine.set_visible(False)
        
        # Add colorbar for each column at the bottom (without labels)
        cbar = plt.colorbar(im, ax=ax, orientation='horizontal', 
                           pad=0.02, fraction=0.05, aspect=10)
        cbar.set_ticks([])  # Remove all tick marks and labels
    
    plt.suptitle('Partition Strategies Comparison', 
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
        f.write("="*120 + "\n")
        f.write("PARTITION STRATEGIES COMPARISON SUMMARY\n")
        f.write("="*120 + "\n\n")
        
        # Format and write table
        f.write(f"{'Strategy':<30} {'Parts':>6} {'Edges':>10} {'Nodes':>8} {'Repl':>6} "
                f"{'Overall':>8} {'PT':>7} {'C2':>7} {'C3':>7} {'C4':>7}\n")
        f.write("-"*120 + "\n")
        
        for _, row in df.iterrows():
            f.write(f"{row['strategy']:<30} "
                   f"{int(row['partitions']):>6} "
                   f"{int(row['edges_total']):>10,} "
                   f"{int(row['nodes_total']):>8,} "
                   f"{row['replication_factor']:>6.2f} "
                   f"{row['retention_overall']:>8.4f} "
                   f"{row['retention_PT']:>7.4f} "
                   f"{row['retention_C2']:>7.4f} "
                   f"{row['retention_C3']:>7.4f} "
                   f"{row['retention_C4']:>7.4f}\n")
        
        f.write("="*120 + "\n\n")
        
        # Add statistics
        f.write("STATISTICS:\n")
        f.write("-"*120 + "\n")
        f.write(f"Number of strategies analyzed: {len(df)}\n")
        f.write(f"Best overall retention: {df['strategy'].iloc[df['retention_overall'].idxmax()]} "
                f"({df['retention_overall'].max():.4f})\n")
        f.write(f"Best PT retention: {df['strategy'].iloc[df['retention_PT'].idxmax()]} "
                f"({df['retention_PT'].max():.4f})\n")
        f.write(f"Lowest replication factor: {df['strategy'].iloc[df['replication_factor'].idxmin()]} "
                f"({df['replication_factor'].min():.2f}x)\n")
        f.write(f"Highest replication factor: {df['strategy'].iloc[df['replication_factor'].idxmax()]} "
                f"({df['replication_factor'].max():.2f}x)\n")
    
    print(f"Summary table saved to: {output_path}")


def main():
    dataset = 'wikidata5m'
    # dataset = 'YAGO3-10'
    # dataset = 'FB15k'
    # dataset = 'FB15k-237'
    base_dir = os.path.join('out', dataset)
    output_heatmap = os.path.join('out', dataset, 'comparison_heatmap.png')
    output_summary = os.path.join('out', dataset, 'comparison_summary.txt')
    
    print("Reading partition metrics...")
    df = read_metrics(base_dir)
    
    if df.empty:
        print("No metrics found. Please run partition strategies first.")
        return
    
    print(f"\nFound {len(df)} strategies:")
    for strategy in df['strategy']:
        print(f"  - {strategy}")
    
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
    print(df[['strategy', 'partitions', 'replication_factor', 'retention_overall']].to_string(index=False))


if __name__ == '__main__':
    main()
