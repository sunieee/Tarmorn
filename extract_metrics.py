#!/usr/bin/env python3
"""
Extract evaluation metrics from log files and output to CSV
"""
import re
import csv
import glob
import os
import argparse
from pathlib import Path


def extract_metrics_from_log(log_file):
    """
    Extract MRR, hits@1, and hits@3 from a log file
    
    Args:
        log_file: Path to the log file
        
    Returns:
        dict with 'parameter', 'MRR', 'hits@1', 'hits@3' or None if not found
    """
    # Extract parameter name from filename (remove 'eval-' prefix and '.log' suffix)
    filename = os.path.basename(log_file)
    if filename.startswith('eval-') and filename.endswith('.log'):
        parameter = filename[5:-4]  # Remove 'eval-' and '.log'
    else:
        parameter = filename
    
    # Regular expression to match the metrics line
    pattern = r'MRR\s+([\d.]+),\s*hits@1\s+([\d.]+),\s*hits@3\s+([\d.]+)'
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            content = f.read()
            match = re.search(pattern, content)
            
            if match:
                return {
                    'parameter': parameter,
                    'MRR': float(match.group(1)),
                    'hits@1': float(match.group(2)),
                    'hits@3': float(match.group(3))
                }
    except Exception as e:
        print(f"Error reading {log_file}: {e}")
    
    return None


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Extract evaluation metrics from log files and output to CSV")
    parser.add_argument("--dataset", type=str, default="FB15k-237", 
                        help="Dataset name (default: FB15k-237)")
    args = parser.parse_args()
    
    # Define the directory containing log files
    log_dir = f'out/{args.dataset}'
    
    # Get all log files matching the pattern
    log_pattern = os.path.join(log_dir, 'eval-*.log')
    log_files = glob.glob(log_pattern)
    
    if not log_files:
        print(f"No log files found matching pattern: {log_pattern}")
        return
    
    print(f"Dataset: {args.dataset}")
    print(f"Found {len(log_files)} log files")
    
    # Extract metrics from all log files
    results = []
    for log_file in sorted(log_files):
        metrics = extract_metrics_from_log(log_file)
        if metrics:
            results.append(metrics)
            print(f"Processed: {metrics['parameter']}")
        else:
            print(f"No metrics found in: {os.path.basename(log_file)}")
    
    # Write results to CSV
    if results:
        output_file = os.path.join(log_dir, 'metrics_summary.csv')
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['parameter', 'MRR', 'hits@1', 'hits@3']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(results)
        
        print(f"\nResults written to: {output_file}")
        print(f"Total entries: {len(results)}")
    else:
        print("No metrics extracted from any log files")


if __name__ == '__main__':
    main()
