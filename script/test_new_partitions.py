#!/usr/bin/env python3
"""
Quick test script for new partitioning algorithms.
Tests hub_replication, bfs_expansion, and relation_centric on FB15k-237.
"""

import subprocess
import sys

strategies = [
    ('hub_replication', ['--hub_threshold', '100']),
    ('bfs_expansion', ['--bfs_radius', '2']),
    ('relation_centric', []),
]

dataset = 'data\\FB15k-237'
out_dir = 'out\\FB15k-237'

print("Testing new partitioning algorithms on FB15k-237...")
print("="*60)

for strategy, extra_args in strategies:
    print(f"\nTesting: {strategy}")
    print("-"*60)
    
    cmd = [
        sys.executable,
        'script\\partition.py',
        '--dataset', dataset,
        '--out', out_dir,
        '--strategy', strategy,
        '--verbose',
        '--max_edges_per_part', '50000',
    ] + extra_args
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False, text=True)
        print(f"✓ {strategy} completed successfully")
    except subprocess.CalledProcessError as e:
        print(f"✗ {strategy} failed with error code {e.returncode}")
        continue

print("\n" + "="*60)
print("All tests completed!")
print("\nResults are in:")
for strategy, _ in strategies:
    print(f"  - out\\FB15k-237\\{strategy}\\metrics.json")
