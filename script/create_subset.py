"""
Create FB15k-237-rp dataset by filtering test and valid data based on relation path richness.

For each triple (h, r, t), calculate:
1. Number of relation paths from h to t within 3 hops
2. Number of attributes for h + number of attributes for t

Sort by harmonic mean of these two metrics and split into new test/valid sets.
"""

from collections import defaultdict
from typing import Set, List, Tuple, Dict
import os
import csv
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
from tqdm import tqdm


def load_triples(file_path: str) -> List[Tuple[str, str, str]]:
    """Load triples from a file."""
    triples = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                parts = line.split('\t')
                if len(parts) == 3:
                    triples.append((parts[0], parts[1], parts[2]))
    return triples


def build_graph(triples: List[Tuple[str, str, str]]) -> Tuple[Dict, Dict]:
    """
    Build forward and backward adjacency lists for the knowledge graph.
    Returns (forward_graph, backward_graph)
    forward_graph[entity] = [(relation, tail_entity), ...]
    """
    forward_graph = defaultdict(list)
    backward_graph = defaultdict(list)
    
    for h, r, t in triples:
        forward_graph[h].append((r, t))
        backward_graph[t].append((r, h))
    
    return forward_graph, backward_graph


def count_relation_paths(h: str, t: str, forward_graph: Dict, backward_graph: Dict, max_hops: int = 3) -> int:
    """
    Count the number of distinct relation paths from h to t within max_hops.
    Uses bidirectional DFS: forward 2-hop DFS from h and backward 1-hop from t.
    A relation path is a sequence of relations, not caring about intermediate nodes.
    """
    if max_hops != 3:
        raise ValueError("This implementation is optimized for max_hops=3")
    
    # Step 1: DFS from head for 1 hop and 2 hops, collect (node, relation_path)
    # forward_1hop: {node: [relation_path]} - nodes reachable in 1 hop from h
    # forward_2hop: {node: [relation_path]} - nodes reachable in 2 hops from h
    forward_1hop = defaultdict(list)  # node -> list of 1-relation paths
    forward_2hop = defaultdict(list)  # node -> list of 2-relation paths
    
    # 1-hop from h
    for r1, n1 in forward_graph.get(h, []):
        forward_1hop[n1].append((r1,))
        
        # 2-hop from h
        for r2, n2 in forward_graph.get(n1, []):
            forward_2hop[n2].append((r1, r2))
    
    # Step 2: Collect relation paths
    relation_paths = set()
    
    # Direct 1-hop: h -> t
    for path in forward_1hop.get(t, []):
        relation_paths.add(path)
    
    # 2-hop: h -> intermediate -> t
    for path in forward_2hop.get(t, []):
        relation_paths.add(path)
    
    # 3-hop: h -> n1 -> n2 -> t
    # Check all nodes that can reach t in 1 hop (backward from t)
    backward_1hop = {}  # node -> list of relations to t
    for r3, n2 in backward_graph.get(t, []):
        if n2 not in backward_1hop:
            backward_1hop[n2] = []
        backward_1hop[n2].append(r3)
    
    # Find intersections: nodes reachable from h in 2 hops that can reach t in 1 hop
    for n2 in forward_2hop:
        if n2 in backward_1hop:
            # There are paths h -> ... -> n2 -> t
            for forward_path in forward_2hop[n2]:
                for r3 in backward_1hop[n2]:
                    full_path = forward_path + (r3,)
                    relation_paths.add(full_path)
    
    return len(relation_paths)


def count_attributes(entity: str, forward_graph: Dict) -> int:
    """
    Count the number of attributes (outgoing edges) for an entity.
    """
    return len(forward_graph.get(entity, []))


def harmonic_mean(a: float, b: float) -> float:
    """Calculate harmonic mean of two numbers."""
    if a + b == 0:
        return 0.0
    return 2 * a * b / (a + b)


def process_triple_batch(args: Tuple) -> List[Tuple[Tuple[str, str, str], float, int, int]]:
    """
    Process a batch of triples and calculate their metrics.
    Args: (triples_batch, forward_graph, backward_graph, worker_id)
    Returns list of (triple, score, path_count, total_attrs).
    """
    triples_batch, forward_graph, backward_graph, worker_id = args
    results = []
    
    # Use tqdm in each worker process
    for h, r, t in tqdm(triples_batch, desc=f"Worker {worker_id}", position=worker_id, leave=False):
        # Count relation paths from h to t using bidirectional DFS
        path_count = count_relation_paths(h, t, forward_graph, backward_graph, max_hops=3)
        
        # Count attributes for h and t
        h_attrs = count_attributes(h, forward_graph)
        t_attrs = count_attributes(t, forward_graph)
        total_attrs = h_attrs + t_attrs
        
        # Calculate harmonic mean
        # Add 1 to avoid zero values
        score = harmonic_mean(path_count + 1, total_attrs + 1)
        
        results.append(((h, r, t), score, path_count, total_attrs))
    
    return results


def process_dataset(data_dir: str):
    """Process the FB15k-237-rp dataset."""
    print(f"Processing dataset in {data_dir}")
    
    # Load training data to build the graph
    print("Loading training data...")
    train_file = os.path.join(data_dir, "train.txt")
    train_triples = load_triples(train_file)
    print(f"Loaded {len(train_triples)} training triples")
    
    # Build graph
    print("Building knowledge graph...")
    forward_graph, backward_graph = build_graph(train_triples)
    print(f"Graph built with {len(forward_graph)} entities")
    
    # Load original test and valid data
    print("Loading test and valid data...")
    test_file = os.path.join(data_dir, "test.txt")
    valid_file = os.path.join(data_dir, "valid.txt")
    
    test_triples = load_triples(test_file)
    valid_triples = load_triples(valid_file)
    
    print(f"Loaded {len(test_triples)} test triples")
    print(f"Loaded {len(valid_triples)} valid triples")
    
    # Combine test and valid
    all_eval_triples = test_triples + valid_triples
    print(f"Total evaluation triples: {len(all_eval_triples)}")
    
    # Calculate metrics for each triple using multiprocessing
    print("Calculating metrics for each triple using multiprocessing...")
    
    # Determine number of processes
    n_workers = cpu_count()
    print(f"Using {n_workers} workers")
    
    # Split triples into chunks
    chunk_size = max(100, len(all_eval_triples) // (n_workers * 2))
    chunks = []
    for i in range(0, len(all_eval_triples), chunk_size):
        chunks.append(all_eval_triples[i:i + chunk_size])
    
    print(f"Split into {len(chunks)} chunks of ~{chunk_size} triples each")
    
    # 准备共享数据，添加worker_id
    eval_args = [(chunk, forward_graph, backward_graph, i)
                 for i, chunk in enumerate(chunks)]
    
    # 并行评估
    print(f"Starting parallel evaluation with {n_workers} workers...")
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        chunk_results = list(executor.map(process_triple_batch, eval_args))
    
    # 合并结果: [(triple, score, path_count, total_attrs), ...]
    triple_scores = []
    for chunk_result in chunk_results:
        triple_scores.extend(chunk_result)
    
    print(f"Completed processing {len(triple_scores)} triples")
    
    print("Sorting triples by score...")
    # Sort by score in descending order
    triple_scores.sort(key=lambda x: x[1], reverse=True)
    
    # Split into two halves
    split_point = len(triple_scores) // 2
    new_test = [item[0] for item in triple_scores[:split_point]]
    new_valid = [item[0] for item in triple_scores[split_point:]]
    
    print(f"New test set: {len(new_test)} triples")
    print(f"New valid set: {len(new_valid)} triples")
    
    # Write CSV with all metrics
    print("Writing metrics to CSV...")
    csv_file = os.path.join(data_dir, "triple_metrics.csv")
    with open(csv_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['head', 'relation', 'tail', 'path_count', 'total_attributes', 'harmonic_mean', 'split'])
        
        for idx, (triple, score, path_count, total_attrs) in enumerate(triple_scores):
            h, r, t = triple
            split = 'test' if idx < split_point else 'valid'
            writer.writerow([h, r, t, path_count, total_attrs, f"{score:.6f}", split])
    
    print(f"Metrics saved to {csv_file}")
    
    # Write new test.txt
    print("Writing new test.txt...")
    with open(test_file, 'w', encoding='utf-8') as f:
        for h, r, t in new_test:
            f.write(f"{h}\t{r}\t{t}\n")
    
    # Write new valid.txt
    print("Writing new valid.txt...")
    with open(valid_file, 'w', encoding='utf-8') as f:
        for h, r, t in new_valid:
            f.write(f"{h}\t{r}\t{t}\n")
    
    print("Done!")
    
    # Print some statistics
    print("\nStatistics:")
    print(f"  Top score: {triple_scores[0][1]:.4f} (paths={triple_scores[0][2]}, attrs={triple_scores[0][3]})")
    print(f"  Median score: {triple_scores[len(triple_scores)//2][1]:.4f}")
    print(f"  Bottom score: {triple_scores[-1][1]:.4f} (paths={triple_scores[-1][2]}, attrs={triple_scores[-1][3]})")


if __name__ == "__main__":
    data_dir = "data/FB15k-237-rp"
    process_dataset(data_dir)
