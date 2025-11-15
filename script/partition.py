import os
import json
import random
import argparse
import time
from collections import defaultdict
from typing import List, Tuple, Dict, Set

# ----------- Global arguments -----------
args = None

# ----------- IO helpers -----------

def read_triples(dataset_dir: str) -> List[Tuple[str, str, str]]:
    files = [
        os.path.join(dataset_dir, 'train.txt'),
        # os.path.join(dataset_dir, 'valid.txt'),
        # os.path.join(dataset_dir, 'test.txt'),
    ]
    triples: List[Tuple[str, str, str]] = []
    for fp in files:
        if not os.path.exists(fp):
            continue
        with open(fp, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) != 3:
                    # Some datasets may be space-separated
                    parts = line.split()
                    if len(parts) != 3:
                        continue
                h, r, t = parts
                triples.append((h, r, t))
    return triples


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def write_partition(out_dir: str, part_id: int, triples: List[Tuple[str, str, str]]):
    ensure_dir(os.path.join(out_dir, 'partitions'))
    fp = os.path.join(out_dir, 'partitions', f'part_{part_id}.tsv')
    with open(fp, 'w', encoding='utf-8') as f:
        for h, r, t in triples:
            f.write(f"{h}\t{r}\t{t}\n")


def write_metrics(out_dir: str, metrics: Dict):
    ensure_dir(out_dir)
    fp = os.path.join(out_dir, 'metrics.json')
    with open(fp, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)


# ----------- Partition strategies -----------

def partition_random_nonoverlap(triples: List[Tuple[str, str, str]], max_edges_per_part: int, seed: int) -> List[List[Tuple[str, str, str]]]:
    rnd = random.Random(seed)
    indices = list(range(len(triples)))
    rnd.shuffle(indices)
    parts: List[List[Tuple[str, str, str]]] = []
    cur: List[Tuple[str, str, str]] = []
    for idx in indices:
        cur.append(triples[idx])
        if len(cur) >= max_edges_per_part:
            parts.append(cur)
            cur = []
    if cur:
        parts.append(cur)
    return parts


def partition_random_multi(triples: List[Tuple[str, str, str]], max_edges_per_part: int, repeats: int, seed: int) -> List[List[Tuple[str, str, str]]]:
    parts_all: List[List[Tuple[str, str, str]]] = []
    for i in range(repeats):
        parts = partition_random_nonoverlap(triples, max_edges_per_part, seed + i)
        parts_all.extend(parts)
    return parts_all


def partition_edge_cut(triples: List[Tuple[str, str, str]], seed: int) -> List[List[Tuple[str, str, str]]]:
    """
    k-way edge-cut partitioning using greedy balanced assignment.
    Minimizes edges crossing partitions while keeping partition sizes balanced.
    k is determined by max_edges_per_part.
    """
    rnd = random.Random(seed)
    k = max(1, len(triples) // args.max_edges_per_part + (1 if len(triples) % args.max_edges_per_part else 0))
    
    # Build adjacency for graph structure
    adj: Dict[str, Set[str]] = defaultdict(set)
    for h, r, t in triples:
        adj[h].add(t)
        adj[t].add(h)
    
    nodes = list(adj.keys())
    rnd.shuffle(nodes)
    
    # Initialize k partitions with vertex assignments
    node_to_part: Dict[str, int] = {}
    part_sizes = [0] * k
    
    # Greedy assignment: assign each node to partition minimizing edge cut
    for node in nodes:
        # Count neighbors already assigned to each partition
        neighbor_count = [0] * k
        for neighbor in adj.get(node, []):
            if neighbor in node_to_part:
                neighbor_count[node_to_part[neighbor]] += 1
        
        # Choose partition with most neighbors (minimize cut) and smallest size (balance)
        # Score: neighbor_count[i] - 0.1 * part_sizes[i] (prioritize connectivity over balance)
        scores = [neighbor_count[i] - 0.1 * part_sizes[i] for i in range(k)]
        chosen = max(range(k), key=lambda i: scores[i])
        
        node_to_part[node] = chosen
        part_sizes[chosen] += len(adj.get(node, []))
    
    # Assign edges to partitions based on node assignments
    parts: List[List[Tuple[str, str, str]]] = [[] for _ in range(k)]
    for h, r, t in triples:
        # Assign edge to partition of head (or tail if head not found)
        part_id = node_to_part.get(h, node_to_part.get(t, rnd.randint(0, k - 1)))
        parts[part_id].append((h, r, t))
    
    return parts


def partition_vertex_cut(triples: List[Tuple[str, str, str]], seed: int) -> List[List[Tuple[str, str, str]]]:
    """
    Vertex-cut (PowerGraph style) partitioning.
    Each edge is assigned to exactly one partition, but vertices can be replicated.
    Uses greedy heuristic: assign edge to partition with fewest replicas needed.
    k is determined by max_edges_per_part.
    """
    rnd = random.Random(seed)
    k = max(1, len(triples) // args.max_edges_per_part + (1 if len(triples) % args.max_edges_per_part else 0))
    
    # Track which partitions each vertex appears in
    vertex_partitions: Dict[str, Set[int]] = defaultdict(set)
    parts: List[List[Tuple[str, str, str]]] = [[] for _ in range(k)]
    edge_counts = [0] * k
    
    # Shuffle for randomness
    edge_list = list(triples)
    rnd.shuffle(edge_list)
    
    for h, r, t in edge_list:
        # Calculate cost of assigning to each partition
        # Cost = number of new vertex replicas needed + load imbalance penalty
        costs = []
        avg_load = sum(edge_counts) / k
        for i in range(k):
            replica_cost = 0
            if i not in vertex_partitions[h]:
                replica_cost += 1
            if i not in vertex_partitions[t]:
                replica_cost += 1
            # Add load balancing term
            load_penalty = abs(edge_counts[i] - avg_load) * 0.01
            costs.append(replica_cost + load_penalty)
        
        # Assign to partition with minimum cost
        chosen = min(range(k), key=lambda i: costs[i])
        parts[chosen].append((h, r, t))
        edge_counts[chosen] += 1
        vertex_partitions[h].add(chosen)
        vertex_partitions[t].add(chosen)
    
    return parts


def partition_louvain(triples: List[Tuple[str, str, str]], max_edges_per_part: int, seed: int) -> List[List[Tuple[str, str, str]]]:
    """
    Community detection using simplified Louvain-style algorithm.
    Groups nodes into communities based on modularity, then assign edges accordingly.
    """
    rnd = random.Random(seed)
    
    # Build weighted adjacency (edge count between nodes)
    adj: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    node_degree: Dict[str, int] = defaultdict(int)
    total_edges = 0
    
    for h, r, t in triples:
        adj[h][t] += 1
        adj[t][h] += 1
        node_degree[h] += 1
        node_degree[t] += 1
        total_edges += 1
    
    nodes = list(adj.keys())
    
    # Initialize: each node in its own community
    node_to_comm: Dict[str, int] = {node: i for i, node in enumerate(nodes)}
    
    # Simplified Louvain: one pass of greedy modularity optimization
    improved = True
    iterations = 0
    max_iterations = 5
    
    while improved and iterations < max_iterations:
        improved = False
        iterations += 1
        rnd.shuffle(nodes)
        
        for node in nodes:
            current_comm = node_to_comm[node]
            
            # Try moving to neighbor communities
            neighbor_comms: Dict[int, int] = defaultdict(int)
            for neighbor, weight in adj[node].items():
                neighbor_comms[node_to_comm[neighbor]] += weight
            
            if not neighbor_comms:
                continue
            
            # Find best community (most connections)
            best_comm = max(neighbor_comms.items(), key=lambda x: x[1])[0]
            
            if best_comm != current_comm:
                node_to_comm[node] = best_comm
                improved = True
    
    # Group edges by community
    comm_edges: Dict[int, List[Tuple[str, str, str]]] = defaultdict(list)
    for h, r, t in triples:
        # Assign edge to head's community (or tail's if different)
        comm = node_to_comm.get(h, node_to_comm.get(t, 0))
        comm_edges[comm].append((h, r, t))
    
    # Merge small communities and split large ones
    parts: List[List[Tuple[str, str, str]]] = []
    current_batch: List[Tuple[str, str, str]] = []
    
    # Sort communities by size for better packing
    sorted_comms = sorted(comm_edges.items(), key=lambda x: len(x[1]), reverse=True)
    
    for comm_id, edges in sorted_comms:
        if len(edges) > max_edges_per_part:
            # Large community: flush current batch and split this community
            if current_batch:
                parts.append(current_batch)
                current_batch = []
            # Split into multiple partitions
            for i in range(0, len(edges), max_edges_per_part):
                parts.append(edges[i:i + max_edges_per_part])
        elif len(current_batch) + len(edges) <= max_edges_per_part:
            # Small community: add to current batch
            current_batch.extend(edges)
        else:
            # Current batch would overflow: flush and start new batch
            if current_batch:
                parts.append(current_batch)
            current_batch = list(edges)
    
    # Don't forget the last batch
    if current_batch:
        parts.append(current_batch)
    
    return parts


def partition_hub_replication(triples: List[Tuple[str, str, str]], max_edges_per_part: int, hub_threshold: int, seed: int) -> List[List[Tuple[str, str, str]]]:
    """
    Degree-based hub replication strategy with adaptive threshold adjustment.
    Identifies high-degree nodes (hubs) and replicates them across partitions.
    Automatically adjusts hub_threshold to satisfy partition size constraints.
    """
    rnd = random.Random(seed)
    
    # Calculate node degrees
    node_degree: Dict[str, int] = defaultdict(int)
    for h, r, t in triples:
        node_degree[h] += 1
        node_degree[t] += 1
    
    total_edges = len(triples)
    max_total_parts = max(1, int(3 * total_edges / max_edges_per_part))
    
    # Adaptive threshold adjustment loop
    current_threshold = hub_threshold
    max_iterations = 20
    iteration = 0
    
    while iteration < max_iterations:
        iteration += 1
        
        # Identify hubs with current threshold
        hubs = {node for node, deg in node_degree.items() if deg >= current_threshold}
        
        # Separate hub-connected edges from regular edges
        hub_edges: List[Tuple[str, str, str]] = []
        regular_edges: List[Tuple[str, str, str]] = []
        
        for h, r, t in triples:
            if h in hubs or t in hubs:
                hub_edges.append((h, r, t))
            else:
                regular_edges.append((h, r, t))
        
        # Check constraint 1: hub_edges must fit in each partition
        if len(hub_edges) >= max_edges_per_part:
            if args.verbose:
                print(f"[hub_replication] Iteration {iteration}: hub_edges={len(hub_edges)} ≥ max_edges_per_part={max_edges_per_part}, threshold too low", flush=True)
            # Increase threshold by 20%
            current_threshold = int(current_threshold * 1.2) + 1
            continue
        
        # Calculate partition parameters
        regular_edges_per_part = max_edges_per_part - len(hub_edges)
        n_parts = max(1, (len(regular_edges) + regular_edges_per_part - 1) // regular_edges_per_part)
        
        # Check constraint 2: total partitions must not exceed limit
        if n_parts > max_total_parts:
            if args.verbose:
                print(f"[hub_replication] Iteration {iteration}: n_parts={n_parts} > max_total_parts={max_total_parts}, threshold too low", flush=True)
            # Increase threshold by 20%
            current_threshold = int(current_threshold * 1.2) + 1
            continue
        
        # Both constraints satisfied
        if args.verbose:
            print(f"[hub_replication] Threshold converged: {hub_threshold} → {current_threshold}", flush=True)
            print(f"[hub_replication] hubs={len(hubs)}, hub_edges={len(hub_edges)}, regular_edges={len(regular_edges)}", flush=True)
            print(f"[hub_replication] n_parts={n_parts}, regular_per_part={regular_edges_per_part}", flush=True)
            print(f"[hub_replication] Expected total edges: {n_parts * len(hub_edges) + len(regular_edges)}", flush=True)
        
        # Distribute regular edges
        rnd.shuffle(regular_edges)
        parts: List[List[Tuple[str, str, str]]] = []
        
        for i in range(n_parts):
            start = i * regular_edges_per_part
            end = min(start + regular_edges_per_part, len(regular_edges))
            part = regular_edges[start:end]
            # Replicate ALL hub edges to this partition
            part.extend(hub_edges)
            parts.append(part)
        
        return parts
    
    # If we reach here, threshold adjustment failed - fall back to random partitioning
    print(f"[WARNING] Hub replication failed to converge after {max_iterations} iterations", flush=True)
    print(f"[WARNING] Falling back to random partitioning", flush=True)
    all_edges = list(triples)
    rnd.shuffle(all_edges)
    parts: List[List[Tuple[str, str, str]]] = []
    for i in range(0, len(all_edges), max_edges_per_part):
        parts.append(all_edges[i:i + max_edges_per_part])
    return parts


def partition_bfs_expansion(triples: List[Tuple[str, str, str]], max_edges_per_part: int, radius: int, seed: int) -> List[List[Tuple[str, str, str]]]:
    """
    BFS/Radius expansion strategy.
    Starts with seed nodes, expands radius-hop neighborhoods as partitions.
    Overlapping neighborhoods ensure path preservation.
    """
    rnd = random.Random(seed)
    
    # Constraint: total partitions ≤ 3 * (total_edges / max_edges_per_part)
    total_edges = len(triples)
    max_total_parts = max(1, int(3 * total_edges / max_edges_per_part))
    
    # Build adjacency list
    adj: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    for h, r, t in triples:
        adj[h].append((h, r, t))
        adj[t].append((h, r, t))
    
    nodes = list(adj.keys())
    rnd.shuffle(nodes)
    
    parts: List[List[Tuple[str, str, str]]] = []
    covered_nodes: Set[str] = set()
    current_partition: List[Tuple[str, str, str]] = []
    
    for seed_node in nodes:
        if seed_node in covered_nodes:
            continue
        
        # BFS expansion from seed
        partition_edges: Set[Tuple[str, str, str]] = set()
        visited: Set[str] = set()
        queue: List[Tuple[str, int]] = [(seed_node, 0)]
        visited.add(seed_node)
        
        while queue:
            node, dist = queue.pop(0)
            covered_nodes.add(node)
            
            # Add all edges connected to this node
            for edge in adj.get(node, []):
                partition_edges.add(edge)
            
            # Expand if within radius
            if dist < radius:
                for edge in adj.get(node, []):
                    h, r, t = edge
                    for neighbor in [h, t]:
                        if neighbor not in visited:
                            visited.add(neighbor)
                            queue.append((neighbor, dist + 1))
        
        # Try to merge with current partition
        edge_list = list(partition_edges)
        if len(current_partition) + len(edge_list) <= max_edges_per_part:
            # Can merge with current partition
            current_partition.extend(edge_list)
        else:
            # Flush current partition and start new one
            if current_partition:
                parts.append(current_partition)
            
            # Check if this neighborhood is too large by itself
            if len(edge_list) > max_edges_per_part:
                # Split into smaller chunks
                for i in range(0, len(edge_list), max_edges_per_part):
                    parts.append(edge_list[i:i + max_edges_per_part])
                current_partition = []
            else:
                current_partition = edge_list
        
        # Check partition count constraint
        if len(parts) >= max_total_parts:
            if args.verbose:
                print(f"[BFS] Reached partition limit {max_total_parts}, stopping early", flush=True)
            # Merge remaining with last partition
            if current_partition:
                if parts:
                    parts[-1].extend(current_partition)
                else:
                    parts.append(current_partition)
            break
        
        # Stop if we have enough coverage
        if len(covered_nodes) >= len(nodes) * 0.95:
            break
    
    # Flush the last partition
    if current_partition:
        parts.append(current_partition)
    
    # Add remaining uncovered edges
    all_covered_edges = set()
    for part in parts:
        all_covered_edges.update(part)
    
    remaining = [e for e in triples if e not in all_covered_edges]
    if remaining:
        # Try to merge with last partition if possible
        if parts and len(parts[-1]) + len(remaining) <= max_edges_per_part:
            parts[-1].extend(remaining)
        else:
            parts.append(remaining)
    
    return parts


def partition_relation_centric(triples: List[Tuple[str, str, str]], max_edges_per_part: int, seed: int) -> List[List[Tuple[str, str, str]]]:
    """
    Relation-centric partitioning.
    Groups edges by relation type to preserve relation-specific patterns.
    Within each relation group, applies random partitioning if needed.
    """
    rnd = random.Random(seed)
    
    # Group edges by relation
    relation_edges: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    for h, r, t in triples:
        relation_edges[r].append((h, r, t))
    
    # Sort relations by edge count (descending) for better packing
    sorted_relations = sorted(relation_edges.items(), key=lambda x: len(x[1]), reverse=True)
    
    parts: List[List[Tuple[str, str, str]]] = []
    current_batch: List[Tuple[str, str, str]] = []
    
    for relation, edges in sorted_relations:
        if len(edges) > max_edges_per_part:
            # Large relation: flush current batch and split this relation
            if current_batch:
                parts.append(current_batch)
                current_batch = []
            # Split into multiple partitions
            rnd.shuffle(edges)
            for i in range(0, len(edges), max_edges_per_part):
                parts.append(edges[i:i + max_edges_per_part])
        elif len(current_batch) + len(edges) <= max_edges_per_part:
            # Small relation: add to current batch
            current_batch.extend(edges)
        else:
            # Current batch would overflow: flush and start new batch
            if current_batch:
                parts.append(current_batch)
            current_batch = list(edges)
    
    # Don't forget the last batch
    if current_batch:
        parts.append(current_batch)
    
    return parts


# ----------- Index builders -----------

def build_indexes(triples: List[Tuple[str, str, str]]):
    edges_out: Dict[str, List[Tuple[str, str]]] = defaultdict(list)  # h -> [(r, t)]
    edges_in: Dict[str, List[Tuple[str, str]]] = defaultdict(list)   # t -> [(r, h)]
    edge_set: Set[Tuple[str, str, str]] = set()
    for h, r, t in triples:
        edges_out[h].append((r, t))
        edges_in[t].append((r, h))
        edge_set.add((h, r, t))
    return edges_out, edges_in, edge_set


# ----------- Retention computation -----------

def map_edge_to_partitions(parts: List[List[Tuple[str, str, str]]]) -> Dict[Tuple[str, str, str], Set[int]]:
    e2p: Dict[Tuple[str, str, str], Set[int]] = defaultdict(set)
    for pid, part in enumerate(parts):
        for e in part:
            e2p[e].add(pid)
    return e2p


def retention_pt(edges_out: Dict[str, List[Tuple[str, str]]], edges_in: Dict[str, List[Tuple[str, str]]], e2p: Dict[Tuple[str, str, str], Set[int]], sample_ratio: float = 1.0, verbose: bool = True) -> Tuple[int, int]:
    total = 0
    kept = 0
    rnd = random.Random(42)
    processed = 0
    total_nodes = len(edges_in)
    start_time = time.time()
    if verbose:
        print(f"[PT] Starting enumeration over {total_nodes} middle nodes...", flush=True)
    
    for y, in_list in edges_in.items():
        # optional sampling on middle nodes
        if sample_ratio < 1.0 and rnd.random() > sample_ratio:
            continue
        processed += 1
        if verbose and processed % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"[PT] processed={processed}/{total_nodes} instances_total={total} instances_kept={kept} elapsed={elapsed:.1f}s", flush=True)
        
        out_list = edges_out.get(y, [])
        if not in_list or not out_list:
            continue
        for (r1, h) in in_list:
            e1 = (h, r1, y)
            parts_e1 = e2p.get(e1, set())
            if not parts_e1:
                # Edge not present in partitions at all (shouldn't happen if partitions cover all edges)
                continue
            for (r2, z) in out_list:
                e2 = (y, r2, z)
                total += 1
                parts_e2 = e2p.get(e2, set())
                if parts_e1 & parts_e2:
                    kept += 1
    
    if verbose:
        elapsed = time.time() - start_time
        print(f"[PT] Completed. kept={kept} total={total} retention={(kept/total if total else 0):.4f} elapsed={elapsed:.1f}s", flush=True)
    return kept, total


def retention_cycle_len2(edge_set: Set[Tuple[str, str, str]], e2p: Dict[Tuple[str, str, str], Set[int]], verbose: bool = True) -> Tuple[int, int]:
    # count pairs (h,r,t) and (t, r2, h)
    total = 0
    kept = 0
    start_time = time.time()
    if verbose:
        print(f"[C2] Starting enumeration over {len(edge_set)} edges...", flush=True)
    
    # Build quick index for reverse lookup by pair (t,h)
    by_th: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = defaultdict(list)
    for h, r, t in edge_set:
        by_th[(t, h)].append((t, r, h))
    
    processed = 0
    for h, r, t in edge_set:
        processed += 1
        if verbose and processed % 10000 == 0:
            elapsed = time.time() - start_time
            print(f"[C2] processed={processed}/{len(edge_set)} cycles_total={total} cycles_kept={kept} elapsed={elapsed:.1f}s", flush=True)
        
        e1 = (h, r, t)
        parts_e1 = e2p.get(e1, set())
        revs = by_th.get((h, t), [])
        for e2 in revs:
            total += 1
            parts_e2 = e2p.get(e2, set())
            if parts_e1 & parts_e2:
                kept += 1
    
    if verbose:
        elapsed = time.time() - start_time
        print(f"[C2] Completed. kept={kept} total={total} retention={(kept/total if total else 0):.4f} elapsed={elapsed:.1f}s", flush=True)
    return kept, total


def retention_cycle_len3(edges_out: Dict[str, List[Tuple[str, str]]], e2p: Dict[Tuple[str, str, str], Set[int]], sample_ratio: float = 1.0, degree_cap: int = 1000, verbose: bool = True) -> Tuple[int, int]:
    # enumerate a->b->c->a
    total = 0
    kept = 0
    rnd = random.Random(43)
    processed = 0
    total_nodes = len(edges_out)
    start_time = time.time()
    if verbose:
        print(f"[C3] Starting enumeration over {total_nodes} anchor nodes...", flush=True)
    
    for a, out_a in edges_out.items():
        if sample_ratio < 1.0 and rnd.random() > sample_ratio:
            continue
        processed += 1
        if verbose and processed % 5000 == 0:
            elapsed = time.time() - start_time
            print(f"[C3] processed={processed}/{total_nodes} cycles_total={total} cycles_kept={kept} elapsed={elapsed:.1f}s", flush=True)
        
        # a->b
        for (r1, b) in out_a:
            out_b = edges_out.get(b, [])
            if len(out_b) > degree_cap:
                # avoid explosion on hubs
                continue
            e1 = (a, r1, b)
            parts_e1 = e2p.get(e1, set())
            if not parts_e1:
                continue
            # b->c
            for (r2, c) in out_b:
                e2 = (b, r2, c)
                parts_e2 = e2p.get(e2, set())
                out_c = edges_out.get(c, [])
                if len(out_c) > degree_cap:
                    continue
                # c->a
                for (r3, a2) in out_c:
                    if a2 != a:
                        continue
                    total += 1
                    e3 = (c, r3, a)
                    parts_e3 = e2p.get(e3, set())
                    if parts_e1 & parts_e2 & parts_e3:
                        kept += 1
    
    if verbose:
        elapsed = time.time() - start_time
        print(f"[C3] Completed. kept={kept} total={total} retention={(kept/total if total else 0):.4f} elapsed={elapsed:.1f}s", flush=True)
    return kept, total


def retention_cycle_len4(
    edges_out: Dict[str, List[Tuple[str, str]]],
    e2p: Dict[Tuple[str, str, str], Set[int]],
    sample_ratio: float = 0.5,
    degree_cap: int = 200,
    progress_interval_nodes: int = 2000,
    max_cycles: int = 200000,
    verbose: bool = True
) -> Tuple[int, int]:
    """Enumerate 4-cycles a->b->c->d->a with sampling & degree caps.

    Adds progress output and early stopping to avoid long hangs on large graphs.
    Returns (kept, total) possibly truncated if max_cycles reached.
    """
    total = 0
    kept = 0
    rnd = random.Random(44)
    processed_anchor_nodes = 0
    start_time = time.time()
    last_report = start_time

    for a, out_a in edges_out.items():
        if sample_ratio < 1.0 and rnd.random() > sample_ratio:
            continue
        processed_anchor_nodes += 1
        # Progress report by anchor nodes processed
        if verbose and processed_anchor_nodes % progress_interval_nodes == 0:
            now = time.time()
            elapsed = now - start_time
            print(f"[C4] anchor_nodes={processed_anchor_nodes} cycles_total={total} cycles_kept={kept} elapsed={elapsed:.1f}s retention_so_far={(kept/total if total else 0):.4f}", flush=True)
            last_report = now

        for (r1, b) in out_a:
            out_b = edges_out.get(b, [])
            if len(out_b) > degree_cap:
                continue
            e1 = (a, r1, b)
            parts_e1 = e2p.get(e1, set())
            if not parts_e1:
                continue
            for (r2, c) in out_b:
                e2 = (b, r2, c)
                parts_e2 = e2p.get(e2, set())
                out_c = edges_out.get(c, [])
                if len(out_c) > degree_cap:
                    continue
                for (r3, d) in out_c:
                    e3 = (c, r3, d)
                    parts_e3 = e2p.get(e3, set())
                    out_d = edges_out.get(d, [])
                    if len(out_d) > degree_cap:
                        continue
                    for (r4, a2) in out_d:
                        if a2 != a:
                            continue
                        total += 1
                        e4 = (d, r4, a)
                        parts_e4 = e2p.get(e4, set())
                        if parts_e1 & parts_e2 & parts_e3 & parts_e4:
                            kept += 1
                        if total >= max_cycles:
                            if verbose:
                                print(f"[C4] Early stop: reached max_cycles={max_cycles}. kept={kept} total={total} retention={(kept/total if total else 0):.4f}", flush=True)
                            return kept, total
    if verbose:
        elapsed = time.time() - start_time
        print(f"[C4] Completed enumeration. kept={kept} total={total} retention={(kept/total if total else 0):.4f} elapsed={elapsed:.1f}s", flush=True)
    return kept, total


# ----------- End-to-end runner -----------

def run_strategy(name: str, triples: List[Tuple[str, str, str]], parts: List[List[Tuple[str, str, str]]]):
    out_dir = os.path.join(args.out, name)
    ensure_dir(out_dir)
    
    print(f"\n{'='*60}")
    print(f"Strategy: {name}")
    print(f"Partitions: {len(parts)}")
    print(f"Total edges: {len(triples)}")
    print(f"{'='*60}\n", flush=True)
    
    # write partitions
    if args.verbose:
        print(f"[{name}] Writing {len(parts)} partition files...", flush=True)
    for pid, part in enumerate(parts):
        write_partition(out_dir, pid, part)
    if args.verbose:
        print(f"[{name}] Partition files written. Building indexes...", flush=True)
    
    # build indexes and retention
    edges_out, edges_in, edge_set = build_indexes(triples)
    e2p = map_edge_to_partitions(parts)
    if args.verbose:
        print(f"[{name}] Indexes built. Starting retention evaluation...\n", flush=True)

    t0 = time.time()
    kept_pt, total_pt = retention_pt(edges_out, edges_in, e2p, sample_ratio=args.pt_sample_ratio, verbose=args.verbose)
    t1 = time.time()
    
    kept_c2, total_c2 = retention_cycle_len2(edge_set, e2p, verbose=args.verbose)
    t2 = time.time()
    
    kept_c3, total_c3 = retention_cycle_len3(edges_out, e2p, sample_ratio=args.c3_sample_ratio, degree_cap=args.c3_degree_cap, verbose=args.verbose)
    t3 = time.time()
    
    kept_c4, total_c4 = retention_cycle_len4(edges_out, e2p, sample_ratio=args.c4_sample_ratio, degree_cap=args.c4_degree_cap, 
                                              progress_interval_nodes=args.c4_progress_interval, max_cycles=args.c4_max_cycles, verbose=args.verbose)
    t4 = time.time()

    total_structs = total_pt + total_c2 + total_c3 + total_c4
    kept_structs = kept_pt + kept_c2 + kept_c3 + kept_c4
    retention = (kept_structs / total_structs) if total_structs else 1.0

    # Calculate partition statistics
    partition_stats = []
    total_edges_in_parts = 0
    total_nodes_in_parts = 0  # Sum of nodes across all partitions (with replication)
    unique_nodes_in_parts = set()  # Unique nodes (without replication)
    
    for pid, part in enumerate(parts):
        nodes_in_part = set()
        for h, r, t in part:
            nodes_in_part.add(h)
            nodes_in_part.add(t)
        partition_stats.append({
            'partition_id': pid,
            'num_edges': len(part),
            'num_nodes': len(nodes_in_part)
        })
        total_edges_in_parts += len(part)
        total_nodes_in_parts += len(nodes_in_part)  # Add with replication
        unique_nodes_in_parts.update(nodes_in_part)  # Track unique nodes
    
    # Extract edges and nodes lists
    partition_edges = [p['num_edges'] for p in partition_stats]
    partition_nodes = [p['num_nodes'] for p in partition_stats]

    metrics = {
        'strategy': name,
        'partitions': len(parts),
        'edges_original': len(triples),
        'edges_total': total_edges_in_parts,
        'edges_per_partition': partition_edges,
        'nodes_total': total_nodes_in_parts,  # Sum across all partitions (with replication)
        'nodes_unique': len(unique_nodes_in_parts),  # Unique nodes (without replication)
        'nodes_per_partition': partition_nodes,
        'replication_factor': round(total_edges_in_parts / len(triples), 3) if len(triples) > 0 else 0,
        'retention_overall': retention,
        'details': {
            'PT': {'kept': kept_pt, 'total': total_pt, 'time_sec': round(t1 - t0, 3), 'sample_ratio': args.pt_sample_ratio},
            'CP_len2': {'kept': kept_c2, 'total': total_c2, 'time_sec': round(t2 - t1, 3)},
            'CP_len3': {'kept': kept_c3, 'total': total_c3, 'time_sec': round(t3 - t2, 3), 'sample_ratio': args.c3_sample_ratio, 'degree_cap': args.c3_degree_cap},
            'CP_len4': {'kept': kept_c4, 'total': total_c4, 'time_sec': round(t4 - t3, 3), 'sample_ratio': args.c4_sample_ratio, 'degree_cap': args.c4_degree_cap},
        }
    }
    write_metrics(out_dir, metrics)
    print(f"\n{'='*60}")
    print(f"[{name}] SUMMARY:")
    print(f"  Partitions: {len(parts)}")
    print(f"  Original Edges: {len(triples)}")
    print(f"  Total Edges (with replication): {total_edges_in_parts}")
    print(f"  Replication Factor: {total_edges_in_parts / len(triples):.2f}x")
    print(f"  Edges per partition: min={min(partition_edges)}, max={max(partition_edges)}, avg={sum(partition_edges)/len(partition_edges):.1f}")
    print(f"  Nodes (total/unique): {total_nodes_in_parts}/{len(unique_nodes_in_parts)}")
    print(f"  Nodes per partition: min={min(partition_nodes)}, max={max(partition_nodes)}, avg={sum(partition_nodes)/len(partition_nodes):.1f}")
    print(f"  Overall Retention: {retention:.4f}")
    print(f"  PT:      {kept_pt}/{total_pt} = {(kept_pt/total_pt if total_pt else 0):.4f}")
    print(f"  C2:      {kept_c2}/{total_c2} = {(kept_c2/total_c2 if total_c2 else 0):.4f}")
    print(f"  C3:      {kept_c3}/{total_c3} = {(kept_c3/total_c3 if total_c3 else 0):.4f}")
    print(f"  C4:      {kept_c4}/{total_c4} = {(kept_c4/total_c4 if total_c4 else 0):.4f}")
    print(f"{'='*60}\n", flush=True)


def main():
    global args
    parser = argparse.ArgumentParser(description='Partition FB15k-237 and evaluate retention')
    parser.add_argument('--dataset', default='FB15k-237')
    parser.add_argument('--max_edges_per_part', type=int, default=100000)
    parser.add_argument('--seed', type=int, default=2025)
    # parser.add_argument('--multi_repeats', type=int, default=3)
    parser.add_argument('--pt_sample_ratio', type=float, default=0.5, help='Sampling ratio for PT enumeration')
    parser.add_argument('--c3_sample_ratio', type=float, default=0.1, help='Sampling ratio for C3 enumeration')
    parser.add_argument('--c3_degree_cap', type=int, default=100, help='Degree cap for C3 enumeration')
    parser.add_argument('--c4_sample_ratio', type=float, default=0.05)
    parser.add_argument('--c4_degree_cap', type=int, default=100)
    parser.add_argument('--c4_progress_interval', type=int, default=2000)
    parser.add_argument('--c4_max_cycles', type=int, default=100000)
    parser.add_argument('--strategy', type=str, default='all', 
                        choices=['all', 'random_nonoverlap', 'random_multi', 'edge_cut', 'vertex_cut', 'louvain',
                                'hub_replication', 'bfs_expansion', 'relation_centric'],
                        help='Partitioning strategy to use')
    parser.add_argument('--hub_threshold', type=int, default=50, help='Degree threshold for hub identification')
    parser.add_argument('--bfs_radius', type=int, default=2, help='BFS expansion radius')
    parser.add_argument('--verbose', action='store_true', help='Verbose progress output')
    args = parser.parse_args()

    args.data = os.path.join('data', args.dataset)
    args.out = os.path.join('out', args.dataset)

    triples = read_triples(args.data)
    partition_count = (len(triples) + args.max_edges_per_part - 1) // args.max_edges_per_part
    args.pt_sample_ratio = min(args.pt_sample_ratio, 1.0 / partition_count)
    if not triples:
        raise RuntimeError(f"No triples found under {args.data}")

    print(f"\n{'='*60}")
    print(f"Dataset: {args.data}")
    print(f"Total triples: {len(triples)}")
    print(f"Max edges per partition: {args.max_edges_per_part}")
    print(f"Strategy: {args.strategy}")
    print(f"Seed: {args.seed}")
    print(f"Verbose: {args.verbose}")
    print(f"{'='*60}\n", flush=True)

    # Strategy 1: non-overlap random split
    if args.strategy in ['all', 'random_nonoverlap']:
        print(f"\n{'*'*60}\nStarting Strategy: random_nonoverlap\n{'*'*60}", flush=True)
        run_strategy('random_nonoverlap', triples, partition_random_nonoverlap(triples, args.max_edges_per_part, args.seed))

    # Strategy 2: multi random split (k repeats)
    if args.strategy in ['all', 'random_multi']:
        print(f"\n{'*'*60}\nStarting Strategy: random_multi (k=2)\n{'*'*60}", flush=True)
        run_strategy(f'random_multi_k2', triples, partition_random_multi(triples, args.max_edges_per_part, 2, args.seed))

        print(f"\n{'*'*60}\nStarting Strategy: random_multi (k=3)\n{'*'*60}", flush=True)
        run_strategy(f'random_multi_k3', triples, partition_random_multi(triples, args.max_edges_per_part, 3, args.seed))

    # Strategy 3: k-way edge-cut
    if args.strategy in ['all', 'edge_cut']:
        print(f"\n{'*'*60}\nStarting Strategy: edge_cut\n{'*'*60}", flush=True)
        run_strategy('edge_cut', triples, partition_edge_cut(triples, args.seed))

    # Strategy 4: vertex-cut (PowerGraph)
    if args.strategy in ['all', 'vertex_cut']:
        print(f"\n{'*'*60}\nStarting Strategy: vertex_cut\n{'*'*60}", flush=True)
        run_strategy('vertex_cut', triples, partition_vertex_cut(triples, args.seed))

    # Strategy 5: community detection (Louvain)
    if args.strategy in ['all', 'louvain']:
        print(f"\n{'*'*60}\nStarting Strategy: louvain (community detection)\n{'*'*60}", flush=True)
        run_strategy('louvain', triples, partition_louvain(triples, args.max_edges_per_part, args.seed))

    # Strategy 6: hub replication
    if args.strategy in ['all', 'hub_replication']:
        print(f"\n{'*'*60}\nStarting Strategy: hub_replication (threshold={args.hub_threshold})\n{'*'*60}", flush=True)
        run_strategy(f'hub_replication_t{args.hub_threshold}', triples, partition_hub_replication(triples, args.max_edges_per_part, args.hub_threshold, args.seed))

    # Strategy 7: BFS expansion
    if args.strategy in ['all', 'bfs_expansion']:
        print(f"\n{'*'*60}\nStarting Strategy: bfs_expansion (radius={args.bfs_radius})\n{'*'*60}", flush=True)
        run_strategy(f'bfs_expansion_r{args.bfs_radius}', triples, partition_bfs_expansion(triples, args.max_edges_per_part, args.bfs_radius, args.seed))

    # Strategy 8: relation-centric
    if args.strategy in ['all', 'relation_centric']:
        print(f"\n{'*'*60}\nStarting Strategy: relation_centric\n{'*'*60}", flush=True)
        run_strategy('relation_centric', triples, partition_relation_centric(triples, args.max_edges_per_part, args.seed))


if __name__ == '__main__':
    main()
