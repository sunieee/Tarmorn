#!/usr/bin/env python3
"""
基于规则的三元组分类评估程序

使用方法:
  python main.py --train data/FB15k-237/train.txt --rules rules.txt --test data/FB15k-237
"""

import argparse
import os
import random
import math
import multiprocessing as mp
from typing import List, Tuple, Set, Dict
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

from structures import IdManager, RelationPath, Atom, H2B2Rule, RuleHash2Combo
from rule_loader import load_data_and_rules, KnowledgeGraphLoader


def find_relation_paths(
    r2h2t: Dict[int, Dict[int, Set[int]]],
    start: int, end: int, max_len: int = 3
) -> Set[int]:
    """
    双向BFS查找从start到end的所有关系路径（编码为RelationPath）
    
    Args:
        r2h2t: relation -> head -> tails 索引（包含逆关系）
        start: 起始实体
        end: 目标实体
        max_len: 最大路径长度（默认3）
        
    Returns:
        编码后的关系路径集合（可直接与body_atom.relation_id比较）
    """
    if start == end:
        return {0}  # 空路径
    
    results: Set[int] = set()
    
    # 正向1跳: start -> ?
    fwd1: Dict[int, Set[int]] = {}  # node -> set of relations to reach it
    for rel, h2t in r2h2t.items():
        if start in h2t:
            for node in h2t[start]:
                if node not in fwd1:
                    fwd1[node] = set()
                fwd1[node].add(rel)
    
    # L1: 检查1跳直达
    if end in fwd1:
        for r in fwd1[end]:
            results.add(r)  # 单关系直接就是ID
    
    if max_len < 2:
        return results
    
    # 反向1跳: ? -> end
    bwd1: Dict[int, Set[int]] = {}  # node -> set of relations from it to end
    for rel, h2t in r2h2t.items():
        for node, tails in h2t.items():
            if end in tails:
                if node not in bwd1:
                    bwd1[node] = set()
                bwd1[node].add(rel)
    
    # L2: 1+1 相遇
    for mid in fwd1:
        if mid in bwd1:
            for r1 in fwd1[mid]:
                for r2 in bwd1[mid]:
                    results.add(RelationPath.encode([r1, r2]))
    
    if max_len < 3:
        return results
    
    # 正向2跳: start -> ? -> ?
    fwd2: Dict[int, Set[int]] = {}  # node -> set of encoded (r1, r2) paths
    for mid, rels1 in fwd1.items():
        for rel2, h2t in r2h2t.items():
            if mid in h2t:
                for node in h2t[mid]:
                    if node not in fwd2:
                        fwd2[node] = set()
                    for r1 in rels1:
                        fwd2[node].add(RelationPath.encode([r1, rel2]))
    
    # L3: 2+1 相遇
    for mid in fwd2:
        if mid in bwd1:
            for path2 in fwd2[mid]:
                for r3 in bwd1[mid]:
                    results.add(RelationPath.connect_tail(path2, r3))
    
    return results


def _evaluate_chunk(args) -> List[Tuple[float, int]]:
    """
    多进程评估的工作函数 - 返回每个三元组的(score, label)
    
    Args:
        args: (triples, r2h2t, h2b2rule, rule_hash2combo, rel2heads, scoring_mode, disable_combo, worker_id)
    
    预测逻辑：
    对于三元组<h,r,t>，找到能预测这个三元组的规则：
    1. 规则head atom为: r(y), r(t), r'(h) 或 r(x) (当h==t时)
    2. 找h到t之间的所有relation paths（3hop以内）
    3. 检查规则是否满足，并记录满足的规则
    4. 对于combo规则，只有当所有分支规则都满足时，combo才满足
    5. 使用surprisal聚合：
       - maxplus: max(所有满足的rule和combo的surprisal)
       - noisyor: greedy-packing方法，按lift从高到低选择不冲突的combo
    """
    from structures import Combo
    
    triples, r2h2t, h2b2rule, rule_hash2combo, rel2heads, scoring_mode, disable_combo, worker_id = args
    
    results = []
    # 为每个worker创建独立的进度条
    pbar = tqdm(triples, desc=f"Worker {worker_id}", position=worker_id, leave=True)
    for h, r, t, label in pbar:
        satisfied_rules = []  # 存储满足的rule对象
        heads = rel2heads.get(r, set())
        
        if heads:
            # 找h到t之间的所有relation paths (3hop以内)
            paths_h2t = find_relation_paths(r2h2t, h, t, max_len=3)
            
            for head_atom in heads:
                # 跳过不匹配的head atom
                if head_atom.is_unary and head_atom.entity_id != t:
                    continue
                # r(x)<-R(c) 只有h==t时才激活
                if head_atom.is_loop and h != t:
                    continue
                
                b2rule = h2b2rule.get(head_atom, {})
                for body_atom, rule in b2rule.items():
                    satisfied = False
                    body_path = body_atom.relation_id
                    
                    # 检查规则是否满足
                    if body_atom.is_binary:
                        # r(y) <- R(y): R在paths_h2t内
                        satisfied = body_path in paths_h2t
                    
                    elif body_atom.is_unary:
                        # r(t) <- R(c): c在r2h2t[R][h]中（单跳）
                        c = body_atom.entity_id
                        if body_path in r2h2t and h in r2h2t[body_path]:
                            satisfied = c in r2h2t[body_path][h]
                    
                    elif body_atom.is_existence:
                        # r(t) <- R(*): r2h2t[R][h]非空（Ud规则）
                        if body_path in r2h2t and h in r2h2t[body_path]:
                            satisfied = len(r2h2t[body_path][h]) > 0
                    
                    elif body_atom.is_loop:
                        # r(x) <- R(c) 或 R(x)：仅当h==t时
                        # 注意：这里body_atom.is_loop表示body是自环，即R(x)
                        # 检查 h 是否在 r2h2t[R][h]中（即h有自环R）
                        if body_path in r2h2t and h in r2h2t[body_path]:
                            satisfied = h in r2h2t[body_path][h]
                    
                    if satisfied:
                        satisfied_rules.append(rule)
        
        # 检查combo规则（只有所有分支都满足时才满足）
        satisfied_combos = []
        if not disable_combo and satisfied_rules:
            # 统计每个combo有多少个分支满足
            combo2counts = {}
            for rule in satisfied_rules:
                rule_hash = hash(rule)
                if rule_hash in rule_hash2combo:
                    combo = rule_hash2combo[rule_hash]  # 直接获取Combo对象
                    if combo not in combo2counts:
                        combo2counts[combo] = 0
                    combo2counts[combo] += 1
            
            # 找出所有分支都满足的combo
            for combo, count in combo2counts.items():
                if count == len(combo.branches):
                    satisfied_combos.append(combo)
        
        # 聚合得分
        if not satisfied_rules and not satisfied_combos:
            score = 0.0
        elif scoring_mode == 'maxplus':
            # maxplus: 选择最大的surprisal（包括rule和combo）
            max_surprisal = max([r.metric.surprisal for r in satisfied_rules], default=0.0)
            if satisfied_combos:
                max_surprisal = max(max_surprisal, 
                                   max([c.metric.surprisal for c in satisfied_combos]))
            score = max_surprisal
            
        elif scoring_mode == 'noisyor':
            # noisyor: greedy-packing方法
            # 1. 按lift从高到低排序combo
            combos_with_lift = []
            for combo in satisfied_combos:
                try:
                    lift = combo.get_lift()
                    combos_with_lift.append((combo, lift))
                except:
                    # 如果无法计算lift，跳过该combo
                    pass
            combos_with_lift.sort(key=lambda x: x[1], reverse=True)
            
            # 2. greedy-packing: 选择不冲突的combo
            used_rule_hashes = set()
            aggregated_surprisal = 0.0
            
            for combo, lift in combos_with_lift:
                # 检查combo的所有branch rule是否已被使用
                branch_hashes = {hash((combo.head, branch)) for branch in combo.branches}
                
                # 如果没有冲突，使用这个combo
                if not branch_hashes & used_rule_hashes:  # 集合交集为空
                    aggregated_surprisal += lift
                    used_rule_hashes.update(branch_hashes)
            
            # 3. 加上未被使用的单个rule的surprisal
            for rule in satisfied_rules:
                rule_hash = hash(rule)
                if rule_hash not in used_rule_hashes:
                    aggregated_surprisal += rule.metric.surprisal
            
            # 4. 转换为概率
            score = 1.0 - math.exp(-aggregated_surprisal)
        else:
            # 默认使用maxplus
            max_surprisal = max([r.metric.surprisal for r in satisfied_rules], default=0.0)
            if satisfied_combos:
                max_surprisal = max(max_surprisal, 
                                   max([c.metric.surprisal for c in satisfied_combos]))
            score = max_surprisal
        
        results.append((score, label))
    
    return results


def evaluate_triples(
    test_triples: List[Tuple[int, int, int, int]],
    kg: KnowledgeGraphLoader,
    h2b2rule: H2B2Rule,
    rule_hash2combo: RuleHash2Combo,
    rel2heads: Dict[int, Set[Atom]],
    scoring_mode: str = 'maxplus',
    disable_combo: bool = False,
    n_workers: int = None
) -> Dict:
    """
    多进程评估三元组，计算AUC和最佳Accuracy
    
    Args:
        test_triples: [(head, rel, tail, label), ...] label=1正例, label=0负例
        kg: 知识图谱加载器
        h2b2rule: 规则映射
        rule_hash2combo: 组合规则映射
        rel2heads: 关系到规则头的索引
        scoring_mode: 'maxplus' 或 'noisyor'
        disable_combo: 是否禁用combo规则
        n_workers: 进程数，默认为CPU核心数
        
    Returns:
        评估指标字典 {'auc': float, 'best_acc': float, 'best_threshold': float}
    """
    if n_workers is None:
        n_workers = mp.cpu_count()
    
    # 分块
    chunk_size = max(1, (len(test_triples) + n_workers - 1) // n_workers)
    chunks = [test_triples[i:i+chunk_size] for i in range(0, len(test_triples), chunk_size)]
    
    # 准备共享数据，添加worker_id
    eval_args = [(chunk, kg.r2h2t, h2b2rule, 
                  rule_hash2combo, rel2heads, scoring_mode, disable_combo, i)
                 for i, chunk in enumerate(chunks)]
    
    # 并行评估
    print(f"  Using {n_workers} workers for parallel evaluation...")
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        chunk_results = list(executor.map(_evaluate_chunk, eval_args))
    
    # 合并结果: [(score, label), ...]
    all_results = []
    for chunk_result in chunk_results:
        all_results.extend(chunk_result)
    
    scores = [s for s, l in all_results]
    labels = [l for s, l in all_results]
    
    # 计算AUC
    try:
        from sklearn.metrics import roc_auc_score
        auc = roc_auc_score(labels, scores)
    except Exception as e:
        print(f"[WARN] Failed to calculate AUC: {e}")
        auc = 0.0
    
    # 计算最佳Accuracy (遍历所有可能的阈值)
    # 使用所有唯一的score作为候选阈值
    unique_scores = sorted(set(scores))
    best_acc = 0.0
    best_threshold = 0.0
    
    for threshold in unique_scores + [0.0, 1.0]:
        correct = sum(1 for score, label in all_results 
                     if (score >= threshold) == (label == 1))
        acc = correct / len(all_results)
        if acc > best_acc:
            best_acc = acc
            best_threshold = threshold
    
    return {
        'auc': auc,
        'best_acc': best_acc,
        'best_threshold': best_threshold,
        'num_samples': len(all_results)
    }


def generate_negatives(
    positives: List[Tuple[int, int, int]],
    kg: KnowledgeGraphLoader,
    id_manager: IdManager
) -> List[Tuple[int, int, int]]:
    """
    生成负例：对每个正例随机替换head或tail，确保不在KG中
    """
    # 获取所有实体ID（排除变量和特殊ID）
    all_entities = [eid for eid in id_manager.id2entity.keys() if eid > 0]
    kg_set = kg.triples
    
    negatives = []
    for h, r, t in positives:
        # 尝试生成一个负例
        for _ in range(100):  # 最多尝试100次
            if random.random() < 0.5:
                # 替换head
                h_new = random.choice(all_entities)
                candidate = (h_new, r, t)
            else:
                # 替换tail
                t_new = random.choice(all_entities)
                candidate = (h, r, t_new)
            
            # 确保不在KG中
            if candidate not in kg_set:
                negatives.append(candidate)
                break
    
    return negatives


def load_test_triples(
    data_dir: str,
    id_manager: IdManager,
    kg: KnowledgeGraphLoader
) -> List[Tuple[int, int, int, int]]:
    """
    加载测试三元组（正例和负例）
    
    Args:
        data_dir: 数据集文件夹路径
        id_manager: ID管理器
        kg: 知识图谱（用于生成负例时过滤）
    
    Returns:
        [(head, rel, tail, label), ...] label=1正例, label=0负例
    """
    pos_file = os.path.join(data_dir, 'test.txt')
    neg_file = os.path.join(data_dir, 'test_negatives.txt')
    
    # 加载正例
    positives = []
    with open(pos_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) >= 3:
                h = id_manager.get_entity_id(parts[0])
                r = id_manager.get_relation_id(parts[1])
                t = id_manager.get_entity_id(parts[2])
                positives.append((h, r, t))
    
    # 加载或生成负例
    if os.path.exists(neg_file):
        negatives = []
        with open(neg_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 3:
                    h = id_manager.get_entity_id(parts[0])
                    r = id_manager.get_relation_id(parts[1])
                    t = id_manager.get_entity_id(parts[2])
                    negatives.append((h, r, t))
    else:
        # 生成负例
        print(f"  Generating negatives (test_negatives.txt not found)...")
        negatives = generate_negatives(positives, kg, id_manager)
        
        # 保存负例
        with open(neg_file, 'w', encoding='utf-8') as f:
            for h, r, t in negatives:
                h_str = id_manager.get_entity_string(h)
                r_str = id_manager.get_relation_string(r)
                t_str = id_manager.get_entity_string(t)
                f.write(f"{h_str}\t{r_str}\t{t_str}\n")
        print(f"  Saved {len(negatives)} negatives to {neg_file}")
    
    # 合并正负例
    triples = [(h, r, t, 1) for h, r, t in positives]
    triples.extend([(h, r, t, 0) for h, r, t in negatives])
    
    return triples


def main():
    parser = argparse.ArgumentParser(description='Rule-based Triple Classification')
    parser.add_argument('--dataset', type=str, default="codex-m", help='Training triples file')
    parser.add_argument('--rules', type=str, default="", help='Rules file')
    parser.add_argument('--scoring-mode', type=str, default='maxplus',
                        choices=['maxplus', 'noisyor'], help='Scoring mode: maxplus or noisyor')
    parser.add_argument('--disable-combo', action='store_true', help='Disable combo rules')
    parser.add_argument('--workers', type=int, default=None, help='Number of worker processes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    args.train = f"data/{args.dataset}/train.txt"
    args.test = f"data/{args.dataset}"

    if args.rules == "":
        args.rules = f"out/{args.dataset}/rule.txt"

    # 检查文件
    if not os.path.exists(args.train):
        parser.error(f"File not found: {args.train}")
    if not os.path.exists(args.rules):
        parser.error(f"File not found: {args.rules}")
    test_file = os.path.join(args.test, 'test.txt')
    if not os.path.exists(test_file):
        parser.error(f"File not found: {test_file}")
    

    # 加载数据
    print("[1/3] Loading data and rules...")
    id_manager, kg_loader, rule_loader = load_data_and_rules(
        args.train, args.rules, verbose=args.verbose
    )
    print(f"  Entities: {len(id_manager.id2entity) - 27}, Relations: {id_manager.original_relation_count}")
    print(f"  Triples: {len(kg_loader.triples)}, Rules: {rule_loader.stats['normal_rules']}")

    # 构建关系到规则头的索引
    print("\n[2/3] Building rule index...")
    rel2heads: Dict[int, Set[Atom]] = {}
    for head_atom in rule_loader.h2b2rule:
        rel = head_atom.first_relation
        if rel not in rel2heads:
            rel2heads[rel] = set()
        rel2heads[rel].add(head_atom)

    # 加载测试集并评估
    print("\n[3/3] Evaluating...")
    test_triples = load_test_triples(args.test, id_manager, kg_loader)
    pos_count = sum(1 for _, _, _, l in test_triples if l == 1)
    neg_count = len(test_triples) - pos_count
    print(f"  Test triples: {len(test_triples)} (pos={pos_count}, neg={neg_count})")

    results = evaluate_triples(
        test_triples=test_triples,
        kg=kg_loader,
        h2b2rule=rule_loader.h2b2rule,
        rule_hash2combo=rule_loader.rule_hash2combo,
        rel2heads=rel2heads,
        scoring_mode=args.scoring_mode,
        disable_combo=args.disable_combo,
        n_workers=args.workers
    )

    print("\n" + "=" * 40)
    print("RESULTS")
    print("=" * 40)
    print(f"  AUC:            {results['auc']:.4f}")
    print(f"  Best Accuracy:  {results['best_acc']:.4f}")
    print(f"  Best Threshold: {results['best_threshold']:.4f}")
    print(f"  Samples:        {results['num_samples']}")


if __name__ == '__main__':
    main()
