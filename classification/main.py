#!/usr/bin/env python3
"""
基于规则的三元组分类评估程序

使用方法:
  python main.py --train data/FB15k-237/train.txt --rules rules.txt --test data/FB15k-237
"""

import argparse
import os
import random
import multiprocessing as mp
from typing import List, Tuple, Set, Dict
from concurrent.futures import ProcessPoolExecutor

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


def find_outgoing_paths(
    r2h2t: Dict[int, Dict[int, Set[int]]],
    start: int, max_len: int = 3
) -> Set[int]:
    """
    查找从start出发的所有路径（用于existence规则检查）
    
    Returns:
        编码后的关系路径集合
    """
    results: Set[int] = set()
    
    # L1
    fwd1_nodes: Set[int] = set()
    fwd1_rels: Set[int] = set()
    for rel, h2t in r2h2t.items():
        if start in h2t and h2t[start]:
            results.add(rel)
            fwd1_rels.add(rel)
            fwd1_nodes.update(h2t[start])
    
    if max_len < 2:
        return results
    
    # L2
    fwd2_nodes: Set[int] = set()
    fwd2_paths: Set[int] = set()
    for rel2, h2t in r2h2t.items():
        for mid in fwd1_nodes:
            if mid in h2t and h2t[mid]:
                for r1 in fwd1_rels:
                    path = RelationPath.encode([r1, rel2])
                    results.add(path)
                    fwd2_paths.add(path)
                fwd2_nodes.update(h2t[mid])
    
    if max_len < 3:
        return results
    
    # L3
    for rel3, h2t in r2h2t.items():
        for mid in fwd2_nodes:
            if mid in h2t and h2t[mid]:
                for path2 in fwd2_paths:
                    results.add(RelationPath.connect_tail(path2, rel3))
    
    return results


def _evaluate_chunk(args) -> Dict:
    """
    多进程评估的工作函数
    
    Args:
        args: (triples, threshold, r2h2t, h2b2rule, rule_hash2combo, rel2heads, scoring_mode)
    """
    triples, threshold, r2h2t, h2b2rule, rule_hash2combo, rel2heads, scoring_mode = args
    
    tp = fp = tn = fn = 0
    
    for h, r, t, label in triples:
        matched_confs = []
        heads = rel2heads.get(r, set())
        
        if heads:
            # 预计算路径（按需）
            paths_h2t: Set[int] = None
            paths_h2h: Set[int] = None
            paths_exist: Set[int] = None
            
            for head_atom in heads:
                b2rule = h2b2rule.get(head_atom, {})
                for body_atom, rule in b2rule.items():
                    if head_atom.is_unary and head_atom.entity_id != t:
                        continue
                    if head_atom.is_loop and h != t:
                        continue
                    
                    satisfied = False
                    body_path = body_atom.relation_id
                    
                    if body_atom.is_binary:
                        if paths_h2t is None:
                            paths_h2t = find_relation_paths(r2h2t, h, t)
                        satisfied = body_path in paths_h2t
                    elif body_atom.is_unary:
                        paths_to_const = find_relation_paths(r2h2t, h, body_atom.entity_id)
                        satisfied = body_path in paths_to_const
                    elif body_atom.is_existence:
                        if paths_exist is None:
                            paths_exist = find_outgoing_paths(r2h2t, h)
                        satisfied = body_path in paths_exist
                    elif body_atom.is_loop:
                        if paths_h2h is None:
                            paths_h2h = find_relation_paths(r2h2t, h, h)
                        satisfied = body_path in paths_h2h
                    
                    if satisfied:
                        matched_confs.append(rule.metric.confidence)
                        combo = rule_hash2combo.get(hash(rule))
                        if combo:
                            matched_confs.append(combo.metric.confidence)
        
        # 聚合置信度
        if not matched_confs:
            score = 0.0
        elif scoring_mode == 'maxplus':
            score = max(matched_confs)
        elif scoring_mode == 'noisyor':
            prod = 1.0
            for c in matched_confs:
                prod *= (1.0 - c)
            score = 1.0 - prod
        else:
            score = max(matched_confs)
        
        pred = 1 if score >= threshold else 0
        
        if pred == 1 and label == 1:
            tp += 1
        elif pred == 1 and label == 0:
            fp += 1
        elif pred == 0 and label == 0:
            tn += 1
        else:
            fn += 1
    
    return {'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn}


def evaluate_triples(
    test_triples: List[Tuple[int, int, int, int]],
    kg: KnowledgeGraphLoader,
    h2b2rule: H2B2Rule,
    rule_hash2combo: RuleHash2Combo,
    rel2heads: Dict[int, Set[Atom]],
    threshold: float = 0.5,
    scoring_mode: str = 'maxplus',
    n_workers: int = None
) -> Dict:
    """
    多进程评估三元组
    
    Args:
        test_triples: [(head, rel, tail, label), ...] label=1正例, label=0负例
        kg: 知识图谱加载器
        h2b2rule: 规则映射
        rule_hash2combo: 组合规则映射
        rel2heads: 关系到规则头的索引
        threshold: 分类阈值
        scoring_mode: 'maxplus' 或 'noisyor'
        n_workers: 进程数，默认为CPU核心数
        
    Returns:
        评估指标字典
    """
    if n_workers is None:
        n_workers = mp.cpu_count()
    
    # 分块
    chunk_size = max(1, (len(test_triples) + n_workers - 1) // n_workers)
    chunks = [test_triples[i:i+chunk_size] for i in range(0, len(test_triples), chunk_size)]
    
    # 准备共享数据
    eval_args = [(chunk, threshold, kg.r2h2t, h2b2rule, 
                  rule_hash2combo, rel2heads, scoring_mode)
                 for chunk in chunks]
    
    # 并行评估
    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        results = list(executor.map(_evaluate_chunk, eval_args))
    
    # 合并结果
    tp = fp = tn = fn = 0
    for r in results:
        tp += r['tp']
        fp += r['fp']
        tn += r['tn']
        fn += r['fn']
    
    # 计算指标
    total = tp + fp + tn + fn
    accuracy = (tp + tn) / total if total > 0 else 0.0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    
    return {
        'accuracy': accuracy, 'precision': precision, 'recall': recall,
        'f1': f1, 'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn
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
    parser.add_argument('--train', type=str, required=True, help='Training triples file')
    parser.add_argument('--rules', type=str, required=True, help='Rules file')
    parser.add_argument('--test', type=str, required=True, help='Test data directory (contains test.txt)')
    parser.add_argument('--scoring-mode', type=str, default='maxplus',
                        choices=['maxplus', 'noisyor'], help='Scoring mode: maxplus or noisyor')
    parser.add_argument('--threshold', type=float, default=0.5, help='Classification threshold')
    parser.add_argument('--workers', type=int, default=None, help='Number of worker processes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    # 检查文件
    if not os.path.exists(args.train):
        parser.error(f"File not found: {args.train}")
    if not os.path.exists(args.rules):
        parser.error(f"File not found: {args.rules}")
    test_file = os.path.join(args.test, 'test.txt')
    if not os.path.exists(test_file):
        parser.error(f"Test file not found: {test_file}")

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
        threshold=args.threshold,
        scoring_mode=args.scoring_mode,
        n_workers=args.workers
    )

    print("\n" + "=" * 40)
    print("RESULTS")
    print("=" * 40)
    print(f"  Accuracy:  {results['accuracy']:.4f}")
    print(f"  Precision: {results['precision']:.4f}")
    print(f"  Recall:    {results['recall']:.4f}")
    print(f"  F1:        {results['f1']:.4f}")
    print(f"  TP={results['tp']}, FP={results['fp']}, TN={results['tn']}, FN={results['fn']}")


if __name__ == '__main__':
    main()
