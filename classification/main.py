#!/usr/bin/env python3
"""
基于规则的三元组分类评估程序

使用方法:
  python main.py --train data/FB15k-237/train.txt --rules rules.txt --test data/FB15k-237
"""

import argparse
import os
import random
from typing import List, Tuple, Set

from structures import IdManager, ScoringMode
from rule_loader import load_data_and_rules, KnowledgeGraphLoader
from triple_classifier import TripleClassifier


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
    parser.add_argument('--scoring-mode', type=str, default='max',
                        choices=['max', 'noisy_or', 'sum'], help='Scoring mode')
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

    # 创建分类器
    print("\n[2/3] Creating classifier...")
    classifier = TripleClassifier(
        id_manager=id_manager,
        kg=kg_loader,
        h2b2rule=rule_loader.h2b2rule,
        rule_hash2combo=rule_loader.rule_hash2combo,
        scoring_mode=args.scoring_mode
    )

    # 加载测试集并评估
    print("\n[3/3] Evaluating...")
    test_triples = load_test_triples(args.test, id_manager, kg_loader)
    pos_count = sum(1 for _, _, _, l in test_triples if l == 1)
    neg_count = len(test_triples) - pos_count
    print(f"  Test triples: {len(test_triples)} (pos={pos_count}, neg={neg_count})")

    results = classifier.evaluate(test_triples, threshold=args.threshold, n_workers=args.workers)

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
