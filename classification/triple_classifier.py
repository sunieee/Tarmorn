"""
三元组分类器模块
基于规则对三元组进行评分
"""

from typing import List, Tuple, Dict, Set
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

from structures import (
    IdManager, RelationPath, Atom, NormalRule, ComboRule,
    H2B2Rule, RuleHash2Combo, ScoringMode
)
from rule_loader import KnowledgeGraphLoader


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


class TripleClassifier:
    """三元组分类器"""
    
    def __init__(self, id_manager: IdManager, kg: KnowledgeGraphLoader,
                 h2b2rule: H2B2Rule, rule_hash2combo: RuleHash2Combo,
                 scoring_mode: str = ScoringMode.MAX):
        self.id_manager = id_manager
        self.kg = kg
        self.h2b2rule = h2b2rule
        self.rule_hash2combo = rule_hash2combo
        self.scoring_mode = scoring_mode
        
        # 构建关系到规则头的索引
        self._rel2heads: Dict[int, Set[Atom]] = {}
        for head_atom in h2b2rule:
            rel = head_atom.first_relation
            if rel not in self._rel2heads:
                self._rel2heads[rel] = set()
            self._rel2heads[rel].add(head_atom)
    
    def score(self, head: int, relation: int, tail: int) -> float:
        """对单个三元组评分"""
        matched_confs = []
        
        # 获取关系相关的规则头
        heads = self._rel2heads.get(relation, set())
        if not heads:
            return 0.0
        
        # 预计算路径（按需）
        paths_h2t: Set[int] = None  # h -> t
        paths_h2h: Set[int] = None  # h -> h (loop)
        paths_exist: Set[int] = None  # h -> * (existence)
        
        for head_atom in heads:
            b2rule = self.h2b2rule.get(head_atom, {})
            for body_atom, rule in b2rule.items():
                # 检查头部约束
                if head_atom.is_unary and head_atom.entity_id != tail:
                    continue
                if head_atom.is_loop and head != tail:
                    continue
                
                # 检查body是否满足
                satisfied = False
                body_path = body_atom.relation_id
                
                if body_atom.is_binary:
                    # h -> t 路径
                    if paths_h2t is None:
                        paths_h2t = find_relation_paths(self.kg.r2h2t, head, tail)
                    satisfied = body_path in paths_h2t
                    
                elif body_atom.is_unary:
                    # h -> constant 路径
                    target = body_atom.entity_id
                    paths_to_const = find_relation_paths(self.kg.r2h2t, head, target)
                    satisfied = body_path in paths_to_const
                    
                elif body_atom.is_existence:
                    # h -> * 路径
                    if paths_exist is None:
                        paths_exist = find_outgoing_paths(self.kg.r2h2t, head)
                    satisfied = body_path in paths_exist
                    
                elif body_atom.is_loop:
                    # h -> h 路径
                    if paths_h2h is None:
                        paths_h2h = find_relation_paths(self.kg.r2h2t, head, head)
                    satisfied = body_path in paths_h2h
                
                if satisfied:
                    matched_confs.append(rule.metric.confidence)
                    combo = self.rule_hash2combo.get(hash(rule))
                    if combo:
                        matched_confs.append(combo.metric.confidence)
        
        return self._aggregate(matched_confs)
    
    def _aggregate(self, confidences: List[float]) -> float:
        """聚合置信度"""
        if not confidences:
            return 0.0
        
        if self.scoring_mode == ScoringMode.MAX:
            return max(confidences)
        elif self.scoring_mode == ScoringMode.NOISY_OR:
            prod = 1.0
            for c in confidences:
                prod *= (1.0 - c)
            return 1.0 - prod
        elif self.scoring_mode == ScoringMode.SUM:
            return min(1.0, sum(confidences))
        return max(confidences)
    
    def evaluate(self, test_triples: List[Tuple[int, int, int, int]],
                 threshold: float = 0.5, n_workers: int = None) -> Dict:
        """
        多进程评估
        
        Args:
            test_triples: [(head, rel, tail, label), ...]
            threshold: 分类阈值
            n_workers: 进程数，默认为CPU核心数
        """
        if n_workers is None:
            n_workers = mp.cpu_count()
        
        # 单进程直接计算
        if n_workers <= 1 or len(test_triples) < 100:
            return self._evaluate_single(test_triples, threshold)
        
        # 多进程
        chunk_size = (len(test_triples) + n_workers - 1) // n_workers
        chunks = [test_triples[i:i+chunk_size] for i in range(0, len(test_triples), chunk_size)]
        
        # 准备共享数据
        eval_args = [(chunk, threshold, self.kg.r2h2t, self.h2b2rule, 
                      self.rule_hash2combo, self._rel2heads, self.scoring_mode)
                     for chunk in chunks]
        
        with ProcessPoolExecutor(max_workers=n_workers) as executor:
            results = list(executor.map(_evaluate_chunk, eval_args))
        
        # 合并结果
        tp = fp = tn = fn = 0
        for r in results:
            tp += r['tp']
            fp += r['fp']
            tn += r['tn']
            fn += r['fn']
        
        return self._compute_metrics(tp, fp, tn, fn)
    
    def _evaluate_single(self, test_triples: List[Tuple], threshold: float) -> Dict:
        """单进程评估"""
        tp = fp = tn = fn = 0
        
        for h, r, t, label in test_triples:
            score = self.score(h, r, t)
            pred = 1 if score >= threshold else 0
            
            if pred == 1 and label == 1:
                tp += 1
            elif pred == 1 and label == 0:
                fp += 1
            elif pred == 0 and label == 0:
                tn += 1
            else:
                fn += 1
        
        return self._compute_metrics(tp, fp, tn, fn)
    
    @staticmethod
    def _compute_metrics(tp: int, fp: int, tn: int, fn: int) -> Dict:
        """计算评估指标"""
        total = tp + fp + tn + fn
        accuracy = (tp + tn) / total if total > 0 else 0.0
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        
        return {'accuracy': accuracy, 'precision': precision, 'recall': recall,
                'f1': f1, 'tp': tp, 'fp': fp, 'tn': tn, 'fn': fn}


def _evaluate_chunk(args) -> Dict:
    """多进程评估的工作函数"""
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
        
        # 聚合
        if not matched_confs:
            score = 0.0
        elif scoring_mode == ScoringMode.MAX:
            score = max(matched_confs)
        elif scoring_mode == ScoringMode.NOISY_OR:
            prod = 1.0
            for c in matched_confs:
                prod *= (1.0 - c)
            score = 1.0 - prod
        elif scoring_mode == ScoringMode.SUM:
            score = min(1.0, sum(matched_confs))
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
