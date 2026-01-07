"""
数据结构定义模块
用于三元组分类的基本数据结构

设计说明：
- Atom = (relationId: Long, entityId: Int) 
  - relationId: 可以是单个关系ID或编码的关系路径(Long)
  - entityId: Y(-25)表示二元, X(-24)表示自环, 0表示存在性, >0表示常量实体ID

- RelationPath编码规则（与Kotlin一致）：
  - 每个关系占用15位，最多支持4个关系
  - Bits 0-14: 第一个关系(r1)
  - Bits 15-29: 第二个关系(r2)  
  - Bits 30-44: 第三个关系(r3)
  - Bits 45-59: 第四个关系(r4)
  
- IdManager：管理entity和relation的string<->id映射
  - 变量A-Z使用负数ID: A=-1, B=-2, ..., Z=-26
  - 特殊ID: X=-24, Y=-25, Z=-26, *=0
"""

from dataclasses import dataclass, field
from typing import List, Tuple, Dict, Optional, Set
from collections import defaultdict

# 全局配置：未观测平滑参数
NUM_UNSEEN = 5


# ============ RelationPath 编码工具 ============

class RelationPath:
    """
    关系路径编码/解码工具
    将多个关系ID编码为单个Long值，与Kotlin的RelationPath.kt保持一致
    """
    BITS_PER_RELATION = 15
    MAX_RELATION_ID = (1 << BITS_PER_RELATION) - 1  # 32767
    MAX_L2RELATION_ID = (1 << (BITS_PER_RELATION * 2)) - 1  # 1073741823
    RELATION_MASK = MAX_RELATION_ID
    
    @staticmethod
    def encode(relations: List[int]) -> int:
        """将多个关系ID编码为单个Long值"""
        assert len(relations) <= 4, f"Maximum 4 relations supported, got {len(relations)}"
        assert all(1 <= r <= RelationPath.MAX_RELATION_ID for r in relations), \
            f"All relation IDs must be between 1 and {RelationPath.MAX_RELATION_ID}"
        
        encoded = 0
        for i, rel in enumerate(relations):
            encoded |= (rel << (i * RelationPath.BITS_PER_RELATION))
        return encoded
    
    @staticmethod
    def decode(encoded: int) -> List[int]:
        """将编码的Long值解码为关系ID列表"""
        relations = []
        for i in range(4):
            rel = (encoded >> (i * RelationPath.BITS_PER_RELATION)) & RelationPath.RELATION_MASK
            if rel != 0:
                relations.append(rel)
        return relations
    
    @staticmethod
    def connect_tail(existing_path: int, relation: int) -> int:
        """将关系连接到现有路径的尾部"""
        length = RelationPath.get_length(existing_path)
        assert length < 4, f"Maximum 4 relations supported, existing path already has {length} relations"
        return existing_path | (relation << (length * RelationPath.BITS_PER_RELATION))
    
    @staticmethod
    def connect_head(relation: int, existing_path: int) -> int:
        """将关系连接到现有路径的头部"""
        length = RelationPath.get_length(existing_path)
        assert length < 4, f"Maximum 4 relations supported, existing path already has {length} relations"
        return (existing_path << RelationPath.BITS_PER_RELATION) | relation
    
    @staticmethod
    def get_length(encoded: int) -> int:
        """获取关系路径的长度"""
        count = 0
        for i in range(4):
            rel = (encoded >> (i * RelationPath.BITS_PER_RELATION)) & RelationPath.RELATION_MASK
            if rel != 0:
                count += 1
        return count
    
    @staticmethod
    def get_first_relation(encoded: int) -> int:
        """获取路径中的第一个关系"""
        return encoded & RelationPath.RELATION_MASK
    
    @staticmethod
    def get_last_relation(encoded: int) -> int:
        """获取路径中的最后一个关系"""
        length = RelationPath.get_length(encoded)
        if length == 0:
            return 0
        return (encoded >> ((length - 1) * RelationPath.BITS_PER_RELATION)) & RelationPath.RELATION_MASK
    
    @staticmethod
    def is_single_relation(encoded: int) -> bool:
        """判断是否是单个关系"""
        return encoded <= RelationPath.MAX_RELATION_ID


# ============ IdManager ID管理器 ============

class IdManager:
    """
    管理entity和relation的string<->id映射
    与Kotlin的IdManager.kt保持一致
    """
    
    def __init__(self):
        # Entity管理 (Int IDs, 从1开始)
        self.entity2id: Dict[str, int] = {}
        self.id2entity: Dict[int, str] = {}
        self._next_entity_id = 1
        
        # Relation管理 (Long IDs)
        self.relation2id: Dict[str, int] = {}
        self.id2relation: Dict[int, str] = {}
        self._next_relation_id = 1
        
        # 原始关系数量（不包括逆关系）
        self.original_relation_count = 0
        
        # 初始化变量 A-Z，使用负数ID: A=-1, B=-2, ..., Z=-26
        for i, letter in enumerate('ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
            var_id = -(i + 1)
            self.entity2id[letter] = var_id
            self.id2entity[var_id] = letter
        
        # 特殊实体：* 表示存在性，ID=0
        self.entity2id['*'] = 0
        self.id2entity[0] = '*'
    
    def get_entity_id(self, entity: str) -> int:
        """获取或创建实体ID"""
        if entity not in self.entity2id:
            self.entity2id[entity] = self._next_entity_id
            self.id2entity[self._next_entity_id] = entity
            self._next_entity_id += 1
        return self.entity2id[entity]
    
    def get_relation_id(self, relation: str) -> int:
        """获取或创建关系ID"""
        if relation not in self.relation2id:
            self.relation2id[relation] = self._next_relation_id
            self.id2relation[self._next_relation_id] = relation
            self._next_relation_id += 1
        return self.relation2id[relation]
    
    def get_entity_string(self, entity_id: int) -> str:
        """获取实体ID对应的字符串"""
        if entity_id not in self.id2entity:
            raise ValueError(f"Unknown entity ID: {entity_id}")
        return self.id2entity[entity_id]
    
    def get_relation_string(self, relation_id: int) -> str:
        """获取关系ID对应的字符串（支持关系路径）"""
        if relation_id <= RelationPath.MAX_RELATION_ID:
            if relation_id not in self.id2relation:
                raise ValueError(f"Unknown relation ID: {relation_id}")
            return self.id2relation[relation_id]
        else:
            # 关系路径
            relations = RelationPath.decode(relation_id)
            return '*'.join(self.get_relation_string(r) for r in relations)
    
    def add_inverse_relations(self):
        """为所有现有关系添加逆关系"""
        original_relations = dict(self.relation2id)
        self.original_relation_count = len(original_relations)
        
        for relation_name, relation_id in original_relations.items():
            if not relation_name.startswith('INVERSE_'):
                inverse_name = f'INVERSE_{relation_name}'
                inverse_id = relation_id + self.original_relation_count
                self.relation2id[inverse_name] = inverse_id
                self.id2relation[inverse_id] = inverse_name
        
        # 更新下一个可用的关系ID
        if self.id2relation:
            self._next_relation_id = max(self.id2relation.keys()) + 1
    
    def is_inverse_relation(self, relation_id: int) -> bool:
        """判断是否是逆关系"""
        return relation_id > self.original_relation_count
    
    def get_inverse_relation(self, relation_id: int) -> int:
        """获取逆关系ID"""
        if relation_id <= self.original_relation_count:
            return relation_id + self.original_relation_count
        else:
            return relation_id - self.original_relation_count
    
    def get_inverse_path(self, path_id: int) -> int:
        """获取关系路径的逆路径"""
        if path_id <= RelationPath.MAX_RELATION_ID:
            return self.get_inverse_relation(path_id)
        else:
            relations = RelationPath.decode(path_id)
            inverse_relations = [self.get_inverse_relation(r) for r in reversed(relations)]
            return RelationPath.encode(inverse_relations)
    
    # 特殊ID获取
    def get_x_id(self) -> int:
        return self.entity2id['X']  # -24
    
    def get_y_id(self) -> int:
        return self.entity2id['Y']  # -25
    
    def get_z_id(self) -> int:
        return self.entity2id['Z']  # -26
    
    def is_variable(self, entity_id: int) -> bool:
        """判断是否是变量"""
        return entity_id < 0
    
    def is_xyz(self, entity_id: int) -> bool:
        """判断是否是X/Y/Z变量"""
        return entity_id in (self.get_x_id(), self.get_y_id(), self.get_z_id())
    
    def encode_relation_path(self, relation_strs: List[str]) -> int:
        """将关系字符串列表编码为关系路径ID"""
        relation_ids = [self.get_relation_id(r) for r in relation_strs]
        if len(relation_ids) == 1:
            return relation_ids[0]
        return RelationPath.encode(relation_ids)


# ============ Atom 原子类 ============

@dataclass(frozen=True)
class Atom:
    """
    原子类 - 表示规则中的一个原子
    与Kotlin的MyAtom保持一致
    
    relationId: 关系或关系路径ID (Long)
    entityId: 
        - Y(-25) 表示二元规则 r(X,Y)
        - X(-24) 表示自环规则 r(X,X)
        - 0 表示存在性规则 r(X,*)
        - >0 表示常量实体 r(X,entity)
    """
    relation_id: int  # 关系或关系路径ID
    entity_id: int    # 实体ID
    
    def __hash__(self):
        return hash((self.relation_id, self.entity_id))
    
    def __eq__(self, other):
        if not isinstance(other, Atom):
            return False
        return self.relation_id == other.relation_id and self.entity_id == other.entity_id
    
    @property
    def is_binary(self) -> bool:
        """是否是二元原子 r(X,Y)"""
        return self.entity_id == -25  # Y的ID
    
    @property
    def is_loop(self) -> bool:
        """是否是自环原子 r(X,X)"""
        return self.entity_id == -24  # X的ID
    
    @property
    def is_unary(self) -> bool:
        """是否是一元原子（常量实体）"""
        return self.entity_id > 0
    
    @property
    def is_existence(self) -> bool:
        """是否是存在性原子 r(X,*)"""
        return self.entity_id == 0
    
    @property
    def is_single_relation(self) -> bool:
        """是否是单个关系（L1）"""
        return self.relation_id <= RelationPath.MAX_RELATION_ID
    
    @property
    def first_relation(self) -> int:
        """获取第一个关系ID"""
        if self.is_single_relation:
            return self.relation_id
        return RelationPath.get_first_relation(self.relation_id)
    
    def to_string(self, id_manager: 'IdManager') -> str:
        """转换为可读字符串"""
        rel_str = id_manager.get_relation_string(self.relation_id)
        if self.entity_id == id_manager.get_y_id():
            return f"{rel_str}(X,Y)"
        elif self.entity_id == id_manager.get_x_id():
            return f"{rel_str}(X,X)"
        elif self.entity_id == 0:
            return f"{rel_str}(X,*)"
        else:
            entity_str = id_manager.get_entity_string(self.entity_id)
            return f"{rel_str}(X,{entity_str})"


# ============ Metric 度量类 ============

@dataclass
class Metric:
    """
    度量类 - 存储规则的统计度量
    confidence = support / (body_size + NUM_UNSEEN)
    """
    body_size: int = 0      # 体部支持度 (规则前提出现的次数)
    support: int = 0        # 规则支持度 (同时满足前提和结论的次数)
    
    @property
    def confidence(self) -> float:
        """置信度 = support / (body_size + NUM_UNSEEN)"""
        return self.support / (self.body_size + NUM_UNSEEN)
    
    def __str__(self):
        return f"Metric(conf={self.confidence:.4f}, body={self.body_size}, supp={self.support})"
    
    def __repr__(self):
        return self.__str__()


# ============ NormalRule 普通规则 ============

@dataclass
class NormalRule:
    """
    普通规则 (Normal Rule) - 单分支规则
    H ← B
    
    head: Atom (头部原子)
    body: Atom (体部原子)
    """
    head: Atom      # 头部原子
    body: Atom      # 体部原子
    metric: Metric  # 度量指标
    
    def __hash__(self):
        return hash((self.head, self.body))
    
    def __eq__(self, other):
        if not isinstance(other, NormalRule):
            return False
        return self.head == other.head and self.body == other.body
    
    def to_string(self, id_manager: 'IdManager') -> str:
        """转换为可读字符串"""
        return f"{self.head.to_string(id_manager)} <= {self.body.to_string(id_manager)} [{self.metric}]"


# ============ ComboRule 组合规则 ============

@dataclass
class ComboRule:
    """
    组合规则 (Combo Rule / Multi-branch Rule) - 多分支规则
    H ← B1 ∧ B2 ∧ ... ∧ Bn
    """
    head: Atom              # 头部原子
    branches: Tuple[Atom, ...]  # 分支原子元组（排序后）
    metric: Metric          # 度量指标
    
    def __hash__(self):
        return hash((self.head, self.branches))
    
    def __eq__(self, other):
        if not isinstance(other, ComboRule):
            return False
        return self.head == other.head and self.branches == other.branches
    
    def to_string(self, id_manager: 'IdManager') -> str:
        """转换为可读字符串"""
        branches_str = "; ".join(b.to_string(id_manager) for b in self.branches)
        return f"{self.head.to_string(id_manager)} <= {branches_str} [{self.metric}]"


# ============ 索引结构类型定义 ============

# H2B2Rule: 头部 -> 体部 -> 规则
# Dict[Atom, Dict[Atom, NormalRule]]
# 一级key是head Atom，二级key是body Atom，value是NormalRule
H2B2Rule = Dict[Atom, Dict[Atom, NormalRule]]

# RuleHash2Combo: 规则哈希 -> 组合规则
# Dict[int, ComboRule]
# key是NormalRule的hashCode，value是ComboRule
RuleHash2Combo = Dict[int, ComboRule]
