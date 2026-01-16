# Rule-based Triple Classification

基于规则的三元组分类系统。

## 概述

这个模块实现了基于规则的三元组分类（Triple Classification），用于判断给定的三元组 `<h, r, t>` 是否为正确的事实。

与传统的Link Prediction不同，Triple Classification是一个二分类任务：
- **Link Prediction**: 给定 `<h, r, ?>` 或 `<?, r, t>`，预测缺失的实体
- **Triple Classification**: 给定完整的 `<h, r, t>`，判断该三元组是否为真

## 核心数据结构

### Atom (原子)
```python
Atom(relation_id: int, entity_id: int)
```
- `relation_id`: 关系或关系路径ID（使用15位编码，最多4个关系）
- `entity_id`: 
  - `-25` (Y): 二元规则 `r(X,Y)`
  - `-24` (X): 自环规则 `r(X,X)`
  - `0`: 存在性规则 `r(X,*)`
  - `>0`: 一元规则 `r(X,entity)`

### RelationPath (关系路径编码)
将多个关系编码为单个Long值：
- 每个关系占用15位
- 最多支持4个关系的路径
- 与Kotlin代码 `RelationPath.kt` 保持一致

### H2B2Metric / HB2Combo
- `H2B2Metric`: Head -> Body -> Metric 索引，存储普通规则
- `HB2Combo`: (Head, Branches) -> Combo 索引，存储多分支规则

## 使用方法

### 命令行

```bash
# 评估模式（带标签的测试集）
python main.py --train data/FB15k-237/train.txt \
               --rules rules.txt \
               --test test.txt \
               --with-labels

# 预测模式（不带标签）
python main.py --train data/FB15k-237/train.txt \
               --rules rules.txt \
               --test test.txt \
               --output results.txt

# 交互模式
python main.py --train data/FB15k-237/train.txt \
               --rules rules.txt \
               --interactive
```

### 参数说明

| 参数 | 说明 |
|------|------|
| `--train` | 训练三元组文件 (格式: h TAB r TAB t) |
| `--rules` | 规则文件 |
| `--test` | 测试三元组文件 |
| `--output` | 输出文件路径 |
| `--scoring-mode` | 评分模式: max, noisy_or, sum |
| `--threshold` | 分类阈值 (默认0.5) |
| `--with-labels` | 测试文件包含标签 |
| `--interactive` | 交互模式 |
| `--verbose` | 详细输出 |

### Python API

```python
from structures import IdManager
from rule_loader import load_data_and_rules
from triple_classifier import TripleClassifier

# 加载数据和规则
id_manager, kg_loader, rule_loader = load_data_and_rules(
    'train.txt', 'rules.txt', verbose=True
)

# 创建分类器
classifier = TripleClassifier(
    id_manager=id_manager,
    kg=kg_loader,
    h2b2metric=rule_loader.h2b2metric,
    hb2combo=rule_loader.hb2combo,
    scoring_mode='max'
)

# 分类单个三元组
head_id = id_manager.get_entity_id('/m/012345')
rel_id = id_manager.get_relation_id('/film/film/genre')
tail_id = id_manager.get_entity_id('/m/067890')

result = classifier.classify(head_id, rel_id, tail_id, threshold=0.5)
print(f"Score: {result.score}, Prediction: {result.prediction}")
```

## 规则文件格式

```
# 二元规则
0.85  rel <= rel1*rel2

# 一元规则
0.90  rel(/m/xxx) <= rel1*rel2(/m/yyy)

# 组合规则 (多分支)
0.75  rel <= branch1; branch2; branch3

# 自环规则
0.80  rel(X) <= body_path(*)
```

## 评分模式

1. **MAX**: 取所有匹配规则中置信度最大的
2. **NOISY_OR**: `1 - ∏(1 - conf_i)`，概率模型
3. **SUM**: 置信度求和（截断到1.0）

## 文件结构

```
classification/
├── structures.py       # 核心数据结构
├── rule_loader.py      # 规则加载器
├── triple_classifier.py  # 三元组分类器
├── main.py             # 主程序入口
└── README.md           # 说明文档
```

## 评估指标

评估模式下输出以下指标：
- **Accuracy**: 准确率
- **Precision**: 精确率
- **Recall**: 召回率
- **F1 Score**: F1分数
- **TP/FP/TN/FN**: 混淆矩阵

## 注意事项

1. 规则文件应使用简写格式（与analysis_rule.py兼容）
2. 实体和关系ID在加载训练数据时自动分配
3. 逆关系会自动创建并索引
4. 关系路径最多支持4跳
