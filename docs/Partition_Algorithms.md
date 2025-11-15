# Graph Partitioning Algorithms

本文档描述了 `partition.py` 中实现的五种图分割算法。

## 1. Random Non-overlap (随机不重叠分割)

**策略**: `random_nonoverlap`

**算法**:
- 随机打乱所有边
- 按顺序分配到大小为 `max_edges_per_part` 的分区
- 每条边仅属于一个分区（无重叠）

**特点**:
- ✅ 简单快速
- ✅ 分区大小均衡
- ❌ 不考虑图结构，保留率低
- ❌ 路径和环结构易被破坏

**使用场景**: 基线对比

## 2. Random Multi-repeat (随机多次重复)

**策略**: `random_multi`

**算法**:
- 执行 k 次随机不重叠分割
- 每次使用不同的随机种子
- 最终得到 k × n 个分区（n 为单次分区数）

**特点**:
- ✅ 通过重叠提高结构保留率
- ✅ 简单易实现
- ❌ 分区数量增加 k 倍
- ❌ 仍然不考虑图结构

**参数**:
- `--multi_repeats`: 重复次数（默认3）

## 3. k-way Edge-Cut (边切割)

**策略**: `edge_cut`

**算法**:
1. 构建图的邻接关系
2. 使用贪心策略为每个节点分配分区：
   - 优先分配到已有最多邻居的分区（最小化边切割）
   - 考虑负载均衡（避免分区过大）
3. 边根据端点节点的分区分配

**特点**:
- ✅ 最小化跨分区边的数量
- ✅ 保持社区结构完整
- ✅ 分区数量可控（k个分区）
- ⚠️ 可能有分区大小不平衡

**参数**:
- `--k_partitions`: 分区数量（默认7）

**评分函数**:
```
score[i] = neighbor_count[i] - 0.1 × part_sizes[i]
```
- `neighbor_count[i]`: 分区 i 中已有的邻居数
- `part_sizes[i]`: 分区 i 的当前大小

## 4. Vertex-Cut (顶点切割 / PowerGraph 风格)

**策略**: `vertex_cut`

**算法**:
1. 每条边分配到恰好一个分区
2. 顶点可以复制到多个分区
3. 使用贪心策略选择分区：
   - 最小化需要创建的新顶点副本
   - 考虑负载均衡

**特点**:
- ✅ 边不跨分区（完全保留本地结构）
- ✅ 适合高度节点（hub）密集的图
- ✅ 分区大小更均衡
- ⚠️ 顶点会被复制（需要后期合并）

**参数**:
- `--k_partitions`: 分区数量（默认7）

**代价函数**:
```
cost[i] = replica_cost + 0.01 × |edge_counts[i] - avg_load|
```
- `replica_cost`: 需要新建的顶点副本数（0-2）
- 负载均衡惩罚项

**适用场景**:
- 幂律分布图（少数高度节点）
- 需要完整保留边两端关系的应用

## 5. Louvain Community Detection (社区发现)

**策略**: `louvain`

**算法**:
1. 初始化：每个节点为一个社区
2. 迭代优化（最多5轮）：
   - 遍历每个节点
   - 尝试将节点移动到邻居最多的社区
   - 如果改进则接受移动
3. 将每个社区的边分配为一个分区
4. 如果社区过大（超过 `max_edges_per_part`），再次切分

**特点**:
- ✅ 基于模块度优化，保留真实社区结构
- ✅ 高内聚低耦合
- ✅ 路径和环结构保留率高
- ⚠️ 分区数量不可控（取决于社区结构）
- ⚠️ 计算时间较长

**参数**:
- `--max_edges_per_part`: 单个社区的最大边数（超过则切分）

**优化目标**: 最大化模块度
```
Q = Σ [edges_within_community - expected_edges_random]
```

## 6. Hub Replication (高度节点复制)

**策略**: `hub_replication`

**算法**:
1. 计算所有节点的度数
2. 识别高度节点（度数 ≥ threshold）作为hub
3. 将非hub边随机分配到各分区
4. **将所有hub相关边复制到所有分区**

**特点**:
- ✅ 保证hub节点的完整邻域在每个分区都可见
- ✅ 极大提高hub相关路径的保留率
- ✅ 适合幂律分布的知识图谱
- ⚠️ 边复制率高（取决于hub连接的边数）
- ⚠️ 分区间有较多冗余

**参数**:
- `--hub_threshold`: Hub识别阈值（默认100，度数≥此值为hub）

**复制因子**:
```
replication_factor = (regular_edges + hub_edges × n_partitions) / total_edges
```

**适用场景**:
- 存在明显hub节点的图（如社交网络、知识图谱）
- 需要保留hub为中心的星型结构
- 规则学习需要完整的高频实体上下文

## 7. BFS/Radius Expansion (广度优先扩展)

**策略**: `bfs_expansion`

**算法**:
1. 随机选择种子节点
2. 从种子节点进行BFS扩展radius跳
3. 将扩展范围内的所有边加入当前分区
4. 标记已覆盖节点，选择下一个未覆盖节点作为新种子
5. 重复直到覆盖95%节点
6. 将剩余边加入最后一个分区

**特点**:
- ✅ 天然保留局部邻域结构
- ✅ radius-hop路径完整保留在分区内
- ✅ 分区间有自然重叠（边界节点的邻域）
- ⚠️ 分区大小可能不均衡
- ⚠️ 种子选择影响结果

**参数**:
- `--bfs_radius`: BFS扩展半径（默认2）
  - radius=1: 保留1-hop路径（边）
  - radius=2: 保留2-hop路径（三角形）
  - radius=3: 保留3-hop路径（TLearn所需）

**保留保证**:
- 所有长度 ≤ radius 的路径完整保留在至少一个分区内
- 环结构：长度 ≤ 2×radius 的环有机会保留

**适用场景**:
- 需要保留k-hop邻域的应用
- 图具有明显的局部聚类特性
- TLearn规则挖掘（设置radius=3可保留大部分3-hop路径）

## 8. Relation-Centric Partition (关系中心分割)

**策略**: `relation_centric`

**算法**:
1. 按关系类型分组所有边
2. 每个关系组形成一个或多个分区
3. 如果关系组过大，随机切分
4. 合并过小的分区以提高效率

**特点**:
- ✅ 同一关系的所有实例在相同分区
- ✅ 保留关系特定的模式（如对称性、传递性）
- ✅ 便于关系级别的规则学习
- ✅ 减少跨关系干扰
- ⚠️ 分区数量取决于关系数量
- ⚠️ 关系频率差异大时分区不均衡

**参数**:
- `--max_edges_per_part`: 单个关系组的最大边数

**保留保证**:
- 单关系路径（如 r→r→r）完全保留
- 跨关系路径需要关系间有边重叠

**适用场景**:
- 关系类型数量适中（10-100个）
- 需要学习关系特定规则
- 关系间独立性强的知识图谱

## 性能对比 (FB15k-237, 310k edges)

| 策略 | 分区数 | 复制率 | PT保留率 | C2保留率 | C3保留率 | 总体保留率 | 时间 |
|------|-------|-------|---------|---------|---------|-----------|------|
| random_nonoverlap | 7 | 1.0x | ~16% | ~34% | ~3% | ~16% | 快 |
| random_multi (k=3) | 21 | 3.0x | ~40% | ~38% | ~8% | ~38% | 快 |
| edge_cut (k=7) | 7 | 1.0x | ~?% | ~?% | ~?% | ~?% | 中等 |
| vertex_cut (k=7) | 7 | 1.0x | ~?% | ~?% | ~?% | ~?% | 中等 |
| louvain | 可变 | 1.0x | ~?% | ~?% | ~?% | ~?% | 慢 |
| hub_replication (t=100) | ~7 | ~1.5-3x | ~?% | ~?% | ~?% | ~?% | 快 |
| bfs_expansion (r=2) | 可变 | ~1.2-2x | ~?% | ~?% | ~?% | ~?% | 中等 |
| relation_centric | ~237 | 1.0x | ~?% | ~?% | ~?% | ~?% | 快 |

**注**: 需要实际运行后填写测量数据

## 使用示例

### 运行单个策略
```bash
# Edge-cut (7个分区)
python script\partition.py --dataset data\FB15k-237 --strategy edge_cut --k_partitions 7 --verbose

# Vertex-cut (10个分区)
python script\partition.py --dataset data\FB15k-237 --strategy vertex_cut --k_partitions 10 --verbose

# Louvain社区发现
python script\partition.py --dataset data\FB15k-237 --strategy louvain --max_edges_per_part 50000 --verbose

# Hub replication (阈值100)
python script\partition.py --dataset data\FB15k-237 --strategy hub_replication --hub_threshold 100 --verbose

# BFS expansion (半径2)
python script\partition.py --dataset data\FB15k-237 --strategy bfs_expansion --bfs_radius 2 --verbose

# Relation-centric
python script\partition.py --dataset data\FB15k-237 --strategy relation_centric --verbose
```

### 运行所有策略对比
```bash
python script\partition.py --dataset data\FB15k-237 --strategy all --verbose
```

### 只运行快速评估（不评估C4）
```bash
python script\partition.py --dataset data\FB15k-237 --strategy edge_cut --verbose \
    --pt_sample_ratio 0.5 --c3_sample_ratio 0.3
```

## 评估指标说明

### 基本统计
- **edges_original**: 原始图的边数
- **edges_total**: 所有分区的边数总和（含复制）
- **replication_factor**: edges_total / edges_original（复制倍数）
- **edges_per_partition**: 每个分区的边数列表 [e1, e2, ..., en]
- **nodes_per_partition**: 每个分区的节点数列表 [n1, n2, ..., nn]

### 结构保留指标
- **PT (Property Transition)**: 长度为2的路径 (h, r1, y, r2, t)
- **C2 (Cycle Length 2)**: 长度为2的环 (h, r1, t, r2, h)
- **C3 (Cycle Length 3)**: 长度为3的环 (a, r1, b, r2, c, r3, a)
- **C4 (Cycle Length 4)**: 长度为4的环（默认禁用）
- **保留率**: kept / total，越高说明分割后结构越完整

## 推荐配置

### 小数据集 (< 500k edges)
```bash
--strategy all --verbose --pt_sample_ratio 1.0 --c3_sample_ratio 1.0
```

### 中等数据集 (500k - 2M edges)
```bash
--strategy edge_cut --k_partitions 10 --verbose \
    --pt_sample_ratio 0.5 --c3_sample_ratio 0.3
```

### 大数据集 (> 2M edges)
```bash
--strategy vertex_cut --k_partitions 20 --verbose \
    --pt_sample_ratio 0.3 --c3_sample_ratio 0.1
```

### 针对TLearn规则挖掘
```bash
# 高保留率方案（推荐）
--strategy bfs_expansion --bfs_radius 3 --verbose

# 高效率方案
--strategy hub_replication --hub_threshold 50 --verbose

# 关系专注方案
--strategy relation_centric --verbose
```

