# DepAtom EDIS采样实现文档

## 概述

为DepAtom实现了**EDIS (Entity-Distribution-Independent Sampling)** 采样功能，专门用于处理复杂Cyclic规则的评估。

**核心设计特点**：
- ✅ **内部缓存**：非L1 BinaryAtom自动维护`instanceSet`缓存
- ✅ **渐进式采样**：支持多次调用`sampleBinaryInstancesEDIS`逐步累积实例
- ✅ **验证即缓存**：`hasBinaryInstance`验证成功后自动添加到缓存
- ✅ **线程安全**：使用`ConcurrentHashMap.newKeySet()`保证并发安全

## 核心特性

### 1. 内部实例缓存 (`instanceSet`)

**设计思想**：每个非L1 BinaryAtom维护一个内部实例集合

```kotlin
// 内部实现（自动懒加载）
private val _instanceSet: MutableSet<Long>? by lazy {
    if (isBinary && !isL1Atom) {
        java.util.concurrent.ConcurrentHashMap.newKeySet<Long>()
    } else null
}

// 统一访问接口
val instanceSet: Set<Long>
    get() = when {
        isL1Atom && isBinary -> DepLearn.r2instanceSet[relationId] ?: emptySet()
        !isL1Atom && isBinary -> _instanceSet ?: emptySet()
        else -> emptySet()
    }
```

**特点**：
- L1 atom：直接返回DepLearn的全局缓存（只读）
- 非L1 atom：返回自己的实例缓存（可读写）
- 线程安全：使用ConcurrentHashMap支持并发访问

### 2. EDIS采样 (`sampleBinaryInstancesEDIS`)

**新API设计**：不再返回Set，而是返回新增实例数量

```kotlin
fun sampleBinaryInstancesEDIS(
    maxAttempts: Int = 100000,
    maxGroundings: Int = 1000,
    maxRepetitions: Int = 5
): Int  // 返回新增实例数，不是总数
```

**使用方式**：

```kotlin
val atom = DepAtom(relationPath, IdManager.getYId())

// 第一次采样
val newCount1 = atom.sampleBinaryInstancesEDIS()
println("New: $newCount1, Total: ${atom.instanceSet.size}")

// 第二次采样（继续累积）
val newCount2 = atom.sampleBinaryInstancesEDIS()
println("New: $newCount2, Total: ${atom.instanceSet.size}")

// 获取所有缓存的实例
val allInstances = atom.instanceSet
```

**优势**：
- ✅ 支持渐进式采样：多次调用逐步扩充缓存
- ✅ 避免重复采样：已有实例不会被重复统计
- ✅ 内存可控：缓存大小由采样参数控制

**算法流程**：
```kotlin
1. 对于L1原子：
   - 直接使用DepLearn的全局缓存（不需要采样）
   - 返回0（无新增实例）

2. 对于L2+原子：
   a. 初始化内部_instanceSet（如果未初始化）
   
   b. 获取起始实体（均匀分布）
      - 从第一个关系获取所有head实体
      - 随机采样maxAttempts个实体
   
   c. 对每个起始实体进行随机游走
      - 沿着关系路径随机选择下一跳
      - 支持OI约束（避免重复访问实体）
      - 将新发现的(起点, 终点)对添加到_instanceSet
   
   d. 提前终止条件
      - 达到目标grounding数（currentSize + maxGroundings）
      - 达到maxAttempts次尝试
      - 连续maxRepetitions次重复
   
   e. 返回本次新增的实例数量
```

**复杂度**：
- L1原子：O(1) - 不需要采样
- L2+原子：O(maxAttempts × pathLength)

### 3. 验证即缓存 (`hasBinaryInstance`)

**新特性**：验证成功后自动添加到缓存

```kotlin
fun hasBinaryInstance(instance: Long): Boolean {
    // 1. 先检查缓存
    if (_instanceSet?.contains(instance) == true) {
        return true  // 快速路径
    }
    
    // 2. 使用bi-directional DFS验证
    val verified = ... // 双向搜索
    
    // 3. 验证成功，添加到缓存
    if (verified) {
        _instanceSet?.add(instance)
    }
    
    return verified
}
```

**优势**：
- ✅ 减少重复验证：验证过的实例直接命中缓存
- ✅ 逐步完善：每次验证都在完善实例集
- ✅ 透明缓存：调用者无需关心缓存逻辑

### 2. 级联采样 (`cascadeSamplingWith`)

**功能**：用于复杂规则 `r(X,Y) <= R1(X,Y) && R2(X,Y)` 的高效评估

**策略**：
1. **智能选择strict atom**
   - 估算两个atom的grounding大小
   - 选择grounding更少的先采样

2. **采样+验证**
   - 对strict atom进行EDIS采样（结果存入其instanceSet）
   - 获取strictAtom.instanceSet中的所有实例
   - 对每个实例，使用bi-directional DFS验证loose atom
   - 验证成功的实例会自动添加到looseAtom.instanceSet

3. **统计结果**
   - predictedBoth：同时满足R1和R2的数量
   - correctlyPredictedBoth：同时满足R1、R2和head的数量
   - confidence = correctlyPredictedBoth / predictedBoth

**优势**：
- ✅ 避免对两个atom独立采样的交集误差放大
- ✅ 利用bi-directional DFS快速验证
- ✅ 自适应选择最优采样顺序
- ✅ 复杂度可控：O(1000 × L1 + 1000 × 验证L2) << O(V^(L1+L2))

**示例**：
```kotlin
val atom1 = DepAtom(relationPath1, IdManager.getYId())
val atom2 = DepAtom(relationPath2, IdManager.getYId())
val headRel = IdManager.getRelationId("target_relation")

val (predicted, correct, headSize) = atom1.cascadeSamplingWith(
    otherAtom = atom2,
    headRelation = headRel
)

val confidence = correct.toDouble() / predicted

// 采样结果已缓存
println("Atom1 cached: ${atom1.instanceSet.size}")
println("Atom2 cached: ${atom2.instanceSet.size}")
```

### 4. 辅助方法

#### `estimateGroundingSize()`
估算atom的grounding大小，用于选择strict atom

- L1原子：精确统计
- L2+原子：启发式估算 = firstRelSize × avgDegree^(pathLength-1)

#### `beamCyclicPath()`
随机游走完成路径验证

- 从起始实体出发
- 沿着关系路径随机选择下一跳
- 支持OI约束
- 返回终点实体或null

#### `hasBinaryInstance()`
使用bi-directional DFS验证实例是否存在

- L1原子：O(1) 哈希表查询
- L2原子：O(N) 交集计算
- L3原子：O(N²) 双向搜索
- L4+原子：O(N^(L/2)) 双向BFS

## 使用场景

### 场景1：单次采样
```kotlin
// L3 atom: r1*r2*r3(X,Y)
val atom = DepAtom(longRelationPath, IdManager.getYId())
val newCount = atom.sampleBinaryInstancesEDIS()
println("Sampled $newCount new instances")
println("Total cached: ${atom.instanceSet.size}")
```

### 场景2：渐进式采样
```kotlin
val atom = DepAtom(relationPath, IdManager.getYId())

// 第一轮：快速采样少量实例
atom.sampleBinaryInstancesEDIS(maxAttempts = 1000, maxGroundings = 100)

// 第二轮：如果需要更多，继续采样
atom.sampleBinaryInstancesEDIS(maxAttempts = 5000, maxGroundings = 500)

// 第三轮：追加采样
atom.sampleBinaryInstancesEDIS(maxAttempts = 10000, maxGroundings = 1000)

// 所有采样结果都在instanceSet中
println("Total instances: ${atom.instanceSet.size}")
```

### 场景3：复杂规则评估
```kotlin
// r(X,Y) <= R1(X,Y) && R2(X,Y)
val (predicted, correct, headSize) = atom1.cascadeSamplingWith(
    otherAtom = atom2,
    headRelation = headRel
)

// atom1和atom2的instanceSet都已填充
```

### 场景4：验证驱动的缓存
```kotlin
val atom = DepAtom(relationPath, IdManager.getYId())

// 验证一些实例
for (instance in candidateInstances) {
    if (atom.hasBinaryInstance(instance)) {
        // 验证成功，instance已自动添加到atom.instanceSet
    }
}
渐进式采样策略
```kotlin
val atom = DepAtom(relationPath, IdManager.getYId())

// 快速预热
var newCount = atom.sampleBinaryInstancesEDIS(maxAttempts = 1000, maxGroundings = 100)
println("Round 1: +$newCount, total=${atom.instanceSet.size}")

// 如果新增很多，继续采样
if (newCount > 80) {
    newCount = atom.sampleBinaryInstancesEDIS(maxAttempts = 5000, maxGroundings = 500)
    println("Round 2: +$newCount, total=${atom.instanceSet.size}")
}

// 检查饱和度
if (newCount < 50) {
    println("Saturation detected, stopping.")
}
| 方法 | 复杂度 | 准确度 | 适用场景 |
|------|--------|--------|---------|
| **完全枚举** | O(V^L) | 100% | 不可行 |
| **独立采样+交集** | O(2000×L) | 低（交集太小） | ❌ |
| **级联采样** | O(1000×L1 + 1000×验证L2) | 高 | ✅ 推荐 |
| **联合随机游走** | O(N×(L1+L2)) | 高 | 交集不太小 |

## 配置参数

在`Settings.kt`中可以调整以下参数：

```kotlin
BEAM_SAMPLING_MAX_BODY_GROUNDINGS = 1000         // 最多采样数
BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS = 100000  // 最多尝试次数
BEAM_SAMPLING_MAX_REPETITIONS = 5               // 连续重复停止
OI_CONSTRAINTS_ACTIVE = true                    // 启用OI约束
```

## 实验建议

### 1. 调整采样数量
```knewCount = atom.sampleBinaryInstancesEDIS()
val elapsed = System.currentTimeMillis() - startTime
println("Sampled $newCount new instances in ${elapsed}ms")
println("Total cached: ${atom.instanceSet.size}")
```

### 3. 比较采样vs验证
运行`DepAtomSamplingExample`查看示例：
```bash
mvn exec:java -Dexec.mainClass="tarmorn.structure.TLearn.DepAtomSamplingExample"
```

### 2. 监控采样效率
```kotlin
val startTime = System.currentTimeMillis()
val samples = atom.sampleBinaryInstancesEDIS()
val elapsed = System.currentTimeMillis() - startTime
println("Sampled ${samples.size} in ${elapsed}ms")
```

### 3. 比较不同策略
运行`DepAtomSamplingExample`查看不同参数的影响
使用全局缓存，不需要采样
3. **OI约束**：默认启用，避免路径中重复实体
4. **并发安全**：instanceSet使用ConcurrentHashMap，支持多线程
5. **内存控制**：每个atom独立缓存，注意总体内存使用
6. **渐进式采样**：多次调用会累积实例，不会重复
7. **验证即缓存**：hasBinaryInstance会自动缓存验证成功的实例

## 设计优势

### vs 原始设计（返回Set）

| 特性 | 原设计 | 新设计 |
|------|--------|--------|
| **返回值** | Set<Long> | Int (新增数) |
| **多次采样** | 每次返回新Set | 累积到instanceSet |
| **内存使用** | 多份副本 | 单份缓存 |
| **验证缓存** | 需手动管理 | 自动缓存 |
| **线程安全** | 调用方负责 | 内置支持 |
| **API复杂度** | 高（需管理Set） | 低（透明缓存） |

### 关键改进

1. **透明缓存**：调用者无需管理实例集合
2. **渐进式**：支持分批采样，适应不同场景
3. **高效验证**：验证过的实例自动缓存，避免重复计算
4. **线程安全**：使用ConcurrentHashMap天然支持并发
5. **内存优化**：每个atom一份缓存，避免重复存储ryInstances()`
2. **L1优化**：L1 atom直接从缓存获取，无需采样
3. **OI约束**：默认启用，避免路径中重复实体
4. **并发安全**：所有方法都是线程安全的（只读操作）
5. **内存控制**：采样结果存储在内存中，注意maxGroundings不要过大

## 扩展方向

1. **多规则合取**：实现N个规则的高效组合
2. **自适应参数**：根据数据特征动态调整采样参数
3. **增量采样**：支持流式处理大规模数据
4. **缓存机制**：缓存常用atom的采样结果
5. **并行化**：多线程并行采样多个atom

## 参考文献

- EDIS采样：Entity-Distribution-Independent Sampling for knowledge graphs
- Bi-directional DFS：双向搜索优化路径验证
- AnyBURL系统的Beam采样实现
