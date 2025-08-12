# MinHash与LSH算法实现文档

## 概述

本文档描述了Tarmorn项目中实现的MinHash（最小哈希）算法和LSH（局部敏感哈希）系统，用于高效的原子化公式相似性检测和分桶操作。

## 算法背景

### MinHash算法
MinHash是一种用于快速估计集合Jaccard相似度的概率算法。对于两个集合A和B：
- **Jaccard相似度**: J(A,B) = |A ∩ B| / |A ∪ B|
- **MinHash估计**: 通过k个独立哈希函数的最小值来估计相似度

### LSH (Locality Sensitive Hashing)
LSH是一种将相似项映射到同一桶的哈希技术，通过band技术实现：
- 将MinHash签名分为b个band，每个band包含r行
- 只要有一个band完全匹配，两个项目就被认为是候选相似对

## 系统架构

### 核心参数配置
```kotlin
const val MH_DIM = 128      // MinHash签名维度
const val R = 2             // 每个band的行数
const val BANDS = MH_DIM / R // band数量 = 64
```

### 数据结构
```kotlin
// 原子定义
data class MyAtom(val relationId: Long, val entityId: Int)
// entityId < 0: Binary原子 r(X,Y)
// entityId = 0: 存在性原子 r(X,·)
// entityId > 0: 常量原子 r(X,c)

// 公式定义
data class Formula(val atom1: MyAtom?, val atom2: MyAtom?, val atom3: MyAtom?)

// LSH存储结构：双级Map
val bandToFormulas = mutableMapOf<Int, MutableMap<Int, MutableList<Formula>>>()
```

## 哈希函数设计

### 设计原则
1. **空间分离**: Binary原子使用正数空间，Unary原子使用负数空间
2. **高性能**: 使用位移操作替代乘法运算
3. **顺序敏感**: Binary原子区分(entity1, entity2)和(entity2, entity1)
4. **随机性**: 通过种子确保不同哈希函数的独立性

### Binary原子哈希函数
```kotlin
fun computeBinaryHash(entity1: Int, entity2: Int, seedIndex: Int): Int {
    val seed = globalHashSeeds[seedIndex]
    // 使用位移和异或，避免乘法运算
    val hash1 = (entity1 shl 7) xor (entity1 ushr 9) xor seed
    val hash2 = (entity2 shl 11) xor (entity2 ushr 5) xor (seed shl 3)
    val combinedHash = hash1 xor (hash2 shl 13) xor (seedIndex shl 17)
    return combinedHash and Int.MAX_VALUE // 确保为正数 [0, Int.MAX_VALUE]
}
```

**设计特点:**
- **顺序敏感**: entity1和entity2使用不同的位移量(7 vs 11)
- **正数空间**: 通过`and Int.MAX_VALUE`确保结果为正
- **高性能**: 纯位运算，无乘法操作

### Unary原子哈希函数
```kotlin
fun computeUnaryHash(entity: Int, seedIndex: Int): Int {
    val seed = globalHashSeeds[seedIndex]
    // 使用位移和异或，避免乘法运算
    val hash = (entity shl 5) xor (entity ushr 11) xor (seed shl 7) xor (seedIndex shl 2)
    return hash or Int.MIN_VALUE // 确保为负数 [Int.MIN_VALUE, -1]
}
```

**设计特点:**
- **负数空间**: 通过`or Int.MIN_VALUE`确保结果为负
- **简化计算**: 相比Binary哈希更简单，适合单实体场景
- **空间隔离**: 与Binary哈希完全分离，避免冲突

### 哈希种子管理
```kotlin
// 全局种子初始化
private fun initializeGlobalHashSeeds() {
    val random = java.util.Random(42) // 固定种子确保可重现
    val seedSet = mutableSetOf<Int>()
    
    while (seedSet.size < MH_DIM) {
        val seed = random.nextInt(Int.MAX_VALUE)
        seedSet.add(seed)
    }
    
    globalHashSeeds = seedSet.toIntArray()
}
```

## MinHash签名计算

### Binary原子MinHash
```kotlin
fun computeBinaryMinHash(instanceSet: Set<Pair<Int, Int>>): IntArray {
    val signature = IntArray(MH_DIM) { Int.MAX_VALUE }
    
    instanceSet.forEach { (entity1, entity2) ->
        for (i in 0 until MH_DIM) {
            val hashValue = computeBinaryHash(entity1, entity2, i)
            if (hashValue < signature[i]) {
                signature[i] = hashValue
            }
        }
    }
    
    return signature
}
```

### Unary原子MinHash
```kotlin
fun computeUnaryMinHash(instanceSet: Set<Int>): IntArray {
    val signature = IntArray(MH_DIM) { Int.MAX_VALUE }
    
    instanceSet.forEach { entity ->
        for (i in 0 until MH_DIM) {
            val hashValue = computeUnaryHash(entity, i)
            if (hashValue < signature[i]) {
                signature[i] = hashValue
            }
        }
    }
    
    return signature
}
```

## LSH分桶系统

### 分桶策略
```kotlin
fun performLSH(formula: Formula, minHashSignature: IntArray, supp: Int, rpLength: Int): Boolean {
    val targetBuckets = mutableSetOf<Pair<Int, Int>>()
    
    // 分为BANDS个band，每个band有R行
    for (bandIndex in 0 until BANDS) {
        val key1 = minHashSignature[bandIndex * R]     // 第一个MinHash值
        val key2 = minHashSignature[bandIndex * R + 1] // 第二个MinHash值
        
        if (rpLength <= 1) {
            targetBuckets.add(Pair(key1, key2))
        } else {
            // 长路径只添加到已存在的桶中
            val level1Map = bandToFormulas[key1]
            val bucket = level1Map?.get(key2)
            if (bucket != null) {
                // 找到相关L1公式，进行相似性评估
                return true
            }
        }
    }
    
    // 添加L1公式到新桶
    if (rpLength <= 1) {
        synchronized(bandToFormulas) {
            targetBuckets.forEach { (key1, key2) ->
                val level1Map = bandToFormulas.getOrPut(key1) { mutableMapOf() }
                val bucket = level1Map.getOrPut(key2) { mutableListOf() }
                bucket.add(formula)
            }
        }
    }
    
    return rpLength <= 1
}
```

### 双级Map结构
- **第一级**: MinHash值1作为键
- **第二级**: MinHash值2作为键
- **值**: 公式列表
- **优势**: 减少哈希冲突，提高查找效率

## 原子化流程

### Binary原子化
1. **数据准备**: 收集关系路径的所有(head, tail)实体对
2. **MinHash计算**: 为实体对集合计算MinHash签名
3. **LSH分桶**: 将Binary原子公式分配到LSH桶中

### Unary原子化
1. **常量原子**: r(X,c) - 为每个常量c创建原子
2. **存在性原子**: r(X,·) - 表示关系的存在性
3. **实例集合**: 收集相应的实体集合
4. **MinHash计算**: 为实体集合计算MinHash签名

### 原子化示例
```kotlin
// Binary原子: relation(X,Y)
val binaryAtom = MyAtom(relationId, -1)

// Unary常量原子: relation(X, entity123)
val unaryAtom = MyAtom(relationId, 123)

// Unary存在性原子: relation(X, ·)
val existenceAtom = MyAtom(relationId, 0)
```

## 性能优化

### 计算优化
1. **位运算替代乘法**: 使用`shl`、`ushr`等位移操作
2. **避免Long运算**: 全部使用Int运算减少开销
3. **内存预分配**: 预先分配LSH桶减少Map扩容

### 并发优化
1. **线程安全**: 关键数据结构使用synchronized保护
2. **读写分离**: LSH查询和更新分别优化
3. **避免重复**: 使用Set去重避免重复添加

### 空间优化
1. **哈希空间分离**: Binary正数，Unary负数，充分利用32位空间
2. **双级Map**: 减少单级Map的哈希冲突
3. **按需存储**: 长路径不创建新桶，仅查询已有桶

## 相似度估计

### Jaccard相似度估计
```kotlin
private fun estimateJaccardSimilarity(signature1: IntArray, signature2: IntArray): Double {
    return signature1.zip(signature2)
        .count { (h1, h2) -> h1 == h2 }
        .toDouble() / signature1.size
}
```

### 交集大小估计
```kotlin
private fun estimateIntersectionSize(jaccardSimilarity: Double, size1: Int, size2: Int): Double {
    return jaccardSimilarity * (size1 + size2) / (1 + jaccardSimilarity)
}
```

## 算法复杂度

### 时间复杂度
- **MinHash计算**: O(|S| × k) - S为实例集合大小，k为哈希函数数量
- **LSH分桶**: O(k) - 每个公式需要处理k个哈希值
- **相似度查询**: O(1) - 平均情况下的桶查找

### 空间复杂度
- **MinHash存储**: O(F × k) - F为公式数量
- **LSH桶**: O(F × b) - 平均每个公式在b个桶中
- **哈希种子**: O(k) - k个全局种子

## 应用场景

### 关系路径学习
1. **L1关系**: 直接原子化为Binary/Unary原子
2. **长路径**: 通过LSH查找相似的L1公式组合
3. **公式生成**: 基于相似性估计生成新的逻辑公式

### 相似性检测
1. **快速过滤**: 通过LSH快速找到候选相似对
2. **精确估计**: 使用MinHash估计Jaccard相似度
3. **阈值过滤**: 根据MIN_SUPP过滤低支持度组合

## 参数调优

### MinHash维度 (MH_DIM)
- **更大维度**: 提高相似度估计精度，但增加计算和存储开销
- **推荐值**: 128 (在精度和性能间平衡)

### Band配置 (BANDS, R)
- **更多Band**: 提高召回率，但可能增加假阳性
- **更大R**: 提高精确率，但可能降低召回率
- **当前配置**: BANDS=64, R=2 (适合中等相似度阈值)

### 支持度阈值 (MIN_SUPP)
- **作用**: 过滤低频模式，减少噪声
- **调优**: 根据数据集特点和应用需求调整

## 总结

本MinHash+LSH系统通过以下关键技术实现了高效的相似性检测：

1. **高性能哈希函数**: 纯位运算，避免昂贵的乘法操作
2. **智能空间分配**: 正负数空间分离，最大化哈希空间利用
3. **优化的LSH分桶**: 双级Map结构减少冲突，支持高并发
4. **渐进式处理**: L1公式建桶，长路径查询，避免组合爆炸

该系统在保证算法正确性的同时，实现了显著的性能提升，适用于大规模关系路径学习任务。
