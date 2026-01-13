package tarmorn.structure.TLearn

import tarmorn.TLearn
import tarmorn.Settings
import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import kotlin.math.abs

/**
 * DepAtom - represents an atom in formulas.
 * @param T Instance type: Int for UnaryAtom, Long for BinaryAtom
 * relationId: relation or relation-path id
 * entityId: Y for binary, X for loop, 0 for existence, >0 for constant entity id
 * instances: set of entity instances covered by this atom (only for non-L1 atoms)
 * minHashSignature: computed MinHash signature for LSH
 * 
 * Note: For L1 BinaryAtom with inverse relation, instances are stored in forward order
 * but isInverseInstances can be derived from (isInverseRelation && isBinary && isL1Atom)
 */
class DepAtom(
    val relationId: Long,
    val entityId: Int
) {
    // 内部缓存的实例集合（仅用于非L1的BinaryAtom）
    // 使用ConcurrentHashMap.newKeySet()保证线程安全
    private val _instances: MutableSet<Long>? by lazy {
        if (isBinary && !isL1Atom) {
            java.util.concurrent.ConcurrentHashMap.newKeySet<Long>()
        } else {
            null
        }
    }
    
    // 标记是否已经采样耗尽（连续采样收益很低）
    @Volatile
    var samplingExhausted: Boolean = false
    
    // 标记是否已经进行过至少一次采样（用于区分首次采样和增量采样）
    @Volatile
    var samplingRound: Int = 0

    val hasBeenSampled: Boolean
        get() = samplingRound > 0
    
    /**
     * 获取实例集合（统一接口）
     * - L1 atom: 从DepLearn缓存获取
     * - 非L1 atom: 返回内部缓存的instances
     */
    val instances: Set<Long>
        get() = when {
            isL1Atom && isBinary -> tarmorn.DepLearn.r2instanceSet[relationId] ?: emptySet()
            !isL1Atom && isBinary -> _instances ?: emptySet()
            else -> emptySet()
        }
    
    // 判断实例集是否为反向存储：仅对 L1 BinaryAtom 且是 inverse relation 时为 true
    val isInverseInstances: Boolean
        get() = isBinary && isL1Atom && IdManager.isInverseRelation(relationId)


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DepAtom) return false
        return relationId == other.relationId && entityId == other.entityId
    }

    override fun hashCode(): Int {
        // IMPORTANT: 注意不能使用简单的31 * relationId.hashCode() + entityId，很容易冲突
        return pairHash32(relationId.hashCode(), entityId)
    }

    override fun toString(): String {
        val relationStr = IdManager.getRelationString(relationId)
        return when {
            entityId == IdManager.getYId() -> relationStr
            entityId == IdManager.getXId() -> "$relationStr(X)"
            entityId == 0 -> "$relationStr(*)"
            else -> {
                val entityStr = IdManager.getEntityString(entityId)
                "$relationStr($entityStr)"
            }
        }
    }

    fun getRuleString(): String {
        // Resolve terminal argument
        fun termString(eid: Int): String = when (eid) {
            IdManager.getYId() -> "Y"
            IdManager.getXId() -> "X"
            0 -> "*"
            else -> IdManager.getEntityString(eid)
        }

        // Decode relation path
        val relations: LongArray = if (relationId <= RelationPath.MAX_RELATION_ID) longArrayOf(relationId) else RelationPath.decode(relationId)
        val n = relations.size
        val tailTerm = termString(entityId)

        // Build node list: X, A, B, ..., tailTerm
        val nodes = Array(n + 1) { "" }
        nodes[0] = "X"
        for (i in 1 until n+1) {
            nodes[i] = ('A'.code + (i - 1)).toChar().toString()
        }
        if (tailTerm != "*") nodes[n] = tailTerm
        val parts = ArrayList<String>(n)
        for (i in 0 until n) {
            val r = relations[i]
            val inv = IdManager.isInverseRelation(r)
            val forward = if (inv) IdManager.getInverseRelation(r) else r
            val name = IdManager.getRelationString(forward)
            // swap args for inverse
            val left = if (inv) nodes[i + 1] else nodes[i]
            val right = if (inv) nodes[i] else nodes[i + 1]
            parts.add("$name($left,$right)")
        }
        return parts.joinToString(", ")
    }
    
    val isBinary: Boolean
        get() = entityId == IdManager.getYId()

    val isL1Atom: Boolean
        get() = relationId < RelationPath.MAX_RELATION_ID

    val isL2Atom: Boolean
        get() = relationId < RelationPath.MAX_L2RELATION_ID

    val isHeadAtom: Boolean
        get() = isL1Atom && entityId != 0

    val firstRelation: Long
        get() = if (isL1Atom) relationId else RelationPath.getFirstRelation(relationId)

    /**
     * Get unary instances (Set<Int>) - only for L1 atoms
     * Returns the set of head entities that satisfy this atom
     */
    fun getUnaryInstances(): Set<Int> {
        require(isL1Atom) { "getUnaryInstances only supports L1 atoms, got: $this" }
        require(!isBinary) { "getUnaryInstances does not support binary atoms, got: $this" }
        
        return when {
            // Loop atom: entities that loop on themselves
            entityId == IdManager.getXId() -> {
                tarmorn.DepLearn.ts.r2loopSet[relationId] ?: emptySet()
            }
            // Existence atom: all heads that have this relation
            entityId == 0 -> {
                tarmorn.DepLearn.r2h2tSet[relationId]?.keys ?: emptySet()
            }
            // Constant atom: heads that connect to this specific entity
            else -> {
                val inverseRelation = RelationPath.getInverseRelation(relationId)
                tarmorn.DepLearn.r2h2tSet[inverseRelation]?.get(entityId) ?: emptySet()
            }
        }
    }

    /**
     * Get binary instances (Set<Long>) - only for L1 atoms
     * Returns the set of (head, tail) pairs as Long
     */
    fun getBinaryInstances(): Set<Long> {
        require(isL1Atom) { "getBinaryInstances only supports L1 atoms, got: $this" }
        require(isBinary) { "getBinaryInstances only supports binary atoms, got: $this" }
        
        return tarmorn.DepLearn.r2instanceSet[relationId] ?: emptySet()
    }

    /**
     * Check if a binary instance exists using bi-directional DFS
     * 验证成功后会将实例添加到缓存
     * @param instance The (head, tail) pair as Long
     * @return true if instance exists
     */
    fun hasBinaryInstance(instance: Long): Boolean {
        require(isBinary) { "hasBinaryInstance only supports binary atoms, got: $this" }
        
        val h = (instance shr 32).toInt()
        val t = instance.toInt()
        
        // For L1 atoms, direct lookup
        if (isL1Atom) {
            return tarmorn.DepLearn.r2h2tSet[relationId]?.get(h)?.contains(t) ?: false
        }
        
        // 先检查缓存
        if (_instances?.contains(instance) == true) {
            return true
        }
        
        // For longer paths, use bi-directional search
        val relations = RelationPath.decode(relationId)
        val pathLength = relations.size
        var verified = false
        
        when (pathLength) {
            2 -> {
                // r1 * r2: check if exists middle node m such that r1(h, m) && r2(m, t)
                val r1 = relations[0]
                val r2 = relations[1]
                
                val r1Tails = tarmorn.DepLearn.r2h2tSet[r1]?.get(h) ?: return false
                val r2Inv = RelationPath.getInverseRelation(r2)
                val r2Heads = tarmorn.DepLearn.r2h2tSet[r2Inv]?.get(t) ?: return false
                
                // Check intersection
                verified = r1Tails.any { it in r2Heads && it != h && it != t }
            }
            3 -> {
                // r1 * r2 * r3: bi-directional search from both ends
                val r1 = relations[0]
                val r2 = relations[1]
                val r3 = relations[2]
                
                // Get 1-hop from head
                val head1hop = tarmorn.DepLearn.r2h2tSet[r1]?.get(h) ?: return false
                
                // Get 1-hop from tail (backward)
                val r3Inv = RelationPath.getInverseRelation(r3)
                val tail1hop = tarmorn.DepLearn.r2h2tSet[r3Inv]?.get(t) ?: return false
                
                // Choose smaller set to iterate
                if (head1hop.size <= tail1hop.size) {
                    // Iterate from head side
                    for (m in head1hop) {
                        if (m == h || m == t) continue
                        // Check if r2(m, ?) intersects with tail1hop
                        val r2Tails = tarmorn.DepLearn.r2h2tSet[r2]?.get(m) ?: continue
                        if (r2Tails.any { it in tail1hop && it != h && it != t }) {
                            verified = true
                            break
                        }
                    }
                } else {
                    // Iterate from tail side
                    for (m in tail1hop) {
                        if (m == h || m == t) continue
                        // Check if r2'(m, ?) intersects with head1hop
                        val r2Inv = RelationPath.getInverseRelation(r2)
                        val r2Heads = tarmorn.DepLearn.r2h2tSet[r2Inv]?.get(m) ?: continue
                        if (r2Heads.any { it in head1hop && it != h && it != t }) {
                            verified = true
                            break
                        }
                    }
                }
            }
            else -> {
                // For paths longer than 3, general bi-directional BFS
                // Start from both ends and meet in the middle
                val midPoint = pathLength / 2
                
                // Build forward reachable set from head
                var forwardReachable = setOf(h)
                for (i in 0 until midPoint) {
                    val nextReachable = mutableSetOf<Int>()
                    for (node in forwardReachable) {
                        val tails = tarmorn.DepLearn.r2h2tSet[relations[i]]?.get(node) ?: continue
                        nextReachable.addAll(tails)
                    }
                    forwardReachable = nextReachable
                }
                
                // Build backward reachable set from tail
                var backwardReachable = setOf(t)
                for (i in pathLength - 1 downTo midPoint) {
                    val nextReachable = mutableSetOf<Int>()
                    for (node in backwardReachable) {
                        val rInv = RelationPath.getInverseRelation(relations[i])
                        val heads = tarmorn.DepLearn.r2h2tSet[rInv]?.get(node) ?: continue
                        nextReachable.addAll(heads)
                    }
                    backwardReachable = nextReachable
                }
                
                verified = forwardReachable.any { it in backwardReachable && it != h && it != t }
            }
        }
        
        // 如果验证成功，添加到缓存
        if (verified) _instances?.add(instance)
        return verified
    }

    /**
     * 级联采样验证 - 用于复杂规则的评估
     * 先对grounding较少的atom进行EDIS采样，然后验证另一个atom是否满足
     * 
     * @param otherAtom 另一个要验证的atom
     * @param maxAttempts 最大采样尝试次数
     * @param maxGroundings 最大grounding数量
     * @return (predictedBoth, correctlyPredictedBoth, headSize)三元组
     */
    fun cascadeSamplingWith(
        otherAtom: DepAtom,
        headRelation: Long,
        maxAttempts: Int = Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS,
        maxGroundings: Int = Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS
    ): Triple<Int, Int, Int> {
        require(isBinary && otherAtom.isBinary) { "Cascade sampling only supports binary atoms" }
        
        // 1. 估算哪个atom的grounding更少（选择性更强）
        val thisSize = estimateGroundingSize()
        val otherSize = otherAtom.estimateGroundingSize()
        
        val (strictAtom, looseAtom) = if (thisSize <= otherSize) {
            Pair(this, otherAtom)
        } else {
            Pair(otherAtom, this)
        }
        
        println("  Strict: $strictAtom")
        println("  Loose: $looseAtom")
        
        // 2. 对strict atom进行EDIS采样（返回新采样的实例）
        val newSamples = strictAtom.sampleBinaryInstancesEDIS(
            maxAttempts = maxAttempts,
            maxGroundings = maxGroundings
        )
        
        // 使用所有已采样的实例（包括之前的）
        val strictSamples = strictAtom.instances
        
        println("  Sampled ${strictSamples.size} instances from strict atom")
        
        // 3. 对每个采样实例，验证loose atom是否也满足
        var predictedBoth = 0
        var correctlyPredictedBoth = 0
        
        val headSize = tarmorn.DepLearn.r2instanceSet[headRelation]?.size ?: 0
        
        for (instance in strictSamples) {
            // 使用bi-directional DFS验证loose atom
            if (looseAtom.hasBinaryInstance(instance)) {
                predictedBoth++
                
                // 验证head relation是否也满足
                val h = unpackHead(instance)
                val t = unpackTail(instance)
                if (tarmorn.DepLearn.r2h2tSet[headRelation]?.get(h)?.contains(t) == true) {
                    correctlyPredictedBoth++
                }
            }
        }
        
        println("  PredictedBoth=$predictedBoth, CorrectlyPredictedBoth=$correctlyPredictedBoth")
        
        return Triple(predictedBoth, correctlyPredictedBoth, headSize)
    }
    
    /**
     * 估算grounding大小（用于选择strict atom）
     * 对于L1 atom返回精确大小，对于L2+返回估算值
     */
    fun estimateGroundingSize(): Int {
        if (!isBinary) return 0
        
        if (isL1Atom) {
            return tarmorn.DepLearn.r2instanceSet[relationId]?.size ?: 0
        }
        
        // 对于L2+原子，估算大小 = 第一个关系的三元组数 * 平均度数^(路径长度-1)
        val relations: LongArray = RelationPath.decode(relationId)
        val firstRelSize: Int = tarmorn.DepLearn.r2instanceSet[relations[0]]?.size ?: 0
        
        // 简单估算：假设平均度数为10
        val avgDegree = 10.0
        val pathLength = relations.size
        
        return (firstRelSize * Math.pow(avgDegree, (pathLength - 1).toDouble())).toInt()
    }
    
    /**
     * EDIS采样 - Entity-D结果先缓存到本地，最后批量添加到内部instances
     * 支持多次调用，逐步累积实例
     * 
     * @param maxAttempts 最大采样尝试次数（默认1000，适合动态采样）
     * @param maxGroundings 目标grounding数量（默认10，适合动态采样）
     * @param maxRepetitions 最大连续重复次数（默认5）
     * @param minNewInstanceRatio 最小新增实例比例阈值（默认0.05，即5%），低于此比例则标记为采样耗尽
     * @return 本次新增的实例集合
     */
    fun sampleBinaryInstancesEDIS(
        maxAttempts: Int = 10000,
        maxGroundings: Int = 100,
        maxRepetitions: Int = Settings.BEAM_SAMPLING_MAX_REPETITIONS
    ): Set<Long> {
        require(isBinary) { "EDIS sampling only supports binary atoms, got: $this" }
        
        // L1原子不需要采样，直接使用DepLearn的缓存
        if (isL1Atom) {
            return emptySet()
        }
        
        // 如果已经标记为采样耗尽，直接返回
        if (samplingExhausted) {
            return emptySet()
        }
        
        // 判断是否为首次采样，如果是则使用更大的参数
        val actualMaxAttempts = if (samplingRound==0) Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS else maxAttempts
        val actualMaxGroundings = if (samplingRound==0) Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS else maxGroundings
        
        // 确保_instances已初始化
        val instances = _instances ?: return emptySet()
        
        // 对于L2+原子，使用EDIS采样
        val relations = RelationPath.decode(relationId)
        val firstRelation = relations[0]
        
        // 1. 获取起始实体（均匀分布）
        val startEntities = getSampledStartEntities(firstRelation, actualMaxAttempts)
        require(startEntities.isNotEmpty()) { "No start entities available for relation $firstRelation" }
        
        // 2. 使用本地缓存收集新实例，避免并发冲突
        val newInstances = mutableSetOf<Long>()
        var attempts = 0
        var repetitions = 0
        val currentSize = instances.size
        
        for (startEntity in startEntities) {
            // 检查停止条件（考虑已有实例和新采样的实例）
            if (attempts >= actualMaxAttempts) break
            if (newInstances.size >= actualMaxGroundings) break
            if (repetitions >= maxRepetitions) break
            
            attempts++
            
            // 随机游走完成路径
            val endEntity = beamCyclicPath(startEntity, relations.toList())
            
            if (endEntity != null && endEntity != startEntity) {
                val instance = packLong(startEntity, endEntity)
                
                // 检查是否已存在于旧实例或新实例中
                if (instance !in instances && instance !in newInstances) {
                    newInstances.add(instance)
                    repetitions = 0
                } else {
                    repetitions++
                }
            }
        }
        
        samplingRound++
        
        // 强制停止：如果采样轮次过多，标记为耗尽并立即返回
        // 检查采样效率：如果新增实例占尝试次数的比例太低，标记为采样耗尽
        if (samplingRound >= 100 || 
            attempts == 0 || 
            repetitions >= maxRepetitions ||
            newInstances.size.toDouble() / attempts < 0.05) 
            samplingExhausted = true        
        // 批量添加新实例到_instances，避免并发冲突
        instances.addAll(newInstances)
        return newInstances
    }
    
    /**
     * 获取采样的起始实体（均匀分布）
     * 类似TripleSet.getNRandomEntitiesByRelation，但针对DepAtom
     */
    private fun getSampledStartEntities(firstRelation: Long, n: Int): List<Int> {
        // 获取该关系的所有head实体
        val allHeads = tarmorn.DepLearn.r2h2tSet[firstRelation]?.keys ?: return emptyList()
        
        // 如果实体数少于n，返回所有实体
        if (allHeads.size <= n) {
            return allHeads.toList()
        }
        
        // 随机采样n个实体（均匀分布）
        return allHeads.shuffled().take(n)
    }
    
    /**
     * 随机游走完成路径
     * 从startEntity出发，沿着relations路径随机游走，返回终点实体
     * 
     * @param startEntity 起始实体
     * @param relations 关系路径（已解码）
     * @return 终点实体，如果路径不通返回null
     */
    private fun beamCyclicPath(startEntity: Int, relations: List<Long>): Int? {
        var currentEntity = startEntity
        val visitedEntities = mutableSetOf(startEntity)
        
        for (relation in relations) {
            // 获取当前实体通过该关系能到达的实体
            val nextEntities = tarmorn.DepLearn.r2h2tSet[relation]?.get(currentEntity)
            
            if (nextEntities == null || nextEntities.isEmpty()) {
                return null // 路径不通
            }
            
            // 随机选择一个未访问过的实体（OI约束）
            val candidateEntities = if (Settings.OI_CONSTRAINTS_ACTIVE) {
                nextEntities.filter { it !in visitedEntities }
            } else {
                nextEntities.toList()
            }
            
            if (candidateEntities.isEmpty()) {
                return null // 没有可选的实体
            }
            
            // 随机选择下一个实体
            currentEntity = candidateEntities.random()
            
            if (Settings.OI_CONSTRAINTS_ACTIVE) {
                visitedEntities.add(currentEntity)
            }
        }
        
        return currentEntity
    }
    
    /**
     * 将(head, tail)打包为Long
     */
    private fun packLong(head: Int, tail: Int): Long {
        return (head.toLong() shl 32) or (tail.toLong() and 0xFFFFFFFFL)
    }
    
    /**
     * 从Long解包出head
     */
    private fun unpackHead(instance: Long): Int {
        return (instance shr 32).toInt()
    }
    
    /**
     * 从Long解包出tail
     */
    private fun unpackTail(instance: Long): Int {
        return instance.toInt()
    }

    companion object {
        fun pairHash32(h: Int, t: Int): Int {
            val uH = h * -0x61c88647     // 0x9E3779B9 的补码（黄金比例常数）
            val uT = t * 0x85ebca6b.toInt()
            return uH xor Integer.rotateLeft(uT, 16)
        }
    }
}