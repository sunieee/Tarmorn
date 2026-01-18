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

    val isInverseRelation: Boolean
        get() = IdManager.isInverseRelation(relationId)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DepAtom) return false
        return relationId == other.relationId && entityId == other.entityId
    }

    fun getBinaryAtom(): DepAtom {
        require(!isBinary) { "getBinaryAtom can only be called on unary atoms" }
        return DepAtom(if (isInverseRelation) IdManager.getInverseRelation(relationId) else relationId, IdManager.getYId())
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

    fun getRuleString(isVariableY: Boolean = false): String {
        if (isVariableY) {
            require(!isBinary) { "isVariableY is only allowed for unary atoms" }
        }
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
        nodes[0] = if(isVariableY) "Y" else "X"
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
     * 具象化原子：获取满足当前原子的所有实体实例
     * 
     * 对于一元原子（unary atom，仅支持L1）：
     * - 返回所有满足该原子的实体集合
     * - 例如：rel(const) 返回所有满足 rel(X, const) 的 X 集合
     * 
     * 对于二元原子（binary atom）：
     * - 需要提供 entityId 和 isHead 参数
     * - isHead=true: 给定head实体，返回所有可能的tail实体
     * - isHead=false: 给定tail实体，返回所有可能的head实体
     * 
     * @param givenEntityId 对于二元原子，需要提供的实体ID（作为head或tail）
     * @param isHead 对于二元原子，givenEntityId是否作为head（true）还是tail（false）
     * @return 满足条件的实体ID集合
     */
    fun materialize(givenEntityId: Int = -1, isHead: Boolean = true): Set<Int> {
        return if (isBinary) {
            materializeBinary(givenEntityId, isHead)
        } else {
            require(isL1Atom) { "materialize only supports L1 unary atoms, got: $this" }
            getUnaryInstances()
        }
    }

    /**
     * 具象化二元原子：给定一个实体（作为head或tail），返回所有能与之形成关系的另一端实体
     */
    private fun materializeBinary(givenEntityId: Int, isHead: Boolean): Set<Int> {
        require(givenEntityId > 0) { "Binary atom materialize requires a valid entityId, got: $givenEntityId" }
        
        // 根据isHead决定使用正向还是反向关系
        val actualRelation = if (isHead) relationId else RelationPath.getInverseRelation(relationId)
        
        // 对于L1原子，直接查询
        if (isL1Atom) {
            return tarmorn.DepLearn.r2h2tSet[actualRelation]?.get(givenEntityId) ?: emptySet()
        }
        
        // 对于L2+原子，沿着关系路径逐步扩展
        val relations = RelationPath.decode(actualRelation)
        var currentLayer = setOf(givenEntityId)
        
        for (relation in relations) {
            val nextLayer = mutableSetOf<Int>()
            for (entity in currentLayer) {
                val successors = tarmorn.DepLearn.r2h2tSet[relation]?.get(entity) ?: continue
                nextLayer.addAll(successors)
            }
            currentLayer = nextLayer
            if (currentLayer.isEmpty()) break
        }
        
        return currentLayer
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
        maxAttempts: Int = Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS,
        maxGroundings: Int = Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS,
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
        val actualMaxAttempts = if (samplingRound==0) maxAttempts else maxAttempts / 10
        val actualMaxGroundings = if (samplingRound==0) maxGroundings else maxGroundings / 10
        
        // 确保_instances已初始化
        val instances = _instances ?: return emptySet()
        
        // 双向采样：forward + inverse
        val forwardInstances = sampleBinaryInstancesEDISDirection(
            relationPathId = relationId,
            isForward = true,
            maxAttempts = actualMaxAttempts,
            maxGroundings = actualMaxGroundings,
            maxRepetitions = maxRepetitions
        )
        val inverseInstances = sampleBinaryInstancesEDISDirection(
            relationPathId = RelationPath.getInverseRelation(relationId),
            isForward = false,
            maxAttempts = actualMaxAttempts,
            maxGroundings = actualMaxGroundings,
            maxRepetitions = maxRepetitions
        )
        
        val newInstances = mutableSetOf<Long>()
        newInstances.addAll(forwardInstances)
        newInstances.addAll(inverseInstances)
        
        samplingRound++
        
        val totalAttempts = actualMaxAttempts * 2
        if (samplingRound >= 100 ||
            newInstances.isEmpty() ||
            newInstances.size.toDouble() / totalAttempts < 0.05) {
            samplingExhausted = true
        }
        
        instances.addAll(newInstances)
        return newInstances
    }

    /**
     * 单向EDIS采样的内部实现
     */
    private fun sampleBinaryInstancesEDISDirection(
        relationPathId: Long,
        isForward: Boolean,
        maxAttempts: Int,
        maxGroundings: Int,
        maxRepetitions: Int
    ): Set<Long> {
        val instances = _instances ?: return emptySet()

        val relations = RelationPath.decode(relationPathId)
        val firstRelation = relations[0]

        val startEntities = getSampledStartEntities(firstRelation, maxAttempts)
        if (startEntities.isEmpty()) return emptySet()

        val newInstances = mutableSetOf<Long>()
        var attempts = 0
        var repetitions = 0

        for (startEntity in startEntities) {
            if (attempts >= maxAttempts) break
            if (newInstances.size >= maxGroundings) break
            if (repetitions >= maxRepetitions) break

            attempts++

            val endEntity = beamCyclicPath(startEntity, relations.toList())
            if (endEntity != null && endEntity != startEntity) {
                val instance = if (isForward) {
                    packLong(startEntity, endEntity)
                } else {
                    packLong(endEntity, startEntity)
                }

                if (instance !in instances && instance !in newInstances) {
                    newInstances.add(instance)
                    repetitions = 0
                } else {
                    repetitions++
                }
            }
        }

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