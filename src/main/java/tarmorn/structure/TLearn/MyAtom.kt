package tarmorn.structure.TLearn

import tarmorn.TLearn
import tarmorn.Settings
import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import kotlin.math.abs

/**
 * MyAtom - represents an atom in formulas.
 * @param T Instance type: Int for UnaryAtom, Long for BinaryAtom
 * relationId: relation or relation-path id
 * entityId: Y for binary, X for loop, 0 for existence, >0 for constant entity id
 * instances: set of entity instances covered by this atom (only for non-L1 atoms)
 * minHashSignature: computed MinHash signature for LSH
 * 
 * Note: For L1 BinaryAtom with inverse relation, instances are stored in forward order
 * but isInverseInstances can be derived from (isInverseRelation && isBinary && isL1Atom)
 */
class MyAtom<T>(
    val relationId: Long,
    val entityId: Int,
    var instances: Set<T> = emptySet()
) {
    init {
        if (instances.isNotEmpty()) {
            require(instances.size >= Settings.MIN_SUPP) {
                "MyAtom instances size ${instances.size} must be >= MIN_SUPP ${Settings.MIN_SUPP}"
            }
        } else {
            @Suppress("UNCHECKED_CAST")
            instances = getInstanceSet() as Set<T>
        }
        // 确保 instances 不为空，否则会出现 metric={"support":0.0, "headSize":19, "bodySize":0, "confidence":NaN}, headInstances=[]..., bodyInstances=[]
        require(instances.isNotEmpty()) {
            "MyAtom instances cannot be empty after initialization for relationId=$relationId, entityId=$entityId"
        }
    }
    
    // 延迟计算：minHashSignature 只有在第一次被访问时才会计算
    // 结果缓存：计算完成后结果会被保存，后续访问直接返回缓存值，不会重复计算
    val minHashSignature: IntArray by lazy { computeMinHashDOPH(instances, isBinary, isInverseInstances) }

    // 判断实例集是否为反向存储：仅对 L1 BinaryAtom 且是 inverse relation 时为 true
    val isInverseInstances: Boolean
        get() = isBinary && isL1Atom && IdManager.isInverseRelation(relationId)

    // ===== L2+ 原子的预计算缓存（用于 hasInstance 优化）=====
    // 缓存 r1Path（前 n-1 个关系的连接）和 r2InvPath（最后一个关系的逆）
    // 只有 L2+ BinaryAtom 才会用到，L1 原子访问时返回 null
    private val l2PlusPathCache: Pair<Long, Long>? by lazy {
        if (isL1Atom || !isBinary) null
        else {
            val relations = RelationPath.decode(relationId)
            require(relations.size >= 2) { "L2+ atom should have at least 2 relations" }
            
            // r2 是最后一个关系
            val r2 = relations.last()
            // r1 是前面所有关系的连接
            val r1 = if (relations.size == 2) {
                relations[0]
            } else {
                var rp = relations[0]
                for (i in 1 until relations.size - 1) {
                    rp = RelationPath.connectHead(rp, relations[i])
                }
                rp
            }
            val r2Inv = RelationPath.getInverseRelation(r2)
            Pair(r1, r2Inv)
        }
    }

    val support: Int
        get() = instances.size

    // 判断是否为采样后的原子（L2/L3 BinaryAtom 且达到最大采样数）
    val isSampled: Boolean
        get() = isBinary && when {
            isL1Atom -> false
            isL2Atom -> support >= Settings.MAX_JOIN_INSTANCES_L2
            else -> support >= Settings.MAX_JOIN_INSTANCES_L3  // L3 Atom
        }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is MyAtom<*>) return false
        return relationId == other.relationId && entityId == other.entityId
    }

    override fun hashCode(): Int {
        // IMPORTANT: 注意不能使用简单的31 * relationId.hashCode() + entityId，很容易冲突
        return pairHash32(relationId.hashCode(), entityId)
    }

    override fun toString(): String {
        require(entityId != IdManager.getZId()) { "EntityId cannot be ZId in toString()" }
        val relationStr = IdManager.getRelationString(relationId)
        return when {
            entityId == IdManager.getYId() -> "$relationStr(X,Y)"
            entityId == IdManager.getXId() -> "$relationStr(X,X)"
            entityId == 0 -> "$relationStr(X,*)"
            else -> {
                val entityStr = IdManager.getEntityString(entityId)
                "$relationStr(X,$entityStr)"
            }
        }
    }

    // 注意：inverse仅仅在validateH2B时调用，head和body同时取反，所以instances不用变
    // isInverseInstances 现在通过 isInverseRelation && isBinary && isL1Atom 自动计算
    fun inverse() = MyAtom(RelationPath.getInverseRelation(relationId), entityId, instances)

    // fun getBinaryAtom(): MyAtom = MyAtom(relationId, IdManager.getYId())

    val isBinary: Boolean
        get() = entityId == IdManager.getYId()

    val isL1Atom: Boolean
        get() = relationId < RelationPath.MAX_RELATION_ID

    val isL2Atom: Boolean
        get() = relationId < RelationPath.MAX_L2RELATION_ID

    // val isHeadAtom: Boolean
    //     get() = isL1Atom &&
    //             ((entityId == IdManager.getYId() || entityId == IdManager.getXId()) && !IdManager.isInverseRelation(relationId)
    //                     || entityId > 0)

    val isHeadAtom: Boolean
        get() = isL1Atom && entityId != 0

    val firstRelation: Long
        get() = if (isL1Atom) relationId else RelationPath.getFirstRelation(relationId)

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

    /**
     * Check if this atom contains the given instance
     * For UnaryAtom: directly check if e is in instances
     * For BinaryAtom: 
     *   - If L1: directly check if e is in instances
     *   - Otherwise: decompose relationId to r1, r2 and check if R2h2t[r1][h] intersects with R2h2t[r2'][t]
     */
    fun hasInstance(e: Any, isInverse: Boolean = false): Boolean {
        return when {
            // UnaryAtom: instances is Set<Int>
            !isBinary -> {
                @Suppress("UNCHECKED_CAST")
                (instances as Set<Int>).contains(e as Int)
            }
            // BinaryAtom: instances is Set<Long>
            else -> {
                val longE = e as Long
                val cache = l2PlusPathCache
                
                if (cache == null) {
                    // L1 原子：直接查实例集
                    val realE = if (isInverse xor isInverseInstances) {
                        // 如果实例集和查询方向不一致，则需要取反
                        val h = (longE ushr 32).toInt()
                        val t = longE.toInt()
                        ((t.toLong() shl 32) or (h.toLong() and 0xFFFFFFFFL))
                    } else {
                        longE
                    }
                    @Suppress("UNCHECKED_CAST")
                    (instances as Set<Long>).contains(realE)
                } else {
                    // L2+ 原子：使用缓存的 r1, r2Inv 查表
                    val (r1, r2Inv) = cache
                    
                    // 分解 Long 为 h, t
                    var h = (longE ushr 32).toInt()
                    var t = longE.toInt()
                    if (isInverse) {
                        val temp = h
                        h = t
                        t = temp
                    }
                    
                    // 检查 R2h2t[r1][h] 与 R2h2t[r2Inv][t] 是否有交集
                    val r1Tails = TLearn.R2h2tSet[r1]?.get(h)
                    val r2InvTails = TLearn.R2h2tSet[r2Inv]?.get(t)
                    
                    if (r1Tails != null && r2InvTails != null) {
                        // 遍历更小的集合以优化性能
                        if (r1Tails.size <= r2InvTails.size) {
                            r1Tails.any { it in r2InvTails }
                        } else {
                            r2InvTails.any { it in r1Tails }
                        }
                    } else {
                        false
                    }
                }
            }
        }
    }

    companion object {
        // OPH + DOPH constants
        private const val OPH_SEED_BIN = 0x9e3779b9.toInt()
        private const val OPH_SEED_RANK = 0x85ebca6b.toInt()
        private const val DOPH_SALT = 0x165667b1.toInt()

        private inline fun pos32(x: Int) = x and 0x7fffffff

        private fun mix64(z0: Long): Long {
            var z = z0 + 0x9E3779B97F4A7C15UL.toLong()
            z = (z xor (z ushr 30)) * 0xBF58476D1CE4E5B9UL.toLong()
            z = (z xor (z ushr 27)) * 0x94D049BB133111EBUL.toLong()
            return z xor (z ushr 31)
        }

        private fun mix32(z0: Int): Int {
            val z = z0.toLong() and 0xFFFF_FFFFL
            return (mix64(z) ushr 32).toInt()
        }

        /**
         * Compute hash for unary atom
         */
        fun computeUnaryHash(entity: Int, seed: Int): Int {
            val hash = mix32(entity xor seed)
            return abs(hash)
        }

        fun pairHash32(h: Int, t: Int): Int {
            val uH = h * -0x61c88647     // 0x9E3779B9 的补码（黄金比例常数）
            val uT = t * 0x85ebca6b.toInt()
            return uH xor Integer.rotateLeft(uT, 16)
        }

        /**
         * Compute hash for binary atom (pair of entities)
         */
        fun computeBinaryHash(entity1: Int, entity2: Int, seed: Int): Int {
            val uH = entity1 * -0x61c88647
            val uT = entity2 * 0x85ebca6b.toInt()
            var hash = uH xor Integer.rotateLeft(uT, 16)
            hash = mix32(hash xor seed)
            return -abs(hash)
        }

        /**
         * Compute MinHash signature using OPH + DOPH algorithm
         * @param isInverseInstances 当为 true 时，对 Long 类型实例进行头尾交换后再计算 hash
         *                           这样正向和反向的 BinaryAtom 会有相同的 MinHash 签名
         */
        fun <T> computeMinHashDOPH(instanceSet: Set<T>, isBinary: Boolean, isInverseInstances: Boolean = false): IntArray {
            if (instanceSet.isEmpty()) {
                throw IllegalArgumentException("Cannot compute MinHash for empty instance set")
            }

            val k = TLearn.MH_DIM
            val sig = IntArray(k) { Int.MAX_VALUE }
            require((k and (k - 1)) == 0) { "MH_DIM must be a power of 2" }
            val mask = k - 1

            // One pass: compute two 32-bit hashes for each element
            for (e in instanceSet) {
                // 对于 Int 类型，直接使用；对于 Long 类型，分解为 h 和 t 两部分
                val baseHash = when (e) {
                    is Int -> e
                    is Long -> {
                        var h = (e ushr 32).toInt()  // high 32 bits
                        var t = e.toInt()             // low 32 bits
                        // 如果是反向实例，交换头尾以保持与正向实例相同的 hash
                        if (isInverseInstances) pairHash32(t, h) else pairHash32(h, t)
                    }
                    else -> e.hashCode()
                }
                
                val hBin = computeUnaryHash(baseHash, OPH_SEED_BIN)
                val binId = pos32(hBin) and mask

                val hRank = computeUnaryHash(baseHash, OPH_SEED_RANK) xor (binId * DOPH_SALT)
                val rank = pos32(mix32(hRank))

                if (rank < sig[binId]) sig[binId] = rank
            }

            // DOPH: densify empty buckets
            if (sig.any { it == Int.MAX_VALUE }) {
                for (i in 0 until k) {
                    if (sig[i] != Int.MAX_VALUE) continue
                    var j = 1
                    while (j < k && sig[(i + j) % k] == Int.MAX_VALUE) j++
                    if (j == k) {
                        sig[i] = pos32(mix32(i * DOPH_SALT + 1))
                    } else {
                        val donorIdx = (i + j) % k
                        val offset = pos32(mix32(i * DOPH_SALT + j))
                        sig[i] = sig[donorIdx] xor offset
                    }
                }
            }

            if (isBinary) {
                for (i in 0 until k) sig[i] = -sig[i]
            }
            return sig
        }

    }

    // Get instance set for this atom
    fun getInstanceSet(): Set<*> {
        // if (!isL1Atom) return instances
        require(isL1Atom) { "getInstanceSet() only supports L1 atoms" }
        return when {
            entityId == IdManager.getZId() -> setOf(0)
            // Binary: r(X,Y)
            entityId == IdManager.getYId() -> TLearn.r2instanceSet[relationId] ?: emptySet()
            // Unary constant: r(X,c)
            entityId > 0 -> {
                val inv = IdManager.getInverseRelation(relationId)
                TLearn.R2h2tSet[inv]?.get(entityId) ?: emptySet()
            }
            // Existence: r(X,*)
            entityId == 0 -> TLearn.R2h2tSet[relationId]?.keys ?: emptySet()
            // Loop: r(X,X)
            entityId == IdManager.getXId() -> TLearn.r2loopSet[relationId] ?: emptySet()
            else -> emptySet()
        }
    }
}

// Type aliases for convenience
typealias UnaryAtom = MyAtom<Int>
typealias BinaryAtom = MyAtom<Long>