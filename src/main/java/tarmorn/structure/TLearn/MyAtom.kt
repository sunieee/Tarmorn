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
    
    val minHashSignature: IntArray = computeMinHashDOPH(instances, isBinary)

    val support: Int
        get() = instances.size

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is MyAtom<*>) return false
        return relationId == other.relationId && entityId == other.entityId
    }

    override fun hashCode(): Int {
        var result = relationId.hashCode()
        result = 31 * result + entityId
        return result
    }

    override fun toString(): String {
        if (entityId == IdManager.getZId()) return ""
        val relationStr = IdManager.getRelationString(relationId)
        return when {
            entityId == IdManager.getYId() -> "$relationStr(X,Y)"
            entityId == IdManager.getXId() -> "$relationStr(X,X)"
            entityId == 0 -> "$relationStr(X,·)"
            else -> {
                val entityStr = IdManager.getEntityString(entityId)
                "$relationStr(X,$entityStr)"
            }
        }
    }

    fun getRuleString(): String = if (entityId == IdManager.getZId()) "" else IdManager.getAtomString(relationId, entityId)

    fun inverse(): MyAtom<T> {
        val inverseRelation = RelationPath.getInverseRelation(relationId)
        
        // 对于 BinaryAtom (Long)，需要交换高低32位
        @Suppress("UNCHECKED_CAST")
        val inverseInstances = if (isBinary && instances.firstOrNull() is Long) {
            instances.map { pair ->
                val longPair = pair as Long
                val h = (longPair ushr 32).toInt()
                val t = longPair.toInt()
                (t.toLong() shl 32) or (h.toLong() and 0xFFFFFFFFL)
            }.toSet() as Set<T>
        } else {
            instances
        }
        
        return MyAtom(inverseRelation, entityId, inverseInstances)
    }

    val isBinary: Boolean
        get() = entityId == IdManager.getYId()

    val isL1Atom: Boolean
        get() = relationId < RelationPath.MAX_RELATION_ID

    val isL2Atom: Boolean
        get() = relationId < RelationPath.MAX_L2RELATION_ID

    val isHeadAtom: Boolean
        get() = isL1Atom &&
                ((entityId == IdManager.getYId() || entityId == IdManager.getXId()) && !IdManager.isInverseRelation(relationId)
                        || entityId > 0)

    val firstRelation: Long
        get() = if (isL1Atom) relationId else RelationPath.getFirstRelation(relationId)

    /**
     * Check if this atom contains the given instance
     * For UnaryAtom: directly check if e is in instances
     * For BinaryAtom: 
     *   - If L1: directly check if e is in instances
     *   - Otherwise: decompose relationId to r1, r2 and check if R2h2t[r1][h] intersects with R2h2t[r2'][t]
     */
    fun hasInstance(e: Any): Boolean {
        return when {
            // UnaryAtom: instances is Set<Int>
            !isBinary -> {
                @Suppress("UNCHECKED_CAST")
                (instances as Set<Int>).contains(e as Int)
            }
            // BinaryAtom: instances is Set<Long>
            else -> {
                val longE = e as Long
                if (isL1Atom) {
                    @Suppress("UNCHECKED_CAST")
                    (instances as Set<Long>).contains(longE)
                } else {
                    // 分解 Long 为 h, t
                    val h = (longE ushr 32).toInt()
                    val t = longE.toInt()
                    
                    // 分解 relationId 为 r1, r2（其中 r2 长度为1）
                    val relations = RelationPath.decode(relationId)
                    require(relations.size >= 2) { "L2+ atom should have at least 2 relations" }
                    
                    // r2 是最后一个关系（长度为1）
                    val r2 = relations.last()
                    // r1 是前面所有关系的连接
                    val r1 = if (relations.size == 2) {
                        relations[0]
                    } else {
                        // 连接前 n-1 个关系
                        var rp = relations[0]
                        for (i in 1 until relations.size - 1) {
                            rp = RelationPath.connectHead(rp, relations[i])
                        }
                        rp
                    }
                    
                    val r2Inv = RelationPath.getInverseRelation(r2)
                    
                    // 检查 R2h2t[r1][h] 与 R2h2t[r2'][t] 是否有交集
                    val r1Tails = TLearn.R2h2tSet[r1]?.get(h)
                    val r2InvTails = TLearn.R2h2tSet[r2Inv]?.get(t)
                    
                    if (r1Tails != null && r2InvTails != null) {
                        // 检查是否有交集
                        r1Tails.any { it in r2InvTails }
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
         * Compute hash for a pair (h, t) - used for Long type
         */
        fun pairHash32(h: Int, t: Int): Int {
            val uH = h * -0x61c88647     // 0x9E3779B9 的补码（黄金比例常数）
            val uT = t * 0x85ebca6b.toInt()
            return uH xor Integer.rotateLeft(uT, 16)
        }

        /**
         * Compute MinHash signature using OPH + DOPH algorithm
         */
        fun <T> computeMinHashDOPH(instanceSet: Set<T>, isBinary: Boolean): IntArray {
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
                        val h = (e ushr 32).toInt()  // high 32 bits
                        val t = e.toInt()             // low 32 bits
                        pairHash32(h, t)
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
            // Existence: r(X,·)
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
