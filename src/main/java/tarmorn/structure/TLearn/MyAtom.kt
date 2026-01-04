package tarmorn.structure.TLearn

import tarmorn.TLearn
import tarmorn.Settings
import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import kotlin.math.abs

/**
 * MyAtom - represents an atom in formulas.
 * relationId: relation or relation-path id
 * entityId: Y for binary, X for loop, 0 for existence, >0 for constant entity id
 * instances: set of entity instances covered by this atom (only for non-L1 atoms)
 * minHashSignature: computed MinHash signature for LSH
 */
class MyAtom(
    val relationId: Long,
    val entityId: Int,
    var instances: Set<Int> = emptySet()
) {
    init {
        if (instances.isNotEmpty()) {
            require(instances.size >= Settings.MIN_SUPP) {
                "MyAtom instances size ${instances.size} must be >= MIN_SUPP ${Settings.MIN_SUPP}"
            }
        } else {
            instances = getInstanceSet()
        }
        // 确保 instances 不为空，否则会出现 metric={"support":0.0, "headSize":19, "bodySize":0, "confidence":NaN}, headInstances=[]..., bodyInstances=[]
        require(instances.isNotEmpty()) {
            "MyAtom instances cannot be empty after initialization for relationId=$relationId, entityId=$entityId"
        }
    }
    
    // 延迟计算：minHashSignature 只有在第一次被访问时才会计算
    // 结果缓存：计算完成后结果会被保存，后续访问直接返回缓存值，不会重复计算
    val minHashSignature: IntArray by lazy { computeMinHashDOPH(instances, isBinary) }

    val support: Int
        get() = instances.size

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is MyAtom) return false
        return relationId == other.relationId && entityId == other.entityId
    }

    override fun hashCode(): Int {
        // IMPORTANT: 注意不能使用简单的31 * relationId.hashCode() + entityId，很容易冲突
        return pairHash32(relationId.hashCode(), entityId)
    }

    override fun toString(): String {
        if (entityId == IdManager.getZId()) return ""
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

    fun getRuleString(): String = if (entityId == IdManager.getZId()) "" else IdManager.getAtomString(relationId, entityId)

    // 注意：inverse仅仅在validateH2B时调用，head和body同时取反，所以instances不用变
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
         */
        fun computeMinHashDOPH(instanceSet: Set<Int>, isBinary: Boolean): IntArray {
            if (instanceSet.isEmpty()) {
                throw IllegalArgumentException("Cannot compute MinHash for empty instance set")
            }

            val k = TLearn.MH_DIM
            val sig = IntArray(k) { Int.MAX_VALUE }
            require((k and (k - 1)) == 0) { "MH_DIM must be a power of 2" }
            val mask = k - 1

            // One pass: compute two 32-bit hashes for each element
            for (e in instanceSet) {
                val hBin = computeUnaryHash(e, OPH_SEED_BIN)
                val binId = pos32(hBin) and mask

                val hRank = computeUnaryHash(e, OPH_SEED_RANK) xor (binId * DOPH_SALT)
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
    fun getInstanceSet(): Set<Int> {
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
