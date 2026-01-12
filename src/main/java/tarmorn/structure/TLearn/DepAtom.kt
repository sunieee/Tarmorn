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

    companion object {
        fun pairHash32(h: Int, t: Int): Int {
            val uH = h * -0x61c88647     // 0x9E3779B9 的补码（黄金比例常数）
            val uT = t * 0x85ebca6b.toInt()
            return uH xor Integer.rotateLeft(uT, 16)
        }
    }
}