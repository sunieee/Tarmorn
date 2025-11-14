package tarmorn.structure.TLearn

import tarmorn.TLearn
import tarmorn.data.IdManager
import tarmorn.data.RelationPath

/**
 * MyAtom - represents an atom in formulas.
 * relationId: relation or relation-path id
 * entityId: Y for binary, X for loop, 0 for existence, >0 for constant entity id
 */
data class MyAtom(val relationId: Long, val entityId: Int) {
    override fun toString(): String {
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

    fun getRuleString(): String = IdManager.getAtomString(relationId, entityId)

    fun inverse() = MyAtom(RelationPath.getInverseRelation(relationId), entityId)

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

    // 获取当前原子的实例集合（用于精确验证），仅对L2原子有效
    fun getInstanceSet(): Set<Int> {
        require(isL1Atom) { "Instance set only available for L1 atoms" }

        return when {
            // Binary: r(X,Y)
            entityId == IdManager.getYId() ->
                TLearn.r2instanceSet[relationId] ?: emptySet()
            // Unary constant: r(X,c) -> 使用逆关系 r'(c,X) 的 tail 集合
            entityId > 0 -> {
                val inv = IdManager.getInverseRelation(relationId)
                TLearn.R2h2tSet[inv]?.get(entityId) ?: emptySet()
            }
            // Existence: r(X,·) -> 所有 head 实体
            entityId == 0 ->
                TLearn.R2h2tSet[relationId]?.keys ?: emptySet()
            // Loop: r(X,X) -> 自环实体集合
            entityId == IdManager.getXId() ->
                TLearn.r2loopSet[relationId] ?: emptySet()
            else -> emptySet()
        }
    }
}
