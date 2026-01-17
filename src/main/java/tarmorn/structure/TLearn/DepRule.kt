package tarmorn.structure.TLearn

import tarmorn.Settings
import tarmorn.data.IdManager
import kotlin.math.ln

/**
 * DepRule wraps a head/body DepAtom with its metric for prediction.
 */
data class DepRule(
    val headAtom: DepAtom,
    val bodyAtom: DepAtom,
    val metric: Metric
) {
    init {
        require(headAtom.entityId != 0) { "Head atom cannot be existence (*)" }
    }
    val relationId: Long
        get() = headAtom.relationId

    val realConfidence: Double
        get() = metric.confidence

    val realSurprisal: Double
        get() = metric.surprisal

    val adjustedConfidence: Double
        get() = metric.support / (metric.bodySize + Settings.UNSEEN_NEGATIVE_EXAMPLES)

    val adjustedSurprisal: Double
        get() {
            val conf = adjustedConfidence
            return if (conf < 1.0) {
                minOf(-ln(1 - conf), Settings.MAX_SURPRISAL)
            } else {
                Settings.MAX_SURPRISAL
            }
        }

    private val isBinaryHead: Boolean
        get() = headAtom.entityId == IdManager.getYId()

    private val isLoopHead: Boolean
        get() = headAtom.entityId == IdManager.getXId()

    private val isInverseHeadRelation: Boolean
        get() = IdManager.isInverseRelation(headAtom.relationId)

    private val isConstantHead: Boolean
        get() = !isBinaryHead && !isLoopHead

    /**
     * Predict tail candidates given head entity.
     */
    fun predictTail(headEntity: Int): Set<Int> {
        return when {
            isBinaryHead -> bodyAtom.materialize(headEntity, true)
            isLoopHead -> {
                val holds = bodyAtom.materialize().contains(headEntity)
                if (holds) setOf(headEntity) else emptySet()
            }
            isConstantHead && isInverseHeadRelation -> {
                // B: r(c,x) <- R(x,d)
                if (headEntity != headAtom.entityId) emptySet() else bodyAtom.materialize()
            }
            isConstantHead -> {
                // C: r(x,c) <- R(x,d)
                val tailConst = headAtom.entityId
                val holds = bodyAtom.materialize().contains(headEntity)
                if (holds) setOf(tailConst) else emptySet()
            }
            else -> emptySet()
        }
    }

    /**
     * Predict head candidates given tail entity.
     */
    fun predictHead(tailEntity: Int): Set<Int> {
        return when {
            isBinaryHead -> bodyAtom.materialize(tailEntity, false)
            isLoopHead -> {
                val holds = bodyAtom.materialize().contains(tailEntity)
                if (holds) setOf(tailEntity) else emptySet()
            }
            isConstantHead && isInverseHeadRelation -> {
                // B: r(c,x) <- R(x,d)
                val holds = bodyAtom.materialize().contains(tailEntity)
                if (holds) setOf(headAtom.entityId) else emptySet()
            }
            isConstantHead -> {
                // C: r(x,c) <- R(x,d)
                if (tailEntity != headAtom.entityId) emptySet() else bodyAtom.materialize()
            }
            else -> emptySet()
        }
    }
}