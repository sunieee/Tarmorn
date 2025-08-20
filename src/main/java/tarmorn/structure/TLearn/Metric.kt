package tarmorn.structure.TLearn

import kotlin.math.min

/**
 * Metric describing jaccard/support/coverage/confidence.
 */
data class Metric(
    val jaccard: Double,
    var support: Double,
    val headSize: Int,
    val bodySize: Int,
) {
    val coverage: Double = support / headSize
    val confidence: Double = support / bodySize

    val valid: Boolean
        get() = support >= tarmorn.TLearn.MIN_SUPP && coverage > 0.1 && confidence > 0.1

    val needValidation: Boolean
        get() = support < tarmorn.TLearn.MIN_SUPP * 2 || support > min(headSize, bodySize).toDouble()

    override fun toString(): String {
        return "{\"jaccard\": $jaccard, \"support\":$support, \"headSize\":$headSize, \"bodySize\":$bodySize, \"confidence\":$confidence}"
    }

    fun inverse() =  Metric(jaccard, support, bodySize, headSize)

    fun betterThan(other: Metric) =
        this.jaccard > other.jaccard && this.confidence >= other.confidence
}
