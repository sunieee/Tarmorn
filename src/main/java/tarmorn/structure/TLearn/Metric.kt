package tarmorn.structure.TLearn

import tarmorn.Settings
import kotlin.math.min

/**
 * Metric describing support/coverage/confidence.
 */
data class Metric(
    var support: Double,
    val headSize: Int,
    val bodySize: Int,
) : Comparable<Metric> {
    val coverage: Double = if (headSize > 0) support / headSize else 0.0
    val confidence: Double = if (bodySize > 0) support / bodySize else 0.0

    val valid: Boolean
        get() = support >= Settings.MIN_SUPP && confidence > Settings.MIN_CONF // && coverage > 0.1

    val estimateValid: Boolean
        get() = support >= Settings.MIN_SUPP * tarmorn.TLearn.ESTIMATE_RATIO // && confidence > Settings.MIN_CONF * tarmorn.TLearn.ESTIMATE_RATIO // && coverage > 0.1 * tarmorn.TLearn.ESTIMATE_RATIO

    val needValidation: Boolean
        get() = support < Settings.MIN_SUPP * 2 || support > min(headSize, bodySize).toDouble()

    override fun toString(): String {
        return "{\"support\":$support, \"headSize\":$headSize, \"bodySize\":$bodySize, \"confidence\":$confidence}"
    }

    fun inverse() =  Metric(support, bodySize, headSize)

    fun betterThan(other: Metric) =
        this.confidence > other.confidence * 1.5

    fun estimateBetterThan(other: Metric) =
        this.confidence > other.confidence * 1.5 * tarmorn.TLearn.ESTIMATE_RATIO

    override fun compareTo(other: Metric): Int {
        // Sort by confidence descending (higher confidence first)
        return other.confidence.compareTo(this.confidence)
    }
}
