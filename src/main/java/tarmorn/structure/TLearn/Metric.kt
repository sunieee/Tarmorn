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
    init {
        // 验证参数合理性
        require(support >= 0) { "Support must be non-negative: $support" }
        require(headSize >= 0) { "HeadSize must be non-negative: $headSize" }
        require(bodySize >= 0) { "BodySize must be non-negative: $bodySize" }
        // 警告：如果 support > bodySize，可能有问题
        if (support > bodySize) {
            println("[Metric] Warning: support ($support) > bodySize ($bodySize), confidence will be capped at ~1.0")
        }
    }
    
    val coverage: Double = if (headSize > 0) support / headSize else 0.0
    val confidence: Double = if (bodySize > 0) {
        val conf = support / (bodySize + Settings.NUM_UNSEEN)
        // 确保 confidence 永远小于 1.0，避免浮点数精度问题
        min(conf, 0.9999999)
    } else 0.0

    val surprisal: Double
        get() = if (confidence < 1.0) -Math.log(1 - confidence) else Double.MAX_VALUE

    val valid: Boolean
        get() = support >= Settings.MIN_SUPP && confidence > Settings.MIN_CONF // && coverage > 0.1

    val needValidation: Boolean
        get() = support < Settings.MIN_SUPP * 2 || support > min(headSize, bodySize).toDouble()

    override fun toString(): String {
        return "{\"support\":$support, \"headSize\":$headSize, \"bodySize\":$bodySize, \"confidence\":$confidence}"
    }

    fun inverse() =  Metric(support, bodySize, headSize)

    override fun compareTo(other: Metric): Int {
        // Sort by confidence descending (higher confidence first)
        return other.confidence.compareTo(this.confidence)
    }
}
