package tarmorn.data

import tarmorn.Settings

/**
 * A triple represents a labeled edge in a knowledge graph.
 * Uses numeric IDs for better performance: entities (Int), relations (Int).
 */
data class Triple(
    val h: Int, // head entity ID
    val r: Int, // relation ID
    val t: Int // tail entity ID
) {
    fun getValue(ifHead: Boolean): Int = if (ifHead) h else t

    override fun toString(): String {
        val headStr = IdManager.getEntityString(h)
        val relStr = IdManager.getRelationString(r)
        val tailStr = IdManager.getEntityString(t)
        return "$headStr $relStr $tailStr"
    }

    fun equals(ifHead: Boolean, subject: Int, rel: Int, `object`: Int): Boolean {
        return if (ifHead) {
            h == subject && t == `object` && r == rel
        } else {
            h == `object` && t == subject && r == rel
        }
    }

    val confidence: Double get() = 1.0

    companion object {
        fun createTriple(h: String, r: String, t: String, reverse: Boolean = false): Triple {
            val headId = IdManager.getEntityId(h)
            val relId = IdManager.getRelationId(r)
            val tailId = IdManager.getEntityId(t)
            return if (reverse) Triple(tailId, relId, headId) else Triple(headId, relId, tailId)
        }

        fun createTriple(h: Int, r: Int, t: Int, reverse: Boolean = false): Triple {
            return if (reverse) Triple(t, r, h) else Triple(h, r, t)
        }
    }
}
