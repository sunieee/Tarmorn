package tarmorn.data

/**
 * A triple represents a labeled edge in a knowledge graph.
 * Uses numeric IDs for better performance: entities (Int), relations (Long).
 */
data class MyTriple(
    val h: Int, // head entity ID
    val r: Long, // relation ID
    val t: Int // tail entity ID
) {
    fun getValue(ifHead: Boolean): Int = if (ifHead) h else t

    override fun toString(): String {
        val headStr = IdManager.getEntityString(h)
        val relStr = IdManager.getRelationString(r)
        val tailStr = IdManager.getEntityString(t)
        return "$headStr $relStr $tailStr"
    }

    fun equals(ifHead: Boolean, subject: Int, rel: Long, `object`: Int): Boolean {
        return if (ifHead) {
            h == subject && t == `object` && r == rel
        } else {
            h == `object` && t == subject && r == rel
        }
    }

    val confidence: Double get() = 1.0

    // Constructor from string values (for backward compatibility)
    constructor(left: String, relation: String, right: String) : this(
        IdManager.getEntityId(left),
        IdManager.getRelationId(relation),
        IdManager.getEntityId(right)
    )
}
