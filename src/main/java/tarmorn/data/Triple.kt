package tarmorn.data

import tarmorn.Settings

/**
 * A triple represents a labeled edge in a knowledge graph.
 */
data class Triple(
    val h: String, // subject
    val r: String,
    val t: String // object
) {
    init {
        require(h.length >= 2 && t.length >= 2) {
            "Constants (entities) need to be described by at least two letters. Ignoring: $h $r $t"
        }
    }

    fun getValue(ifHead: Boolean): String = if (ifHead) h else t

    override fun toString(): String = "$h $r $t"

    fun equals(ifHead: Boolean, subject: String, rel: String, `object`: String): Boolean {
        return if (ifHead) {
            h == subject && t == `object` && r == rel
        } else {
            h == `object` && t == subject && r == rel
        }
    }

    val confidence: Double get() = 1.0

    /**
     * Returns a string representation of this triple by replacing the constant by a variable wherever it appears
     */
    fun getSubstitution(constant: String, variable: String): String {
        val tSub = if (t == constant) variable else t
        val hSub = if (h == constant) variable else h
        return "$r($hSub,$tSub)"
    }

    /**
     * Returns a string representation of this triple by replacing the constant by a variable wherever it appears 
     * and replacing the other constant by a second variable.
     */
    fun getSubstitution(constant: String, variable: String, otherVariable: String): String {
        var tSub = if (t == constant) variable else t
        var hSub = if (h == constant) variable else h
        
        when {
            tSub == variable -> hSub = otherVariable
            hSub == variable -> tSub = otherVariable
        }
        
        return "$r($hSub,$tSub)"
    }

    companion object {
        fun createTriple(h: String, r: String, t: String, reverse: Boolean = false): Triple {
            return if (reverse) Triple(t, r, h) else Triple(h, r, t)
        }
    }
}
