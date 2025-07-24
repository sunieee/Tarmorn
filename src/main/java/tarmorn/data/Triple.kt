package tarmorn.data

import tarmorn.Settings

/**
 * A triple represents a labeled edge a knowledge graph.
 *
 *
 */
class Triple(h: String, r: String, t: String) {
    var invalid: Boolean = false

    @JvmField
    val h: String // subject
    @JvmField
    val t: String // object
    @JvmField
    val r: String

    private var hash = 0

    init {
        if (h.length < 2 || t.length < 2) {
            System.err.println("the triple set you are trying to load contains constants of length 1 ... a constant (entity) needs to be described by at least two letters")
            System.err.println("ignoring: " + h + " " + r + " " + t)
            // System.exit(1);
            this.invalid = true
        }
        this.h = h
        this.r = r
        if (Settings.REWRITE_REFLEXIV && h == t) {
            this.t = Settings.REWRITE_REFLEXIV_TOKEN
        } else {
            this.t = t
        }
        hash = this.h.hashCode() + this.t.hashCode() + this.r.hashCode()
    }

    fun getValue(ifHead: Boolean): String {
        if (ifHead) return this.h
        else return this.t
    }

    override fun toString(): String {
        return this.h + " " + this.r + " " + this.t
    }

    override fun equals(that: Any?): Boolean {
        if (that is Triple) {
            val thatTriple = that
            if (this.h == thatTriple.h && this.t == thatTriple.t && this.r == thatTriple.r) {
                return true
            }
        }
        return false
    }

    override fun hashCode(): Int {
        return hash
    }

    fun equals(ifHead: Boolean, subject: String, rel: String, `object`: String): Boolean {
        if (ifHead) {
            return (this.h == subject && this.t == `object` && this.r == rel)
        } else {
            return (this.h == `object` && this.t == subject && this.r == rel)
        }
    }

    val confidence: Double
        get() = 1.0

    /**
     * Returns a string representation of this triples by replacing the constant by a variable wherever it appears
     *
     * @param constant The constant to be replaced.
     * @param variable The variable that is shown instead of the constant.
     *
     * @return The footprint of a triples that can be compared by equals against the atom in a AC1 rule.
     */
    fun getSubstitution(constant: String, variable: String): String {
        val tSub = if (this.t == constant) variable else this.t
        val hSub = if (this.h == constant) variable else this.h
        return this.r + "(" + hSub + "," + tSub + ")"
    }


    /**
     * Returns a string representation of this triples by replacing the constant by a variable wherever it appears and repalcing the other constant by a sedocn variable.
     *
     * @param constant The constant to be replaced.
     * @param variable The variable that is shown instead of the constant.
     * @param otherVariable The variable that is shown instead of the other constant.
     *
     * @return The footprint of a triples that can be compared by equals against the atom in a AC2 rule .
     */
    fun getSubstitution(constant: String, variable: String, otherVariable: String): String {
        var tSub = (if (this.t == constant) variable else this.t)!!
        var hSub = if (this.h == constant) variable else this.h
        if (tSub == variable) hSub = otherVariable
        if (hSub == variable) tSub = otherVariable
        return this.r + "(" + hSub + "," + tSub + ")"
    }


    companion object {
        fun createTriple(h: String, r: String, t: String, reverse: Boolean): Triple {
            if (reverse) {
                return Triple(t, r, h)
            } else {
                return Triple(h, r, t)
            }
        }
    }
}
