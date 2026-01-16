package tarmorn.structure.TLearn

/**
 * DepFormula as a set-like container of up to three atoms; order-insensitive equality/hash.
 */
data class DepFormula(
    val atom1: DepAtom? = null,
    val atom2: DepAtom? = null,
    val atom3: DepAtom? = null
) {
    override fun hashCode(): Int {
        val atoms = listOfNotNull(atom1, atom2, atom3).sortedBy { it.hashCode() }
        return atoms.fold(0) { acc, atom -> acc xor atom.hashCode() }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DepFormula) return false
        // 正确的equals：比较排序后的原子列表
        val thisAtoms = listOfNotNull(atom1, atom2, atom3).sortedBy { it.hashCode() }
        val otherAtoms = listOfNotNull(other.atom1, other.atom2, other.atom3).sortedBy { it.hashCode() }
        return thisAtoms == otherAtoms
    }

    override fun toString(): String {
        return listOfNotNull(atom1, atom2, atom3).joinToString(" && ")
    }

    fun getRuleString(): String {
        return listOfNotNull(atom1, atom2, atom3).joinToString(" && ") { it.getRuleString() }
    }

    val isBinary: Boolean
        get() = atom1?.isBinary ?: false

    val size: Int
        get() = listOfNotNull(atom1, atom2, atom3).size
}
