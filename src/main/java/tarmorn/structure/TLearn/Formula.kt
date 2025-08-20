package tarmorn.structure.TLearn

/**
 * Formula as a set-like container of up to three atoms; order-insensitive equality/hash.
 */
data class Formula(
    val atom1: MyAtom? = null,
    val atom2: MyAtom? = null,
    val atom3: MyAtom? = null
) {
    override fun hashCode(): Int {
        val atoms = listOfNotNull(atom1, atom2, atom3).sortedBy { it.hashCode() }
        return atoms.fold(0) { acc, atom -> acc xor atom.hashCode() }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Formula) return false
        return this.hashCode() == other.hashCode()
    }

    override fun toString(): String {
        return listOfNotNull(atom1, atom2, atom3).joinToString(" & ")
    }
}
