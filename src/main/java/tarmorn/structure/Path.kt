package tarmorn.structure

import tarmorn.data.Triple
import java.util.*

class Path(var nodes: Array<String>, var markers: CharArray) {
    
    override fun toString(): String {
        return nodes.indices.joinToString(" -> ") { markedNodeToString(it) }
    }

    private fun markedNodeToString(i: Int): String = 
        if (i % 2 == 1) "${markers[(i - 1) / 2]}${nodes[i]}" else nodes[i]

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Path) return false
        return nodes.contentEquals(other.nodes) && markers.contentEquals(other.markers)
    }

    override fun hashCode(): Int {
        return Objects.hash(nodes.contentHashCode(), markers.contentHashCode())
    }

    /**
     * Checks if a path is valid for strict object identity.
     *
     * @return False, if the x and y values appear at the wrong position in the path, or if the
     * same entities appears several times in the body part of the path.
     */
    val isValid: Boolean
        get() {
            val xconst = nodes[0]
            val yconst = nodes[2]
            
            // Check if x or y values appear at wrong positions in body part
            for (i in 4 until nodes.size - 2 step 2) {
                if (nodes[i] == xconst || nodes[i] == yconst) {
                    return false
                }
            }
            
            // Check for duplicate entities (only even indices are entities)
            val visitedEntities = mutableSetOf<String>()
            for (i in 2 until nodes.size step 2) {
                if (!visitedEntities.add(nodes[i])) {
                    return false
                }
            }
            
            return true
        }

    /**
     * Checks if a path is non cyclic, i.e, does not connect the entities of the given triple.
     */
    fun isNonCyclic(t: Triple): Boolean {
        return (4 until nodes.size step 2).none { i ->
            nodes[i] == t.h || nodes[i] == t.t
        }
    }

    /**
     * Checks if the path will result in a cyclic rule.
     *
     * @return True, if its a cyclic path.
     */
    val isCyclic: Boolean
        get() = nodes.last() == nodes[0] || nodes.last() == nodes[2]
}
