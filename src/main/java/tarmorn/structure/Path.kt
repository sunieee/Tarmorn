package tarmorn.structure

import tarmorn.data.Triple
import tarmorn.data.IdManager
import java.util.*

/**
 * Represents a path in the knowledge graph.
 * With inverse relations, all relations are represented uniformly in positive direction.
 * No markers needed since reverse relations are handled by inverse relation triples.
 */
class Path(
    var entityNodes: IntArray,
    var relationNodes: LongArray
) {
    
    // Get string representation for compatibility
    val nodes: Array<String>
        get() {
            val result = Array(entityNodes.size + relationNodes.size) { "" }
            for (i in result.indices) {
                if (i % 2 == 0) {
                    result[i] = IdManager.getEntityString(entityNodes[i / 2])
                } else {
                    result[i] = IdManager.getRelationString(relationNodes[i / 2])
                }
            }
            return result
        }
    
    override fun toString(): String {
        val result = StringBuilder()
        for (i in entityNodes.indices) {
            if (i > 0) result.append(" -> ")
            result.append(IdManager.getEntityString(entityNodes[i]))
            
            if (i < relationNodes.size) {
                result.append(" -> ")
                result.append(IdManager.getRelationString(relationNodes[i]))
            }
        }
        return result.toString()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Path) return false
        return entityNodes.contentEquals(other.entityNodes) && 
               relationNodes.contentEquals(other.relationNodes)
    }

    override fun hashCode(): Int {
        return Objects.hash(entityNodes.contentHashCode(), relationNodes.contentHashCode())
    }

    /**
     * Checks if a path is valid for strict object identity.
     * 
     * @return False, if the x and y values appear at the wrong position in the path, or if the
     * same entities appears several times in the body part of the path.
     */
    val isValid: Boolean
        get() {
            val xconst = entityNodes[0]
            val yconst = entityNodes[1]
            
            // Check if x or y values appear at wrong positions in body part
            for (i in 2 until entityNodes.size - 1) {
                if (entityNodes[i] == xconst || entityNodes[i] == yconst) {
                    return false
                }
            }
            
            // Check for duplicate entities (start from index 1, skip head entity)
            val visitedEntities = mutableSetOf<Int>()
            for (i in 1 until entityNodes.size) {
                if (!visitedEntities.add(entityNodes[i])) {
                    return false
                }
            }
            
            return true
        }

    /**
     * Checks if a path is non cyclic, i.e, does not connect the entities of the given triple.
     */
    fun isNonCyclic(t: Triple): Boolean {
        return (2 until entityNodes.size - 1).none { i ->
            entityNodes[i] == t.h || entityNodes[i] == t.t
        }
    }

    /**
     * Checks if the path will result in a cyclic rule.
     * 
     * @return True, if its a cyclic path.
     */
    val isCyclic: Boolean
        get() = entityNodes.last() == entityNodes[0] || entityNodes.last() == entityNodes[1]
}
