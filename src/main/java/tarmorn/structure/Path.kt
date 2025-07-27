package tarmorn.structure

import tarmorn.data.Triple
import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import java.util.*

/**
 * Represents a path in the knowledge graph.
 * Now uses a single Long to encode the entire relation path (up to 4 relations).
 * Path length is limited to 4 hops maximum.
 */
class Path(
    var entityNodes: IntArray,
    var relation: Long  // Encoded relation path
) {
    
    // Backward compatibility: construct from LongArray
    constructor(entityNodes: IntArray, relationNodes: LongArray) : this(
        entityNodes,
        RelationPath.encode(relationNodes)
    )
    
    // Get relationNodes for backward compatibility
    val relationNodes: LongArray
        get() = RelationPath.decode(relation)
    
    // Get string representation for compatibility
    val nodes: Array<String>
        get() {
            val relations = relationNodes
            val result = Array(entityNodes.size + relations.size) { "" }
            for (i in result.indices) {
                if (i % 2 == 0) {
                    result[i] = IdManager.getEntityString(entityNodes[i / 2])
                } else {
                    result[i] = IdManager.getRelationString(relations[i / 2])
                }
            }
            return result
        }
    
    override fun toString(): String {
        val relations = relationNodes
        val result = StringBuilder()
        for (i in entityNodes.indices) {
            if (i > 0) result.append(" -> ")
            result.append(IdManager.getEntityString(entityNodes[i]))
            
            if (i < relations.size) {
                result.append(" -> ")
                result.append(IdManager.getRelationString(relations[i]))
            }
        }
        return result.toString()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Path) return false
        return entityNodes.contentEquals(other.entityNodes) && 
               relation == other.relation
    }

    override fun hashCode(): Int {
        return Objects.hash(entityNodes.contentHashCode(), relation)
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
