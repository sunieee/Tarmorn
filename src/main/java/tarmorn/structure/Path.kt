package tarmorn.structure

import tarmorn.data.Triple
import tarmorn.data.IdManager
import java.util.*

class Path {
    var entityNodes: IntArray
    var relationNodes: IntArray
    var markers: CharArray
    
    // Constructor for string-based paths (for backward compatibility)
    constructor(nodes: Array<String>, markers: CharArray) {
        this.markers = markers
        val numEntities = (nodes.size + 1) / 2
        val numRelations = nodes.size / 2
        
        this.entityNodes = IntArray(numEntities)
        this.relationNodes = IntArray(numRelations)
        
        for (i in nodes.indices) {
            if (i % 2 == 0) { // entity
                this.entityNodes[i / 2] = IdManager.getEntityId(nodes[i])
            } else { // relation
                this.relationNodes[i / 2] = IdManager.getRelationId(nodes[i])
            }
        }
    }
    
    // Constructor for ID-based paths
    constructor(entityNodes: IntArray, relationNodes: IntArray, markers: CharArray) {
        this.entityNodes = entityNodes
        this.relationNodes = relationNodes
        this.markers = markers
    }
    
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
        val nodeStrings = nodes
        return nodeStrings.indices.joinToString(" -> ") { markedNodeToString(it, nodeStrings) }
    }

    private fun markedNodeToString(i: Int, nodeStrings: Array<String>): String = 
        if (i % 2 == 1) "${markers[(i - 1) / 2]}${nodeStrings[i]}" else nodeStrings[i]

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Path) return false
        return entityNodes.contentEquals(other.entityNodes) && 
               relationNodes.contentEquals(other.relationNodes) && 
               markers.contentEquals(other.markers)
    }

    override fun hashCode(): Int {
        return Objects.hash(entityNodes.contentHashCode(), relationNodes.contentHashCode(), markers.contentHashCode())
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
