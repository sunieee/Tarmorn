package tarmorn.data

import javax.management.relation.Relation

/**
 * Utility class for encoding and decoding multiple relations into a single Long value.
 * Uses 15 bits per relation, supporting up to 4 relations per path.
 * 
 * Bit layout (63 bits total):
 * - Bits 0-14: First relation (r1)
 * - Bits 15-29: Second relation (r2)
 * - Bits 30-44: Third relation (r3)
 * - Bits 45-59: Fourth/Last relation (r4)
 * - Bits 60-62: Unused
 */
object RelationPath {
    private const val BITS_PER_RELATION = 15
    const val MAX_RELATION_ID = (1L shl BITS_PER_RELATION) - 1 // 2^15 - 1 = 32767
    const val MAX_L2RELATION_ID = (1L shl (BITS_PER_RELATION * 2)) - 1 // 2^30 - 1 = 1073741823
    private const val RELATION_MASK = MAX_RELATION_ID
    
    /**
     * Encode multiple relations into a single Long value.
     * Relations are stored from first to last, with unused positions set to 0.
     * 
     * @param relations Array of relation IDs (max 4 relations)
     * @return Encoded Long value
     */
    fun encode(relations: LongArray): Long {
        require(relations.size <= 4) { "Maximum 4 relations supported, got ${relations.size}" }
        require(relations.all { it in 1..MAX_RELATION_ID }) { 
            "All relation IDs must be between 1 and $MAX_RELATION_ID" 
        }
        
        var encoded = 0L
        
        // Encode directly from first to last (natural order)
        for (i in relations.indices) {
            encoded = encoded or (relations[i] shl (i * BITS_PER_RELATION))
        }
        
        return encoded
    }

    fun isL1Relation(rp: Long): Boolean {
        return rp in 1..MAX_RELATION_ID
    }

    /**
     * Connect a single relation to the head of an existing path.
     * Used when the first parameter is a single relation (< MAX_RELATION_ID).
     * Creates: relation -> existing_path
     */
    fun connectHead(relation: Long, existingPath: Long): Long {
        require(relation in 1..MAX_RELATION_ID) {
            "Relation ID must be between 1 and $MAX_RELATION_ID, got $relation"
        }

        val existingLength = getLength(existingPath)
        require(existingLength < 4) { 
            "Maximum 4 relations supported, existing path already has $existingLength relations" 
        }

        // Shift existing path left by 15 bits and put new relation at lowest bits
        return (existingPath shl BITS_PER_RELATION) or relation
    }
    
    /**
     * Connect a single relation to the tail of an existing path.
     * Used when the second parameter is a single relation (< MAX_RELATION_ID).
     * Creates: existing_path -> relation
     */
    fun connectTail(existingPath: Long, relation: Long): Long {
        require(relation in 1..MAX_RELATION_ID) {
            "Relation ID must be between 1 and $MAX_RELATION_ID, got $relation"
        }

        val existingLength = getLength(existingPath)
        require(existingLength < 4) { 
            "Maximum 4 relations supported, existing path already has $existingLength relations" 
        }

        // Put new relation at the position after existing relations
        return existingPath or (relation shl (existingLength * BITS_PER_RELATION))
    }

    @Deprecated("Use connectHead or connectTail instead", ReplaceWith("connectHead(rp, relation)"))
    fun connect(rp: Long, relation: Long): Long {
        return connectHead(rp, relation)
    }
    
    /**
     * Encode a single relation into a Long value.
     */
    fun encode(relation: Long): Long {
        require(relation in 1..MAX_RELATION_ID) { 
            "Relation ID must be between 1 and $MAX_RELATION_ID, got $relation" 
        }
        return relation
    }
    
    /**
     * Decode a Long value into an array of relation IDs.
     * Returns only non-zero relations (actual path length may be less than 4).
     * 
     * @param encoded Encoded Long value
     * @return Array of relation IDs in order (first to last)
     */
    fun decode(encoded: Long): LongArray {
        val relations = mutableListOf<Long>()
        
        // Extract relations directly from first to last (natural order)
        for (i in 0 until 4) {
            val relation = (encoded shr (i * BITS_PER_RELATION)) and RELATION_MASK
            if (relation != 0L) {
                relations.add(relation) // Direct append, no need to insert at beginning
            }
        }
        
        return relations.toLongArray()
    }

    fun hasInverseRelation(encoded: Long): Boolean {
        val relations = decode(encoded)
        // 存在相邻关系是逆关系则返回 true
        for (i in 0 until relations.size - 1) {
            if (IdManager.getInverseRelation(relations[i]) == relations[i + 1])
                return true
        }
        return false
    }
    
    /**
     * Get the length of the relation path (number of non-zero relations).
     */
    fun getLength(encoded: Long): Int {
        var count = 0
        for (i in 0 until 4) {
            val relation = (encoded shr (i * BITS_PER_RELATION)) and RELATION_MASK
            if (relation != 0L) count++
        }
        return count
    }
    
    /**
     * Get the first relation in the path.
     */
    fun getFirstRelation(encoded: Long): Long {
        return encoded and RELATION_MASK // First relation is now at lowest bits
    }
    
    /**
     * Get the last relation in the path.
     */
    fun getLastRelation(encoded: Long): Long {
        val length = getLength(encoded)
        if (length == 0) return 0L
        
        // Last relation is at position (length-1) * BITS_PER_RELATION
        return (encoded shr ((length - 1) * BITS_PER_RELATION)) and RELATION_MASK
    }
    
    /**
     * Get a specific relation by index (0-based, first to last).
     */
    fun getRelation(encoded: Long, index: Int): Long {
        require(index in 0..3) { "Index must be between 0 and 3, got $index" }
        val relations = decode(encoded)
        return if (index < relations.size) relations[index] else 0L
    }
    
    /**
     * Remove the first relation from the path, returning the remaining path.
     * Used for creating rule body from path by removing head relation.
     */
    fun removeFirstRelation(encoded: Long): Long {
        val relations = decode(encoded)
        return if (relations.size <= 1) {
            0L // Empty path
        } else {
            encode(relations.sliceArray(1 until relations.size))
        }
    }
    
    /**
     * Check if this is a single relation (length = 1).
     */
    fun isSingleRelation(encoded: Long): Boolean = getLength(encoded) == 1
    
    /**
     * Get the inverse of a relation path.
     * For single relations (rp <= MAX_RELATION_ID), uses IdManager.getInverseRelation.
     * For relation paths (rp > MAX_RELATION_ID), decomposes into r1, ..., rm 
     * and returns rm', ..., r1' where ' denotes inverse relation.
     */
    fun getInverseRelation(encoded: Long): Long {
        if (encoded <= MAX_RELATION_ID) {
            // Single relation case
            return IdManager.getInverseRelation(encoded)
        } else {
            // Relation path case: decompose and reverse with inverses
            val relations = decode(encoded)
            val inverseRelations = relations.reversed().map { 
                IdManager.getInverseRelation(it) 
            }.toLongArray()
            return encode(inverseRelations)
        }
    }
}
