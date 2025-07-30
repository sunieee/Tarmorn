package tarmorn.data

/**
 * Utility class for encoding and decoding multiple relations into a single Long value.
 * Uses 15 bits per relation, supporting up to 4 relations per path.
 * 
 * Bit layout (63 bits total):
 * - Bits 0-14: Last relation (r % 2^15)
 * - Bits 15-29: Third relation 
 * - Bits 30-44: Second relation
 * - Bits 45-59: First relation
 * - Bits 60-62: Unused
 */
object RelationPath {
    private const val BITS_PER_RELATION = 15
    private const val MAX_RELATION_ID = (1L shl BITS_PER_RELATION) - 1 // 2^15 - 1 = 32767
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
        
        // Encode from last to first (reverse order for easier access)
        for (i in relations.indices) {
            val relation = relations[relations.size - 1 - i]
            encoded = encoded or (relation shl (i * BITS_PER_RELATION))
        }
        
        return encoded
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

        // Shift existing path left and add new relation at the end (becomes first in logical order)
        return (existingPath shl BITS_PER_RELATION) or (relation and RELATION_MASK)
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

        // Add relation at the lowest bits (becomes last in logical order)
        return (existingPath shl BITS_PER_RELATION) or (relation and RELATION_MASK)
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
        
        // Extract relations from last to first
        for (i in 0 until 4) {
            val relation = (encoded shr (i * BITS_PER_RELATION)) and RELATION_MASK
            if (relation != 0L) {
                relations.add(0, relation) // Add to beginning to maintain order
            }
        }
        
        return relations.toLongArray()
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
        val relations = decode(encoded)
        return if (relations.isNotEmpty()) relations[0] else 0L
    }
    
    /**
     * Get the last relation in the path.
     */
    fun getLastRelation(encoded: Long): Long {
        return encoded and RELATION_MASK
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
    
    /**
     * Convert to string representation for debugging.
     */
    fun toString(encoded: Long): String {
        val relations = decode(encoded)
        return relations.joinToString(" -> ") { IdManager.getRelationString(it) }
    }
}