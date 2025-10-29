package tarmorn.data

/**
 * Manages the mapping between string identifiers and numeric IDs for entities and relations.
 * Entities use Int IDs, relations use Long IDs for future expansion.
 * KG variables (A-Z) use negative IDs: A=-1, B=-2, ..., Z=-26
 */
object IdManager {
    // Entity management (Int IDs, starting from 0)
    private val entity2id = mutableMapOf<String, Int>()
    private val id2entity = mutableMapOf<Int, String>()
    private var nextEntityId = 1

    // Relation management (Long IDs)
    private val relation2id = mutableMapOf<String, Long>()
    private val id2relation = mutableMapOf<Long, String>()
    private var nextRelationId = 1L
    
    // Count of original relations (excluding inverse relations)
    public var originalRelationCount = 0L

    init {
        // Initialize KG variables A-Z with IDs -1 to -26
        ('A'..'Z').forEachIndexed { index, letter ->
            val id = -(index + 1)
            val letterStr = letter.toString()
            entity2id[letterStr] = id
            id2entity[id] = letterStr
        }
        entity2id["·"] = 0 // Special entity for existence
        id2entity[0] = "·" // Map existence entity to ID 0
    }

    /**
     * Get or create an entity ID for the given string.
     * Returns negative IDs for KG variables (A-Z).
     */
    fun getEntityId(entity: String): Int {
        return entity2id[entity] ?: run {
            val id = nextEntityId++
            entity2id[entity] = id
            id2entity[id] = entity
            id
        }
    }

    // Get or create a relation ID for the given string.
    fun getRelationId(relation: String): Long {
        return relation2id[relation] ?: run {
            val id = nextRelationId++
            relation2id[relation] = id
            id2relation[id] = relation
            id
        }
    }

    // Get the string representation of an entity ID.
    fun getEntityString(id: Int): String {
        return id2entity[id] ?: throw IllegalArgumentException("Unknown entity ID: $id")
    }

    // Get the string representation of a relation(path) ID.
    fun getRelationString(id: Long): String {
        if (id <= RelationPath.MAX_RELATION_ID) {
            return id2relation[id] ?: throw IllegalArgumentException("Unknown relation ID: $id")
        } else {
            val relations = RelationPath.decode(id)
            return relations.joinToString("·") { getRelationString(it) }
        }
    }

    // Check if an entity ID is a KG variable (A-Z).
    fun isVariable(id: Int): Boolean = id < 0
    fun isXYZ(id: Int): Boolean = id in arrayOf(getXId(), getYId(), getZId())

    // Check if an entity ID exists.
    fun hasEntity(id: Int): Boolean = id2entity.containsKey(id)

    // Check if a relation ID exists.
    fun hasRelation(id: Long): Boolean = id2relation.containsKey(id)

    // Get all entity IDs (excluding KG variables).
    fun getAllEntityIds(): Set<Int> = id2entity.keys.filter { it >= 0 }.toSet()

    // Get all relation IDs.
    fun getAllRelationIds(): Set<Long> = id2relation.keys

    // Get the total number of entities (excluding KG variables).
    fun getEntityCount(): Int = entity2id.size - 27 // Subtract A-Z and 0

    // Get the total number of relations.
    fun getRelationCount(): Int = relation2id.size

    // Add inverse relations for all existing relations
    fun addInverseRelations() {
        val originalRelations = relation2id.toMap() // Create a copy to avoid concurrent modification
        originalRelationCount = originalRelations.size.toLong()
        
        originalRelations.forEach { (relationName, relationId) ->
            // Only create inverse for original relations (not for existing inverse relations)
            if (!relationName.startsWith("INVERSE_")) {
                val inverseName = "INVERSE_$relationName"
                val inverseId = relationId + originalRelationCount
                relation2id[inverseName] = inverseId
                id2relation[inverseId] = inverseName
            }
        }
        
        // Update nextRelationId to continue from the highest ID
        nextRelationId = (id2relation.keys.maxOrNull() ?: 0L) + 1L
    }
    
    // Check if a relation is an inverse relation (much more efficient now!)
    fun isInverseRelation(relation: Long)= relation > originalRelationCount

    // Get the inverse relation ID for a given relation
    fun getInverseRelation(relation: Long) =
        relation + if (relation <= originalRelationCount) originalRelationCount else -originalRelationCount

    // Get KG variable ID for specific letters.
    fun getXId(): Int = getEntityId("X")
    fun getYId(): Int = getEntityId("Y")
    fun getZId(): Int = getEntityId("Z")

    // Convert unary atom to string representation
    fun UnaryAtomToString(r: Long, c: Int): String {
        return "${getRelationString(r)}(X, ${getEntityString(c)})"
    }

    // Convert binary atom to string representation
    fun BinaryAtomToString(r: Long): String {
        return "${getRelationString(r)}(X,Y)"
    }

    /**
     * Convert an atom (relation path + entityId) into a readable string.
     * Rules:
     * - If path length == 1: relation(X, entityString)
     * - If path length  > 1: r1(X,A), r2(A,B), ..., rN(B, entityString)
     * - If any relation is an inverse, convert to its forward name and swap arguments.
     */
    fun getAtomString(relationId: Long, entityId: Int): String {
        // Resolve terminal argument
        fun termString(eid: Int): String = when (eid) {
            getYId() -> "Y"
            getXId() -> "X"
            0 -> "·"
            else -> getEntityString(eid)
        }

        // Decode relation path
        val relations: LongArray = if (relationId <= RelationPath.MAX_RELATION_ID) longArrayOf(relationId) else RelationPath.decode(relationId)
        val n = relations.size
        val tailTerm = termString(entityId)

        // Build node list: X, A, B, ..., tailTerm
        val nodes = Array(n + 1) { "" }
        nodes[0] = "X"
        for (i in 1 until n+1) {
            nodes[i] = ('A'.code + (i - 1)).toChar().toString()
        }
        if (tailTerm != "·") nodes[n] = tailTerm
        val parts = ArrayList<String>(n)
        for (i in 0 until n) {
            val r = relations[i]
            val inv = isInverseRelation(r)
            val forward = if (inv) getInverseRelation(r) else r
            val name = getRelationString(forward)
            // swap args for inverse
            val left = if (inv) nodes[i + 1] else nodes[i]
            val right = if (inv) nodes[i] else nodes[i + 1]
            parts.add("$name($left,$right)")
        }
        return parts.joinToString(", ")
    }

    // Clear all mappings except KG variables (useful for testing).
    fun clear() {
        // Preserve KG variables A-Z
        val kgVariables = ('A'..'Z').associate { letter ->
            letter.toString() to entity2id[letter.toString()]!!
        }
        
        entity2id.clear()
        id2entity.clear()
        relation2id.clear()
        id2relation.clear()
        
        // Restore KG variables
        kgVariables.forEach { (letter, id) ->
            entity2id[letter] = id
            id2entity[id] = letter
        }
        
        nextEntityId = 1
        nextRelationId = 1L
        originalRelationCount = 0L
    }

    fun getAverageEntityStringLength(): Double {
        return if (entity2id.isEmpty()) 0.0
        else entity2id.keys.sumOf { it.length }.toDouble() / entity2id.size
    }
    
    fun getAverageRelationStringLength(): Double {
        return if (relation2id.isEmpty()) 0.0
        else relation2id.keys.sumOf { it.length }.toDouble() / relation2id.size
    }
}
