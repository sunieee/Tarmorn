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

    // Get the string representation of a relation ID.
    fun getRelationString(id: Long): String {
        return id2relation[id] ?: throw IllegalArgumentException("Unknown relation ID: $id")
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
    fun getEntityCount(): Int = entity2id.size - 26 // Subtract A-Z

    // Get the total number of relations.
    fun getRelationCount(): Int = relation2id.size

    // Get KG variable ID for specific letters.
    fun getXId(): Int = getEntityId("X")
    fun getYId(): Int = getEntityId("Y")
    fun getZId(): Int = getEntityId("Z")

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
        
        nextEntityId = 0
        nextRelationId = 0L
    }
}
