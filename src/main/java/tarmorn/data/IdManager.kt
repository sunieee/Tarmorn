package tarmorn.data

/**
 * Manages the mapping between string identifiers and numeric IDs for entities and relations.
 * Entities use Int IDs, relations use Int IDs for future expansion.
 * KG variables (A-Z) use negative IDs: A=-1, B=-2, ..., Z=-26
 */
object IdManager {
    // Entity management (Int IDs, starting from 0)
    private val entityToId = mutableMapOf<String, Int>()
    private val idToEntity = mutableMapOf<Int, String>()
    private var nextEntityId = 0

    // Relation management (Int IDs)
    private val relationToId = mutableMapOf<String, Int>()
    private val idToRelation = mutableMapOf<Int, String>()
    private var nextRelationId = 0

    init {
        // Initialize KG variables A-Z with IDs -1 to -26
        ('A'..'Z').forEachIndexed { index, letter ->
            val id = -(index + 1)
            val letterStr = letter.toString()
            entityToId[letterStr] = id
            idToEntity[id] = letterStr
        }
    }

    /**
     * Get or create an entity ID for the given string.
     * Returns negative IDs for KG variables (A-Z).
     */
    fun getEntityId(entity: String): Int {
        return entityToId[entity] ?: run {
            val id = nextEntityId++
            entityToId[entity] = id
            idToEntity[id] = entity
            id
        }
    }

    /**
     * Get or create a relation ID for the given string.
     */
    fun getRelationId(relation: String): Int {
        return relationToId[relation] ?: run {
            val id = nextRelationId++
            relationToId[relation] = id
            idToRelation[id] = relation
            id
        }
    }

    /**
     * Get the string representation of an entity ID.
     */
    fun getEntityString(id: Int): String {
        return idToEntity[id] ?: throw IllegalArgumentException("Unknown entity ID: $id")
    }

    /**
     * Get the string representation of a relation ID.
     */
    fun getRelationString(id: Int): String {
        return idToRelation[id] ?: throw IllegalArgumentException("Unknown relation ID: $id")
    }

    /**
     * Check if an entity ID is a KG variable (A-Z).
     */
    fun isKGVariable(id: Int): Boolean = id < 0

    /**
     * Check if an entity string is a KG variable (A-Z).
     */
    fun isKGVariable(entity: String): Boolean = entity.length == 1 && entity[0] in 'A'..'Z'

    /**
     * Check if an entity ID exists.
     */
    fun hasEntity(id: Int): Boolean = idToEntity.containsKey(id)

    /**
     * Check if a relation ID exists.
     */
    fun hasRelation(id: Int): Boolean = idToRelation.containsKey(id)

    /**
     * Get all entity IDs (excluding KG variables).
     */
    fun getAllEntityIds(): Set<Int> = idToEntity.keys.filter { it >= 0 }.toSet()

    /**
     * Get all relation IDs.
     */
    fun getAllRelationIds(): Set<Int> = idToRelation.keys

    /**
     * Get the total number of entities (excluding KG variables).
     */
    fun getEntityCount(): Int = entityToId.size - 26 // Subtract A-Z

    /**
     * Get the total number of relations.
     */
    fun getRelationCount(): Int = relationToId.size

    /**
     * Get KG variable ID for specific letters.
     */
    fun getXId(): Int = getEntityId("X")
    fun getYId(): Int = getEntityId("Y")
    fun getZId(): Int = getEntityId("Z")

    /**
     * Clear all mappings except KG variables (useful for testing).
     */
    fun clear() {
        // Preserve KG variables A-Z
        val kgVariables = ('A'..'Z').associate { letter ->
            letter.toString() to entityToId[letter.toString()]!!
        }
        
        entityToId.clear()
        idToEntity.clear()
        relationToId.clear()
        idToRelation.clear()
        
        // Restore KG variables
        kgVariables.forEach { (letter, id) ->
            entityToId[letter] = id
            idToEntity[id] = letter
        }
        
        nextEntityId = 0
        nextRelationId = 0
    }
}
