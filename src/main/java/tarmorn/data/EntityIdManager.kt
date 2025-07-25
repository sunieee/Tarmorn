package tarmorn.data

/**
 * Entity and Relation ID Manager for performance optimization
 * Maps strings to integer IDs to improve lookup performance and reduce memory usage
 */
class EntityIdManager {
    private val entityToId = mutableMapOf<String, Int>()
    private val idToEntity = mutableMapOf<Int, String>()
    private val relationToId = mutableMapOf<String, Int>()
    private val idToRelation = mutableMapOf<Int, String>()
    
    private var nextEntityId = 0
    private var nextRelationId = 0
    
    fun getEntityId(entity: String): Int {
        return entityToId.getOrPut(entity) {
            val id = nextEntityId++
            idToEntity[id] = entity
            id
        }
    }
    
    fun getRelationId(relation: String): Int {
        return relationToId.getOrPut(relation) {
            val id = nextRelationId++
            idToRelation[id] = relation
            id
        }
    }
    
    fun getEntity(id: Int): String? = idToEntity[id]
    fun getRelation(id: Int): String? = idToRelation[id]
    
    // Public read-only access for compatibility
    fun hasEntity(entity: String): Boolean = entity in entityToId
    fun hasRelation(relation: String): Boolean = relation in relationToId
    fun getEntityIdOrNull(entity: String): Int? = entityToId[entity]
    fun getRelationIdOrNull(relation: String): Int? = relationToId[relation]
    
    val entityCount: Int get() = nextEntityId
    val relationCount: Int get() = nextRelationId
    
    fun clear() {
        entityToId.clear()
        idToEntity.clear()
        relationToId.clear()
        idToRelation.clear()
        nextEntityId = 0
        nextRelationId = 0
    }
}
