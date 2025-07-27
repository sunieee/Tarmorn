package tarmorn.test

import tarmorn.data.*

fun main() {
    println("=== Testing getTriplesByEntity Method ===")
    
    // Clear IdManager state
    IdManager.clear()
    
    // Create a simple test dataset
    val testData = """
        alice	knows	bob
        charlie	likes	bob
        bob	works_at	company
    """.trimIndent()
    
    // Write test data to a temporary file
    val tempFile = java.io.File.createTempFile("test_entity_triples", ".txt")
    tempFile.writeText(testData)
    tempFile.deleteOnExit()
    
    // Create TripleSet
    val tripleSet = TripleSet(tempFile.absolutePath, true)
    
    println("Original triples:")
    tripleSet.forEach { println("  $it") }
    
    // Get IDs
    val aliceId = IdManager.getEntityId("alice")
    val bobId = IdManager.getEntityId("bob")
    val charlieId = IdManager.getEntityId("charlie")
    val companyId = IdManager.getEntityId("company")
    
    // Test each entity
    println("\n=== Testing getTriplesByEntity ===")
    
    listOf(
        "alice" to aliceId,
        "bob" to bobId,
        "charlie" to charlieId,
        "company" to companyId
    ).forEach { (name, id) ->
        println("\nTriples for $name:")
        val entityTriples = tripleSet.getTriplesByEntity(id)
        entityTriples.forEach { println("  $it") }
        
        when (name) {
            "alice" -> println("  Expected: alice knows bob")
            "bob" -> println("  Expected: bob works_at company, bob INVERSE_knows alice, bob INVERSE_likes charlie")
            "charlie" -> println("  Expected: charlie likes bob")
            "company" -> println("  Expected: company INVERSE_works_at bob")
        }
    }
    
    println("\n=== Verification ===")
    val bobTriples = tripleSet.getTriplesByEntity(bobId)
    val hasOutgoing = bobTriples.any { it.h == bobId && !IdManager.isInverseRelation(it.r) }
    val hasIncoming = bobTriples.any { it.h == bobId && IdManager.isInverseRelation(it.r) }
    
    println("Bob has outgoing relations: $hasOutgoing")
    println("Bob has incoming relations (as inverse): $hasIncoming")
    println("Total relations for Bob: ${bobTriples.size}")
    
    if (hasOutgoing && hasIncoming && bobTriples.size >= 3) {
        println("✅ SUCCESS: getTriplesByEntity correctly finds both directions")
    } else {
        println("❌ FAILURE: Missing expected relations")
    }
    
    println("\n=== Test Completed ===")
}
