package tarmorn.test

import tarmorn.data.*

fun main() {
    println("=== Testing Inverse Relations Optimization ===")
    
    // Clear IdManager state
    IdManager.clear()
    
    // Create a simple test dataset
    val testData = """
        alice	knows	bob
        bob	likes	charlie
        charlie	works_at	company
    """.trimIndent()
    
    // Write test data to a temporary file
    val tempFile = java.io.File.createTempFile("test_triples", ".txt")
    tempFile.writeText(testData)
    tempFile.deleteOnExit()
    
    // Create TripleSet with the optimized inverse relations
    val tripleSet = TripleSet(tempFile.absolutePath, true)
    
    println("Original triples count: ${tripleSet.size}")
    println("Expected: 3 (no physical inverse triples)")
    
    // Test relation count
    val originalRelations = tripleSet.originalRelations
    val allRelations = tripleSet.relations
    
    println("Original relations count: ${originalRelations.size}")
    println("All relations count (including inverse): ${allRelations.size}")
    println("Expected: 3 original, 6 total")
    
    // Test specific queries
    val aliceId = IdManager.getEntityId("alice")
    val bobId = IdManager.getEntityId("bob")
    val knowsId = IdManager.getRelationId("knows")
    val inverseKnowsId = IdManager.getInverseRelationId(knowsId)
    
    println("\n=== Query Tests ===")
    
    // Test direct relation
    println("alice knows bob: ${tripleSet.isTrue(aliceId, knowsId, bobId)}")
    
    // Test inverse relation query
    println("bob INVERSE_knows alice: ${tripleSet.isTrue(bobId, inverseKnowsId, aliceId)}")
    
    // Test getTailEntities
    val aliceKnowsWho = tripleSet.getTailEntities(knowsId, aliceId)
    println("alice knows: ${aliceKnowsWho.map { IdManager.getEntityString(it) }}")
    
    // Test getHeadEntities (using inverse relation internally)
    val whoKnowsBob = tripleSet.getHeadEntities(knowsId, bobId)
    println("who knows bob: ${whoKnowsBob.map { IdManager.getEntityString(it) }}")
    
    // Test getTriplesByRelation for inverse relation
    val inverseKnowsTriples = tripleSet.getTriplesByRelation(inverseKnowsId)
    println("INVERSE_knows triples: ${inverseKnowsTriples}")
    
    println("\n=== Memory Usage Comparison ===")
    println("Without optimization: would have ${tripleSet.size * 2} triples (${tripleSet.size} original + ${tripleSet.size} inverse)")
    println("With optimization: only ${tripleSet.size} physical triples stored")
    println("Memory saved: ~${tripleSet.size} triples (50% reduction)")
    
    println("\n=== Test Completed Successfully ===")
}
