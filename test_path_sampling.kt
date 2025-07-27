package tarmorn.test

import tarmorn.data.*
import tarmorn.threads.Scorer

fun main() {
    println("=== Testing Path Sampling with Inverse Relations ===")
    
    // Clear IdManager state
    IdManager.clear()
    
    // Create a test dataset with chains that require reverse traversal
    val testData = """
        alice	knows	bob
        charlie	knows	bob
        bob	likes	david
        david	works_at	company
    """.trimIndent()
    
    // Write test data to a temporary file
    val tempFile = java.io.File.createTempFile("test_path_sampling", ".txt")
    tempFile.writeText(testData)
    tempFile.deleteOnExit()
    
    // Create TripleSet
    val tripleSet = TripleSet(tempFile.absolutePath, true)
    
    println("Original triples:")
    tripleSet.forEach { println("  $it") }
    
    // Test getTriplesByEntity for bob (should find both incoming and outgoing relations)
    val bobId = IdManager.getEntityId("bob")
    val bobTriples = tripleSet.getTriplesByEntity(bobId)
    
    println("\nTriples connected to bob (both directions):")
    bobTriples.forEach { println("  $it") }
    println("Expected: alice knows bob, charlie knows bob, bob likes david, bob INVERSE_knows alice, bob INVERSE_knows charlie")
    
    // Test path sampling capability
    println("\nTesting path sampling with reverse relations:")
    
    // Create a scorer to test path sampling
    val scorer = Scorer(tripleSet, 0)
    
    // Use reflection to access private samplePath method
    val samplePathMethod = Scorer::class.java.getDeclaredMethod("samplePath", Int::class.java, Boolean::class.java, Triple::class.java)
    samplePathMethod.isAccessible = true
    
    // Sample some paths
    repeat(10) { i ->
        try {
            val path = samplePathMethod.invoke(scorer, 3, false, null) as? tarmorn.structure.Path
            if (path != null) {
                println("  Path $i: $path")
            } else {
                println("  Path $i: null (failed to generate)")
            }
        } catch (e: Exception) {
            println("  Path $i: Error - ${e.message}")
        }
    }
    
    println("\n=== Test Analysis ===")
    println("With the new getTriplesByEntity method:")
    println("- bob can now be connected to alice/charlie via INVERSE_knows")
    println("- This allows paths like: alice -> bob -> charlie (via bob INVERSE_knows charlie)")
    println("- Or reverse paths like: david -> bob -> alice (via bob likes david + bob INVERSE_knows alice)")
    
    println("\n=== Test Completed ===")
}
