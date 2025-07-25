package tarmorn.data

import kotlin.system.measureTimeMillis

/**
 * Performance comparison between original TripleSet and OptimizedTripleSet
 */
object TripleSetPerformanceTest {
    
    @JvmStatic
    fun main(args: Array<String>) {
        val dataFile = "data/FB15k/train.txt"
        
        println("=== TripleSet Performance Comparison ===")
        
        // Test original TripleSet
        println("\n1. Testing Original TripleSet:")
        val originalTime = measureTimeMillis {
            val originalSet = TripleSet(dataFile)
            println("Original TripleSet loaded ${originalSet.size()} triples")
            
            // Test some operations
            val testEntity = originalSet.entities.first()
            val testRelation = originalSet.relations.first()
            
            val lookupTime = measureTimeMillis {
                repeat(10000) {
                    originalSet.getTriplesByHead(testEntity)
                    originalSet.getTriplesByRelation(testRelation)
                    originalSet.isTrue(testEntity, testRelation, testEntity)
                }
            }
            println("10,000 lookups took: ${lookupTime}ms")
        }
        println("Original TripleSet total time: ${originalTime}ms")
        
        // Test optimized TripleSet
        println("\n2. Testing Optimized TripleSet:")
        val optimizedTime = measureTimeMillis {
            val optimizedSet = OptimizedTripleSet(dataFile)
            println("Optimized TripleSet loaded ${optimizedSet.size()} triples")
            println(optimizedSet.getMemoryStats())
            
            // Test some operations
            val testEntity = optimizedSet.entities.first()
            val testRelation = optimizedSet.relations.first()
            
            val lookupTime = measureTimeMillis {
                repeat(10000) {
                    optimizedSet.getTriplesByHead(testEntity)
                    optimizedSet.getTriplesByRelation(testRelation)
                    optimizedSet.isTrue(testEntity, testRelation, testEntity)
                }
            }
            println("10,000 lookups took: ${lookupTime}ms")
        }
        println("Optimized TripleSet total time: ${optimizedTime}ms")
        
        // Summary
        println("\n=== Performance Summary ===")
        println("Original time: ${originalTime}ms")
        println("Optimized time: ${optimizedTime}ms")
        println("Speedup: ${originalTime.toDouble() / optimizedTime}x")
        
        // Memory usage comparison
        println("\n=== Memory Usage Analysis ===")
        analyzeMemoryUsage()
    }
    
    private fun analyzeMemoryUsage() {
        println("""
            |Current String-based approach issues:
            |
            |1. **String Interning Overhead**: 
            |   - Each string must be interned (.intern() calls)
            |   - Interning has computational cost during loading
            |   - String pool can become large in memory
            |
            |2. **HashMap Key Performance**:
            |   - String hash calculation is slower than int hash
            |   - String comparison for collisions is slower
            |   - More cache misses due to pointer dereferencing
            |
            |3. **Memory Overhead**:
            |   - Each string object has ~24 bytes overhead (object header, length, etc.)
            |   - String content stored as char array (2 bytes per character)
            |   - For FB15k with ~480k triples: estimated ~200MB just for string storage
            |
            |4. **GC Pressure**:
            |   - Many string objects create GC pressure
            |   - Interned strings live until JVM shutdown
            |
            |Optimized Integer-based approach benefits:
            |
            |1. **Faster Lookups**: int hash and equality much faster
            |2. **Memory Efficient**: 4 bytes per ID vs ~20-50 bytes per string
            |3. **Cache Friendly**: integers pack better in memory
            |4. **Reduced GC**: fewer objects, less garbage collection
            |
            |Estimated improvements for large datasets:
            |- Memory usage: 60-80% reduction
            |- Lookup speed: 2-5x faster
            |- Loading time: 20-40% faster (no interning overhead)
        """.trimMargin())
    }
}
