package tarmorn.topdown

import tarmorn.data.*
import tarmorn.Settings
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.mutableSetOf
import kotlin.collections.mutableMapOf
import kotlin.collections.mutableListOf

/**
 * TLearn - Top-down relation path learning algorithm
 * Implements the connection algorithm: Binary Atom L=1 -connect-> Binary Atom L<=MAX_PATH_LENGTH
 */
object TLearn {

    private const val MIN_SUPP = 20
    private const val MAX_PATH_LENGTH = 3

    // Core data structures
    private lateinit var tripleSet: TripleSet
    private lateinit var r2tripleSet: MutableMap<Long, MutableSet<MyTriple>>
    private lateinit var r2h2tSet: MutableMap<Long, MutableMap<Int, MutableSet<Int>>>

    // Thread-safe relation queue using ConcurrentLinkedQueue for producer-consumer pattern
    private val relationQueue = PriorityQueue<RelationPathItem>(
        compareByDescending { it.supportSize }
    )
    private val queueLock = Object()
    
    // Backup of L1 relations for connection attempts
    private lateinit var relationL1: List<Long>
    
    data class RelationPathItem(
        val relationPath: Long,
        val supportSize: Int
    )
    
    /**
     * Main entry point - can be run directly
     */
    @JvmStatic
    fun main(args: Array<String>) {
        println("TLearn - Top-down relation path learning algorithm")
        println("Loading triple set...")

        try {
            // Load the default triple set
            val loadedTripleSet = TripleSet()  // Assuming default constructor loads data
            val results = learn(loadedTripleSet)

            println("\n=== Learning Results ===")
            println("Total learned relation paths: ${results.size}")

            // Print some statistics
            val totalTriples = results.values.sumOf { it.size }
            println("Total triples generated: $totalTriples")

            // Print top 10 relation paths by support
            println("\nTop 10 relation paths by support:")
            results.entries
                .sortedByDescending { it.value.size }
                .take(10)
                .forEachIndexed { index, (pathId, triples) ->
                    val pathString = try {
                        RelationPath.decode(pathId).joinToString(" -> ") {
                            IdManager.getRelationString(it)
                        }
                    } catch (e: Exception) {
                        "Path_$pathId"
                    }
                    println("${index + 1}. $pathString (${triples.size} triples)")
                }

        } catch (e: Exception) {
            println("Error during learning: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Main entry point for the learning algorithm
     */
    fun learn(inputTripleSet: TripleSet): Map<Long, Set<MyTriple>> {
        // Initialize data structures
        tripleSet = inputTripleSet
        r2tripleSet = tripleSet.r2tripleSet
        r2h2tSet = tripleSet.r2h2tSet

        println("Starting TLearn algorithm...")
        println("MIN_SUPP: $MIN_SUPP, MAX_PATH_LENGTH: $MAX_PATH_LENGTH")
        
        // Step 1: Initialize with L=1 relations
        initializeLevel1Relations()
        
        // Step 2: Connect relations using multiple threads
        connectRelations()
        
        println("TLearn algorithm completed. Total relation paths: ${r2tripleSet.size}")
        return r2tripleSet.mapValues { it.value.toSet() }
    }
    
    /**
     * Step 1: Initialize level 1 relations (single relations with sufficient support)
     */
    private fun initializeLevel1Relations() {
        println("Initializing level 1 relations...")
        
        val level1Relations = mutableListOf<Long>()
        var addedCount = 0
        
        synchronized(queueLock) {
            for ((relation, tripleSet) in r2tripleSet) {
                if (tripleSet.size >= MIN_SUPP) {
                    val pathId = RelationPath.encode(relation)
                    val item = RelationPathItem(pathId, tripleSet.size)
                    relationQueue.offer(item)
                    level1Relations.add(relation)
                    addedCount++
                }
            }
        }
        
        relationL1 = level1Relations
        println("Added $addedCount level 1 relations to queue")
        println("Level 1 relations: ${relationL1.map { IdManager.getRelationString(it) }}")
    }
    
    /**
     * Step 2: Connect relations using producer-consumer pattern with multiple threads
     */
    private fun connectRelations() {
        println("Starting relation connection with ${Settings.WORKER_THREADS} threads...")
        
        val threadPool = Executors.newFixedThreadPool(Settings.WORKER_THREADS)
        val processedCount = AtomicInteger(0)
        val addedCount = AtomicInteger(0)
        
        // Create worker threads
        val futures = (1..Settings.WORKER_THREADS).map { threadId ->
            threadPool.submit {
                connectRelationsWorker(threadId, processedCount, addedCount)
            }
        }
        
        // Wait for all threads to complete
        futures.forEach { it.get() }
        threadPool.shutdown()
        
        println("Connection completed. Processed: ${processedCount.get()}, Added: ${addedCount.get()}")
    }
    
    /**
     * Worker thread for connecting relations
     */
    private fun connectRelationsWorker(threadId: Int, processedCount: AtomicInteger, addedCount: AtomicInteger) {
        println("Thread $threadId started")
        
        while (true) {
            // Step 3: Get next relation path from queue
            val currentItem = synchronized(queueLock) {
                relationQueue.poll()
            } ?: break // Queue is empty
            
            val ri = currentItem.relationPath
            val riLength = RelationPath.getLength(ri)
            
            // Skip if already at max length
            if (riLength >= MAX_PATH_LENGTH) {
                continue
            }
            
            processedCount.incrementAndGet()
            
            // Step 4: Try connecting with all L1 relations
            for (rj in relationL1) {
                try {
                    val connectedPath = attemptConnection(ri, rj)
                    if (connectedPath != null) {
                        val support = computeSupport(connectedPath)
                        if (support >= MIN_SUPP) {
                            synchronized(queueLock) {
                                val newItem = RelationPathItem(connectedPath, support)
                                relationQueue.offer(newItem)
                            }
                            addedCount.incrementAndGet()
                            
                            if (addedCount.get() % 100 == 0) {
                                println("Thread $threadId: Added ${addedCount.get()} new paths")
                            }
                        }
                    }
                } catch (e: Exception) {
                    // Log error but continue processing
                    println("Thread $threadId: Error connecting paths: ${e.message}")
                }
            }
        }
        
        println("Thread $threadId completed")
    }
    
    /**
     * Step 3 & 4: Attempt to connect ri with rj
     * Returns the connected path ID if successful, null otherwise
     */
    private fun attemptConnection(ri: Long, rj: Long): Long? {
        // Check if ri's last relation conflicts with inverse of rj
        val riLastRelation = RelationPath.getLastRelation(ri)
        val inverseRj = IdManager.getInverseRelation(rj)

        // Simple check without accessing private RELATION_MASK
        if (riLastRelation == inverseRj) {
            return null // Skip conflicting inverse relations
        }

        // Create connected path rp = ri · rj
        val rp = RelationPath.connect(ri, rj)

        // Check if rp or its inverse already exists
        if (r2tripleSet.containsKey(rp) || r2tripleSet.containsKey(IdManager.getInverseRelation(rp))) {
            return null // Skip existing paths
        }

        return rp
    }

    /**
     * Step 4 & 5: Compute support for a connected relation path
     * Uses the Cartesian product approach described in the algorithm
     */
    private fun computeSupport(rp: Long): Int {
        val relations = RelationPath.decode(rp)
        if (relations.size < 2) return 0

        // For path ri · rj, we need to compute the intersection and Cartesian product
        val ri = RelationPath.encode(relations.sliceArray(0 until relations.size - 1))
        val rj = relations.last()

        // Get head entities that can be reached by ri
        val riHeadEntities = getPathHeadEntities(ri)

        // Get head entities for rj
        val rjHeadEntities = r2h2tSet[rj]?.keys ?: emptySet()

        // Find intersection of possible connecting entities
        val connectingEntities = riHeadEntities.intersect(rjHeadEntities)

        if (connectingEntities.isEmpty()) return 0

        // Compute Cartesian product
        val resultTriples = mutableSetOf<MyTriple>()
        val resultH2TSet = mutableMapOf<Int, MutableSet<Int>>()

        for (connectingEntity in connectingEntities) {
            // Get tail entities reachable from this connecting entity via ri'
            val riTailEntities = getPathTailEntities(ri, connectingEntity)

            // Get tail entities reachable from this connecting entity via rj
            val rjTailEntities = r2h2tSet[rj]?.get(connectingEntity) ?: emptySet()

            // Create Cartesian product
            for (riTail in riTailEntities) {
                for (rjTail in rjTailEntities) {
                    val triple = MyTriple(riTail, rp, rjTail)
                    resultTriples.add(triple)

                    // Update h2t index
                    resultH2TSet.getOrPut(riTail) { mutableSetOf() }.add(rjTail)
                }
            }
        }

        if (resultTriples.size >= MIN_SUPP) {
            // Add to main data structures
            r2tripleSet[rp] = resultTriples
            r2h2tSet[rp] = resultH2TSet
        }

        return resultTriples.size
    }
    
    /**
     * Get head entities that can be reached by a relation path
     */
    private fun getPathHeadEntities(pathId: Long): Set<Int> {
        if (RelationPath.isSingleRelation(pathId)) {
            val relation = RelationPath.getFirstRelation(pathId)
            return r2h2tSet[relation]?.keys ?: emptySet()
        }

        // For complex paths, we need to compute reachable entities
        // This is a simplified version - in practice you might want to cache these
        val relations = RelationPath.decode(pathId)
        var currentEntities = r2h2tSet[relations[0]]?.keys ?: emptySet()
        
        for (i in 1 until relations.size) {
            val nextEntities = mutableSetOf<Int>()
            for (entity in currentEntities) {
                val reachable = r2h2tSet[relations[i]]?.keys ?: emptySet()
                nextEntities.addAll(reachable)
            }
            currentEntities = nextEntities
        }

        return currentEntities
    }

    /**
     * Get tail entities reachable from a specific head entity via a relation path
     */
    private fun getPathTailEntities(pathId: Long, headEntity: Int): Set<Int> {
        if (RelationPath.isSingleRelation(pathId)) {
            val relation = RelationPath.getFirstRelation(pathId)
            return r2h2tSet[relation]?.get(headEntity) ?: emptySet()
        }

        // For complex paths, trace through the path
        val relations = RelationPath.decode(pathId)
        var currentEntities = setOf(headEntity)

        for (relation in relations) {
            val nextEntities = mutableSetOf<Int>()
            for (entity in currentEntities) {
                val reachable = r2h2tSet[relation]?.get(entity) ?: emptySet()
                nextEntities.addAll(reachable)
            }
            currentEntities = nextEntities
        }

        return currentEntities
    }
}
