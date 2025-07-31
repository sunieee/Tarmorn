package tarmorn.topdown

import tarmorn.data.*
import tarmorn.Settings
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.mutableSetOf
import kotlin.collections.mutableMapOf
import kotlin.collections.mutableListOf
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter

/**
 * TLearn - Top-down relation path learning algorithm
 * Implements the connection algorithm: Binary Atom L=1 -connect-> Binary Atom L<=MAX_PATH_LENGTH
 */
object TLearn {

    const val MIN_SUPP = 50
    const val MAX_PATH_LENGTH = 3

    // Core data structures
    val ts: TripleSet = TripleSet(Settings.PATH_TRAINING, true)
    // lateinit var r2tripleSet: MutableMap<Long, MutableSet<MyTriple>>
    lateinit var r2tripleCount: MutableMap<Long, Int>
    lateinit var r2h2tSet: MutableMap<Long, MutableMap<Int, MutableSet<Int>>>
    lateinit var r2hSet: MutableMap<Long, MutableSet<Int>>

    // Thread-safe relation queue using ConcurrentLinkedQueue for producer-consumer pattern
    val relationQueue = PriorityQueue<RelationPathItem>(
        compareByDescending { it.supportSize }
    )
    val queueLock = Object()
    
    // Backup of L1 relations for connection attempts
    lateinit var relationL1: List<Long>
    
    // Synchronized logger for workers
    private val logFile = File("out/workers.log")
    private lateinit var logWriter: BufferedWriter
    
    // Initialize logger and clear the log file
    private fun initializeLogger() {
        synchronized(Any()) {
            try {
                logFile.parentFile?.mkdirs() // Create out directory if it doesn't exist
                logFile.writeText("") // Clear the file content
                logWriter = BufferedWriter(FileWriter(logFile, true)) // Append mode
            } catch (e: Exception) {
                println("Error initializing log file: ${e.message}")
            }
        }
    }
    
    // Synchronized logging function
    private fun logWorkerResult(connectedPath: Long, support: Int) {
        synchronized(logWriter) {
            try {
                val pathString = RelationPath.toString(connectedPath)
                logWriter.write("$pathString: $support\n")
                logWriter.flush()
            } catch (e: Exception) {
                println("Error writing to log: ${e.message}")
            }
        }
    }
    
    data class RelationPathItem(
        val relationPath: Long,
        val supportSize: Int
    )
    
    /**
     * Main entry point - can be run directly
     */
    @JvmStatic
    fun main(args: Array<String>) {
        Settings.load()
        println("TLearn - Top-down relation path learning algorithm")
        println("Loading triple set...")

        try {
            learn()

        } catch (e: Exception) {
            println("Error during learning: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Main entry point for the learning algorithm
     */
    fun learn() {
        // Initialize and clear the log file
        initializeLogger()
        
        // Initialize data structures
        // r2tripleSet = ts.r2tripleSet
        r2h2tSet = ts.r2h2tSet
        r2hSet = ts.r2hSet
        r2tripleCount = ts.r2tripleSet.mapValues { it.value.size }.toMutableMap()

        println("Starting TLearn algorithm...")
        println("MIN_SUPP: $MIN_SUPP, MAX_PATH_LENGTH: $MAX_PATH_LENGTH")
        
        // Step 1: Initialize with L=1 relations
        initializeLevel1Relations()
        
        // Step 2: Connect relations using multiple threads
        try {
            connectRelations()
        } catch (e: Exception) {
            println("Error during relation connection: ${e.message}")
            e.printStackTrace()
        } finally {
            println("TLearn algorithm completed. Total relation paths: ${r2tripleCount.size}")
            println(r2tripleCount)
            
            // Close the log file
            synchronized(logWriter) {
                try {
                    logWriter.close()
                } catch (e: Exception) {
                    println("Error closing log file: ${e.message}")
                }
            }
            // return r2tripleSet.mapValues { it.value.toSet() }
        }
    }
    
    /**
     * Step 1: Initialize level 1 relations (single relations with sufficient support)
     */
    fun initializeLevel1Relations() {
        println("Initializing level 1 relations...")
        
        val level1Relations = mutableListOf<Long>()
        var addedCount = 0
        
        synchronized(queueLock) {
            for ((relation, tripleSet) in ts.r2tripleSet) {
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
        // println("Level 1 relations: ${relationL1.map { IdManager.getRelationString(it) }}")
    }
    
    /**
     * Step 2: Connect relations using producer-consumer pattern with multiple threads
     */
    fun connectRelations() {
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
    fun connectRelationsWorker(threadId: Int, processedCount: AtomicInteger, addedCount: AtomicInteger) {
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
            for (r1 in relationL1) {
                try {
                    val connectedPath = attemptConnection(r1, ri)
                    if (connectedPath != null) {
                        val support = computeSupport(connectedPath, r1, ri)
                        if (support >= MIN_SUPP) {
                            synchronized(queueLock) {
                                val newItem = RelationPathItem(connectedPath, support)
                                relationQueue.offer(newItem)
                            }
                            addedCount.incrementAndGet()

                            // Log the successful path addition
                            logWorkerResult(connectedPath, support)

                            if (addedCount.get() % 100 == 0) {
                                println("Thread $threadId: Added ${addedCount.get()} new paths")
                                println("Thread $threadId: path $connectedPath added with support $support (r1: $r1, ri: $ri) TODO: ${relationQueue.size} remaining in queue")
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
     * Step 3 & 4: Attempt to connect (r1: relation, ri: relation path)，增加前缀
     * Returns the connected path ID if successful, null otherwise
     */
    fun attemptConnection(r1: Long, ri: Long): Long? {
        // Check if ri's last relation conflicts with inverse of r1
        val riFirstRelation = RelationPath.getFirstRelation(ri)
        val inverser1 = IdManager.getInverseRelation(r1)

        // Simple check without accessing RELATION_MASK
        if (riFirstRelation == inverser1) {
            return null // Skip conflicting inverse relations
        }

        // Create connected path rp = r1 · ri (reverse order for better performance)
        val rp = RelationPath.connectHead(r1, ri)

        // Check if rp or its inverse already exists
        // if (r2tripleSet.containsKey(rp) || r2tripleSet.containsKey(IdManager.getInverseRelation(rp))) {
        //     return null // Skip existing paths
        // }
        if (r2tripleCount.containsKey(rp)) {
            return null // Skip existing paths
        }

        return rp
    }

    /**
     * Step 4 & 5: Compute support for a connected relation path
     * Uses the Cartesian product approach described in the algorithm
     */
    fun computeSupport(rp: Long, r1: Long, ri: Long): Int {
        // Get tail entities of r1 (these become connecting entities)
        val pathLength = RelationPath.getLength(rp)
        val r1TailEntities = r2hSet[IdManager.getInverseRelation(r1)] ?: emptySet()

        // Get head entities for ri
        val riHeadEntities = r2hSet[ri] ?: emptySet()

        // Find intersection of possible connecting entities
        // val connectingEntities = r1TailEntities.intersect(riHeadEntities)
        // if (connectingEntities.isEmpty()) return 0

        val connectingEntities = r1TailEntities.asSequence()
            .filter { it in riHeadEntities } 
        // if (!connectingEntities.any()) return 0

        // Compute Cartesian product
        // val resultTriples = mutableSetOf<MyTriple>()
        val resultH2TSet = mutableMapOf<Int, MutableSet<Int>>()
        val resultHSet = mutableSetOf<Int>()
        val resultTSet = mutableSetOf<Int>()
        // val resultT2HSet = mutableMapOf<Int, MutableSet<Int>>()
        var size = 0

        for (connectingEntity in connectingEntities) {
            // Get head entities that can reach this connecting entity via r1
            // This is equivalent to: entities where (entity, r1, connectingEntity) exists
            val r1HeadEntities = r2h2tSet[IdManager.getInverseRelation(r1)]?.get(connectingEntity) ?: emptySet()

            // Get tail entities reachable from this connecting entity via ri
            val riTailEntities = r2h2tSet[ri]?.get(connectingEntity) ?: emptySet()
            
            size += r1HeadEntities.size * riTailEntities.size // Count the number of valid triples
            resultHSet.addAll(r1HeadEntities)
            resultTSet.addAll(riTailEntities)
            // Create Cartesian product
            if (pathLength < 3) {
                // For paths with length < 3, we store the full h2t mapping
                for (r1Head in r1HeadEntities) {
                    for (riTail in riTailEntities) {
                        resultH2TSet.getOrPut(r1Head) { mutableSetOf() }.add(riTail)
                    }
                }
            }
        }

        if (size >= MIN_SUPP) {
            // Add to main data structures
            // r2tripleSet[rp] = resultTriples
            r2tripleCount[rp] = size
            r2tripleCount[RelationPath.getInverseRelation(rp)] = size // Inverse relation also has same support
            if (pathLength < 3) {
                // For paths with length < 3, we store the full h2t mapping
                r2h2tSet[rp] = resultH2TSet
                // r2h2tSet[RelationPath.getInverseRelation(rp)] = resultH2TSet
            }
            
            // Update head entity index for the new relation path
            r2hSet[rp] = resultHSet
            r2hSet[RelationPath.getInverseRelation(rp)] = resultTSet
        }

        return size
    }
    

    fun computeSupportSet(rp: Long, r1: Long, ri: Long): MutableMap<Int, MutableSet<Int>> {
        // Get tail entities of r1 (these become connecting entities)
        val r1TailEntities = r2hSet[IdManager.getInverseRelation(r1)] ?: emptySet()

        // Get head entities for ri
        val riHeadEntities = r2hSet[ri] ?: emptySet()

        // Find intersection of possible connecting entities
        val connectingEntities = r1TailEntities.intersect(riHeadEntities)
        
        
        if (connectingEntities.isEmpty()) return mutableMapOf()

        val resultH2TSet = mutableMapOf<Int, MutableSet<Int>>()

        for (connectingEntity in connectingEntities) {
            // Get head entities that can reach this connecting entity via r1
            // This is equivalent to: entities where (entity, r1, connectingEntity) exists
            val r1HeadEntities = r2h2tSet[IdManager.getInverseRelation(r1)]?.get(connectingEntity) ?: emptySet()

            // Get tail entities reachable from this connecting entity via ri
            val riTailEntities = r2h2tSet[ri]?.get(connectingEntity) ?: emptySet()

            // Create Cartesian product
            for (r1Head in r1HeadEntities) {
                for (riTail in riTailEntities) {
                    resultH2TSet.getOrPut(r1Head) { mutableSetOf() }.add(riTail)
                }
            }
        }
        return resultH2TSet
    }
}
