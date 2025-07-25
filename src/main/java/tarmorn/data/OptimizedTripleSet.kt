package tarmorn.data

import tarmorn.Settings
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.PrintWriter
import java.nio.charset.Charset
import java.nio.file.Files
import kotlin.random.Random

/**
 * Optimized TripleSet using integer IDs for better performance
 * Maintains compatibility with original string-based interface
 */
class OptimizedTripleSet(
    filepath: String? = null,
    ignore4Plus: Boolean = true
) {
    private val idManager = EntityIdManager()
    val optimizedTriples = mutableListOf<OptimizedTriple>()
    private val rand = Random

    // ID-based indices for better performance
    private var h2List = mutableMapOf<Int, MutableList<OptimizedTriple>>()
    private var t2List = mutableMapOf<Int, MutableList<OptimizedTriple>>()
    private var r2List = mutableMapOf<Int, MutableList<OptimizedTriple>>()

    private var h2r2tSet = mutableMapOf<Int, MutableMap<Int, MutableSet<Int>>>()
    private var t2r2hSet = mutableMapOf<Int, MutableMap<Int, MutableSet<Int>>>()

    private var h2r2tList = mutableMapOf<Int, MutableMap<Int, MutableList<Int>>>()
    private var t2r2hList = mutableMapOf<Int, MutableMap<Int, MutableList<Int>>>()

    private var frequentRelationIds = mutableSetOf<Int>()
    private var r2hSample = mutableMapOf<Int, MutableList<Int>>()
    private var r2tSample = mutableMapOf<Int, MutableList<Int>>()
    private var relationCounter = mutableMapOf<Int, Int>()

    init {
        filepath?.let {
            readTriples(it, ignore4Plus)
            indexTriples()
        }
    }

    // String interface compatibility methods
    fun addTriple(h: String, r: String, t: String) {
        val hId = idManager.getEntityId(h)
        val rId = idManager.getRelationId(r)
        val tId = idManager.getEntityId(t)
        addOptimizedTriple(OptimizedTriple(hId, rId, tId))
    }

    fun addTriple(triple: Triple) {
        addTriple(triple.h, triple.r, triple.t)
    }

    private fun addOptimizedTriple(triple: OptimizedTriple) {
        if (!isTrue(triple)) {
            optimizedTriples.add(triple)
            addTripleToIndex(triple)
        }
    }

    private fun indexTriples() {
        var tCounter = 0L
        var divisor = 10000L
        
        optimizedTriples.forEach { triple ->
            tCounter++
            if (tCounter % divisor == 0L) {
                println("* indexed $tCounter triples")
                divisor *= 2
            }
            addTripleToIndex(triple)
        }
        
        println("* set up index for ${r2List.keys.size} relations, ${h2List.keys.size} head entities, and ${t2List.keys.size} tail entities")
    }

    private fun addTripleToIndex(triple: OptimizedTriple) {
        val (hId, rId, tId) = triple
        
        // Helper function to add to nested map structure
        fun <T> addToNestedMap(
            map: MutableMap<Int, MutableMap<Int, T>>,
            key1: Int,
            key2: Int,
            defaultValue: () -> T,
            action: (T) -> Unit
        ) {
            val innerMap = map.getOrPut(key1) { mutableMapOf() }
            val value = innerMap.getOrPut(key2, defaultValue)
            action(value)
        }

        // Index head, tail, relation
        h2List.getOrPut(hId) { mutableListOf() }.add(triple)
        t2List.getOrPut(tId) { mutableListOf() }.add(triple)
        r2List.getOrPut(rId) { mutableListOf() }.add(triple)

        // Index head-relation => tail and tail-relation => head
        addToNestedMap(h2r2tSet, hId, rId, { mutableSetOf<Int>() }) { it.add(tId) }
        addToNestedMap(t2r2hSet, tId, rId, { mutableSetOf<Int>() }) { it.add(hId) }
    }

    // String interface methods for compatibility
    fun isTrue(head: String, relation: String, tail: String): Boolean {
        val hId = idManager.getEntityIdOrNull(head) ?: return false
        val rId = idManager.getRelationIdOrNull(relation) ?: return false
        val tId = idManager.getEntityIdOrNull(tail) ?: return false
        return isTrue(hId, rId, tId)
    }

    fun isTrue(triple: Triple): Boolean = isTrue(triple.h, triple.r, triple.t)

    private fun isTrue(hId: Int, rId: Int, tId: Int): Boolean =
        t2r2hSet[tId]?.get(rId)?.contains(hId) == true

    private fun isTrue(triple: OptimizedTriple): Boolean = isTrue(triple.hId, triple.rId, triple.tId)

    fun getTriplesByHead(head: String): List<Triple> {
        val hId = idManager.getEntityIdOrNull(head) ?: return emptyList()
        return h2List[hId]?.map { convertToStringTriple(it) } ?: emptyList()
    }

    fun getTriplesByRelation(relation: String): List<Triple> {
        val rId = idManager.getRelationIdOrNull(relation) ?: return emptyList()
        return r2List[rId]?.map { convertToStringTriple(it) } ?: emptyList()
    }

    private fun convertToStringTriple(optimizedTriple: OptimizedTriple): Triple {
        val h = idManager.getEntity(optimizedTriple.hId)!!
        val r = idManager.getRelation(optimizedTriple.rId)!!
        val t = idManager.getEntity(optimizedTriple.tId)!!
        return Triple(h, r, t)
    }

    // Performance monitoring methods
    fun getMemoryStats(): String {
        val entityCount = idManager.entityCount
        val relationCount = idManager.relationCount
        val tripleCount = optimizedTriples.size
        
        return """
            |Memory Statistics:
            |  Entities: $entityCount
            |  Relations: $relationCount  
            |  Triples: $tripleCount
            |  Estimated memory savings: ${estimateMemorySavings()}MB
        """.trimMargin()
    }

    private fun estimateMemorySavings(): Long {
        // Rough estimation: each string averages 20 chars = 40 bytes (UTF-16)
        // Each int = 4 bytes
        // Savings per triple: (40*3) - (4*3) = 108 bytes per triple
        return (optimizedTriples.size * 108L) / (1024 * 1024)
    }

    private fun readTriples(filepath: String, ignore4Plus: Boolean) {
        val file = File(filepath).toPath()
        val charset = Charset.forName("UTF8")
        var line: String? = null
        var lineCounter: Long = 0

        try {
            Files.newBufferedReader(file, charset).use { reader ->
                while ((reader.readLine().also { line = it }) != null) {
                    lineCounter++

                    if (lineCounter % 1000000 == 0L) {
                        println(">>> parsed $lineCounter lines")
                    }
                    if (line!!.length <= 2) continue
                    
                    var token = line!!.split("\t").dropLastWhile { it.isEmpty() }.toTypedArray()
                    if (token.size < 3) {
                        token = line!!.split(" ").dropLastWhile { it.isEmpty() }.toTypedArray()
                    }

                    if (token.size >= 3) {
                        val s = if (Settings.SAFE_PREFIX_MODE) {
                            Settings.PREFIX_ENTITY + token[0]
                        } else {
                            token[0]
                        }
                        val r = if (Settings.SAFE_PREFIX_MODE) {
                            Settings.PREFIX_RELATION + token[1]
                        } else {
                            token[1]
                        }
                        val o = if (Settings.SAFE_PREFIX_MODE) {
                            Settings.PREFIX_ENTITY + token[2]
                        } else {
                            token[2]
                        }

                        addTriple(s, r, o)
                    }
                }
            }
        } catch (x: IOException) {
            System.err.format("IOException: %s%n", x)
            System.err.format("Error occurred for line: $line")
        }
        
        println("* read ${optimizedTriples.size} triples")
    }

    val relations: Set<String> 
        get() = (0 until idManager.relationCount).mapNotNull { idManager.getRelation(it) }.toSet()

    val entities: Set<String>
        get() = (0 until idManager.entityCount).mapNotNull { idManager.getEntity(it) }.toSet()

    fun size() = optimizedTriples.size
}
