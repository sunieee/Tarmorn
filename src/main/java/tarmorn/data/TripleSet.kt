package tarmorn.data

import tarmorn.Settings
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.PrintWriter
import java.nio.charset.Charset
import java.nio.file.Files
import kotlin.random.Random

class TripleSet(
    filepath: String? = null,
    ignore4Plus: Boolean = true
) : MutableList<Triple> by mutableListOf() {
    private val rand = Random

    var h2List = mutableMapOf<Int, MutableList<Triple>>()
    var t2List = mutableMapOf<Int, MutableList<Triple>>()
    var r2Set = mutableMapOf<Long, MutableSet<Triple>>()

    var h2r2tSet = mutableMapOf<Int, MutableMap<Long, MutableSet<Int>>>()
    var t2r2hSet = mutableMapOf<Int, MutableMap<Long, MutableSet<Int>>>()

    var h2r2tList = mutableMapOf<Int, MutableMap<Long, MutableList<Int>>>()
    var t2r2hList = mutableMapOf<Int, MutableMap<Long, MutableList<Int>>>()

    var r2hSample = mutableMapOf<Long, MutableList<Int>>()
    var r2tSample = mutableMapOf<Long, MutableList<Int>>()

    init {
        filepath?.let {
            readTriples(it, ignore4Plus)
            addInverseRelations() // Add inverse relations after reading triples
            indexTriples()
        }
    }

    private fun addInverseRelations() {
        println("* Adding inverse relations...")
        
        // First, add inverse relation mappings in IdManager
        IdManager.addInverseRelations()
        
        // Then, create inverse triples for all existing triples
        val originalTriples = this.toList() // Create a copy to avoid concurrent modification
        originalTriples.forEach { triple ->
            val inverseRelationId = IdManager.getInverseRelationId(triple.r)
            add(Triple(triple.t, inverseRelationId, triple.h))
        }
        
        println("* Added ${originalTriples.size} inverse triples for ${IdManager.originalRelationCount} relations")
    }

    // Public method for external access (e.g., testing)
    fun generateInverseRelations() {
        addInverseRelations()
    }

    fun addTripleSet(ts: TripleSet) {
        ts.forEach { addTriple(it) }
    }

    fun addTriples(triples: List<Triple>) {
        triples.forEach { addTriple(it) }
    }

    fun addTriple(t: Triple) {
        if (!isTrue(t)) {
            add(t)
            addTripleToIndex(t)
        }
    }


    private fun indexTriples() {
        var tCounter = 0L
        var divisor = 10000L
        
        forEach { triple ->
            tCounter++
            if (tCounter % divisor == 0L) {
                println("* indexed $tCounter triples")
                divisor *= 2
            }
            addTripleToIndex(triple)
        }
        
        println("* set up index for ${r2Set.keys.size} relations, ${h2List.keys.size} head entities, and ${t2List.keys.size} tail entities")
    }

    fun setupListStructure() {
        print("* set up list structure for randomized access searches during rule learning ... ")

        // Helper function to setup list structure
        fun setupEntityRelationList(
            sourceSet: Map<Int, Map<Long, Set<Int>>>,
            targetList: MutableMap<Int, MutableMap<Long, MutableList<Int>>>
        ) {
            sourceSet.forEach { (entity, relationMap) ->
                targetList[entity] = mutableMapOf()
                relationMap.forEach { (relation, entitySet) ->
                    if (entitySet.size > 10) {
                        targetList[entity]!![relation] = entitySet.toMutableList()
                        sampleSubset(targetList[entity]!![relation]!!)
                    }
                }
            }
        }

        // head -> relation -> tails
        setupEntityRelationList(h2r2tSet, h2r2tList)
        // tail -> relation -> head
        setupEntityRelationList(t2r2hSet, t2r2hList)
        
        println(" done")
    }

    private fun sampleSubset(list: MutableList<Int>) {
        list.shuffle()
        while (list.size > 5000) {
            list.removeAt(list.lastIndex)
        }
    }

    private fun addTripleToIndex(triple: Triple) {
        val (h, r, t) = triple
        
        // Helper function to add to nested map structure
        fun <T> addToNestedMap(
            map: MutableMap<Int, MutableMap<Long, T>>,
            key1: Int,
            key2: Long,
            defaultValue: () -> T,
            action: (T) -> Unit
        ) {
            val innerMap = map.getOrPut(key1) { mutableMapOf() }
            val value = innerMap.getOrPut(key2, defaultValue)
            action(value)
        }

        // Index head, tail, relation
        h2List.getOrPut(h) { mutableListOf() }.add(triple)
        t2List.getOrPut(t) { mutableListOf() }.add(triple)
        r2Set.getOrPut(r) { mutableSetOf() }.add(triple)

        // Index head-relation => tail and tail-relation => head
        addToNestedMap(h2r2tSet, h, r, { mutableSetOf<Int>() }) { it.add(t) }
        addToNestedMap(t2r2hSet, t, r, { mutableSetOf<Int>() }) { it.add(h) }
    }


    private fun readTriples(filepath: String, ignore4Plus: Boolean) {
        val file = (File(filepath)).toPath()
        // Charset charset = Charset.forName("US-ASCII");
        val charset = Charset.forName("UTF8")
        var line: String? = null
        var lineCounter = 0L
        var s: String
        var r: String
        var o: String
        try {
            Files.newBufferedReader(file, charset).use { reader ->
                while ((reader.readLine().also { line = it }) != null) {
                    // println(line);
                    lineCounter++
                    //  if (lineCounter % 7 == 0) continue;
                    if (lineCounter % 1000000 == 0L) {
                        println(">>> parsed " + lineCounter + " lines")
                    }
                    if (line!!.length <= 2) continue
                    var token = line.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                    if (token.size < 3) token =
                        line.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                    var t: Triple? = null
                    if (Settings.SAFE_PREFIX_MODE) {
                        s = (Settings.PREFIX_ENTITY + token[0]).intern()
                        r = (Settings.PREFIX_RELATION + token[1]).intern()
                        o = (Settings.PREFIX_ENTITY + token[2]).intern()
                    } else {
                        s = token[0].intern()
                        r = token[1].intern()
                        o = token[2].intern()
                    }

                    if (token.size == 3) t = Triple(s, r, o)
                    if (token.size != 3 && ignore4Plus) t = Triple(s, r, o)
                    if (token.size == 4 && !ignore4Plus) {
                        if (token[3] == ".") t = Triple(s, r, o)
                        else {
                            System.err.println("could not parse line " + line)
                            t = null
                        }
                    }
                    // VERY SPECIAL CASE FOR SAMUELS DATASET
                    if (token.size == 5 && !ignore4Plus) {
                        var subject = token[0]
                        var relation = token[1]
                        var `object` = token[2]
                        subject = subject.replace(" ", "_")
                        relation = relation.replace(" ", "_")
                        `object` = `object`.replace(" ", "_")
                        t = Triple(subject, relation, `object`)
                    }

                    if (t == null) {
                    } else {
                        this.add(t)
                    }
                }
            }
        } catch (x: IOException) {
            System.err.format("IOException: %s%n", x)
            System.err.format("Error occured for line: " + line + " LINE END")
        }
        // Collections.shuffle(this);
        println("* read " + this.size + " triples")
    }

    fun getTriplesByHead(head: Int) = h2List[head] ?: mutableListOf()

    fun getTriplesByRelation(relation: Long): MutableList<Triple> = 
        r2Set[relation]?.toMutableList() ?: mutableListOf()

    fun getRandomTripleByRelation(relation: Long): Triple? = 
        r2Set[relation]?.randomOrNull(rand)


    fun getNRandomEntitiesByRelation(relation: Long, ifHead: Boolean, n: Int): MutableList<Int> {
        val sampleMap = if (ifHead) r2hSample else r2tSample
        return sampleMap[relation] ?: computeNRandomEntitiesByRelation(relation, ifHead, n)
    }

    fun precomputeNRandomEntitiesPerRelation(n: Int) {
        print("* precomputing random starting points for each relation/direction for the beam search ...")
        relations.forEach { relation ->
            computeNRandomEntitiesByRelation(relation, true, n)
            computeNRandomEntitiesByRelation(relation, false, n)
        }
        println(" done")
    }

    @Synchronized
    private fun computeNRandomEntitiesByRelation(relation: Long, ifHead: Boolean, n: Int): MutableList<Int> {
        val relationTriples = r2Set[relation]
        if (relationTriples == null) {
            System.err.println("Internal reference to relation ${IdManager.getRelationString(relation)}, which is not indexed")
            System.err.println("Check if rule set and triple set fit together")
            return mutableListOf()
        }

        val entities = relationTriples
            .map { it.getValue(ifHead) }
            .distinct()
            .toMutableList()

        val sampledEntities = (0 until n).map { entities.random(rand) }.toMutableList()
        
        val targetMap = if (ifHead) r2hSample else r2tSample
        targetMap[relation] = sampledEntities
        
        return sampledEntities
    }

    val relations: MutableSet<Long> get() = r2Set.keys.toMutableSet()

    // Get only original relations (excluding inverse relations)
    val originalRelations: MutableSet<Long> 
        get() = r2Set.keys.filter { !IdManager.isInverseRelation(it) }.toMutableSet()

    // Check if a triple exists, considering inverse relations
    fun isTrueWithInverse(head: Int, relation: Long, tail: Int): Boolean {
        // First check the direct relation
        if (isTrue(head, relation, tail)) return true
        
        // Then check using inverse relation
        val inverseRelationId = IdManager.getInverseRelationId(relation)
        return if (inverseRelationId != null) {
            isTrue(tail, inverseRelationId, head)
        } else {
            false
        }
    }

    fun getHeadEntities(relation: Long, tail: Int): MutableSet<Int> = 
        t2r2hSet[tail]?.get(relation) ?: mutableSetOf()

    fun getTailEntities(relation: Long, head: Int): MutableSet<Int> = 
        h2r2tSet[head]?.get(relation) ?: mutableSetOf()

    fun getEntities(relation: Long, value: Int, ifHead: Boolean): MutableSet<Int> =
        if (ifHead) getTailEntities(relation, value) else getHeadEntities(relation, value)

    fun getRandomEntity(relation: Long, value: Int, ifHead: Boolean) =
        if (ifHead) getRandomTailEntity(relation, value) else getRandomHeadEntity(relation, value)

    private fun getRandomHeadEntity(relation: Long, tail: Int): Int? {
        val list = t2r2hList[tail]?.get(relation) ?: run {
            val headSet = t2r2hSet[tail]?.get(relation)
            if (headSet?.isNotEmpty() == true) headSet.toMutableList() else return null
        }
        return list.randomOrNull(rand)
    }

    private fun getRandomTailEntity(relation: Long, head: Int): Int? {
        val list = h2r2tList[head]?.get(relation) ?: run {
            val tailSet = h2r2tSet[head]?.get(relation)
            if (tailSet?.isNotEmpty() == true) tailSet.toMutableList() else return null
        }
        return list.randomOrNull(rand)
    }

    fun isTrue(head: Int, relation: Long, tail: Int) =
        t2r2hSet[tail]?.get(relation)?.contains(head) == true

    fun isTrue(triple: Triple) = isTrue(triple.h, triple.r, triple.t)

    fun compareTo(that: TripleSet, thisId: String, thatId: String) {
        println("* Comparing two triple sets")
        val intersectionCount = count { that.isTrue(it) }

        println("* size of $thisId: ${size}")
        println("* size of $thatId: ${that.size}")
        println("* size of intersection: $intersectionCount")
    }

    fun getIntersectionWith(that: TripleSet) = TripleSet().apply {
        filter { that.isTrue(it) }.forEach { addTriple(it) }
    }

    fun minus(that: TripleSet) = TripleSet().apply {
        filter { !that.isTrue(it) }.forEach { addTriple(it) }
    }

    val entities: MutableSet<Int>
        get() = (h2List.keys + t2List.keys).toMutableSet()

    @Throws(FileNotFoundException::class)
    fun write(filepath: String) {
        PrintWriter(filepath).use { pw ->
            forEach { pw.println(it) }
        }
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            // TripleSet ts = TripleSet("data/DB500/ftest.txt")
        }
    }
}
