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
    var r2List = mutableMapOf<Long, MutableList<Triple>>()

    var h2r2tSet = mutableMapOf<Int, MutableMap<Long, MutableSet<Int>>>()
    var t2r2hSet = mutableMapOf<Int, MutableMap<Long, MutableSet<Int>>>()

    var h2r2tList = mutableMapOf<Int, MutableMap<Long, MutableList<Int>>>()
    var t2r2hList = mutableMapOf<Int, MutableMap<Long, MutableList<Int>>>()

    var frequentRelations = mutableSetOf<Long>()
    var r2hSample = mutableMapOf<Long, MutableList<Int>>()
    var r2tSample = mutableMapOf<Long, MutableList<Int>>()
    var relationCounter = mutableMapOf<Long, Int>()

    init {
        filepath?.let {
            readTriples(it, ignore4Plus)
            indexTriples()
        }
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
        
        println("* set up index for ${r2List.keys.size} relations, ${h2List.keys.size} head entities, and ${t2List.keys.size} tail entities")
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
        r2List.getOrPut(r) { mutableListOf() }.add(triple)

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

                    if (token.size == 3) t = Triple.createTriple(s, r, o)
                    if (token.size != 3 && ignore4Plus) t = Triple.createTriple(s, r, o)
                    if (token.size == 4 && !ignore4Plus) {
                        if (token[3] == ".") t = Triple.createTriple(s, r, o)
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
                        t = Triple.createTriple(subject, relation, `object`)
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

    fun getTriplesByHeadNotTail(headOrTail: Int, byHeadNotTail: Boolean) =
        if (byHeadNotTail) getTriplesByHead(headOrTail) else getTriplesByTail(headOrTail)

    fun getNTriplesByHead(head: Int, n: Int): MutableList<Triple> {
        val headTriples = h2List[head] ?: return mutableListOf()
        return if (headTriples.size <= n) {
            headTriples
        } else {
            (0 until n).map { headTriples.random(rand) }.toMutableList()
        }
    }

    fun getTriplesByTail(tail: Int) = t2List[tail] ?: mutableListOf()

    fun getNTriplesByTail(tail: Int, n: Int): MutableList<Triple> {
        val tailTriples = t2List[tail] ?: return mutableListOf()
        return if (tailTriples.size <= n) {
            tailTriples
        } else {
            (0 until n).map { tailTriples.random(rand) }.toMutableList()
        }
    }

    fun getTriplesByRelation(relation: Long) = r2List[relation] ?: mutableListOf()

    fun getRandomTripleByRelation(relation: Long) = r2List[relation]?.randomOrNull(rand)


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
        val relationTriples = r2List[relation]
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

    /**
     * Select randomly n entities that appear in head (or tail) position of a triple using a given relation.
     * More frequent entities appear more frequent. This is the difference compared to the method computeNRandomEntitiesByRelation.
     */
    fun selectNRandomEntitiesByRelation(relation: Long, ifHead: Boolean, n: Int): MutableList<Int>? {
        val relationTriples = r2List[relation] ?: run {
            System.err.println("Internal reference to relation ${IdManager.getRelationString(relation)}, which is not indexed")
            System.err.println("Check if rule set and triple set fit together")
            return null
        }

        val entities = relationTriples.take(n).map { it.getValue(ifHead) }.toMutableList()
        return (0 until n).map { entities.random(rand) }.toMutableList()
    }


    val relations: MutableSet<Long> get() = r2List.keys.toMutableSet()

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

    /*
	public Set<String> getRelations(String head, String tail) {
		if (headTail2RelationSet.get(head) != null) {
			if (headTail2RelationSet.get(head).get(tail) != null) {
				return headTail2RelationSet.get(head).get(tail);
			}
		}
		return new HashSet<Int>();
	}
	*/
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

    val numOfEntities get() = h2List.keys.size + t2List.keys.size

    fun determineFrequentRelations(coverage: Double) {
        // Count relations
        forEach { triple ->
            relationCounter[triple.r] = relationCounter.getOrDefault(triple.r, 0) + 1
        }

        val counts = relationCounter.values.sorted()
        val totalCount = size
        var countUp = 0
        var border = 0
        
        for (count in counts) {
            countUp += count
            if ((totalCount - countUp).toDouble() / totalCount < coverage) {
                border = count
                break
            }
        }

        frequentRelations = relationCounter
            .filterValues { it > border }
            .keys
            .toMutableSet()
    }

    fun isFrequentRelation(relation: Long) = relation in frequentRelations

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
