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
    ignore4Plus: Boolean = true,
    evaluate: Boolean = false
) : MutableList<MyTriple> by mutableListOf() {
    private val rand = Random

    // 统一的实体索引：entity -> 以该实体为相关的所有三元组
    var h2tripleList = mutableMapOf<Int, MutableList<MyTriple>>()
    
    // 关系索引：relation -> 该关系的所有三元组，不存储逆关系
    var r2tripleSet = mutableMapOf<Long, MutableSet<MyTriple>>()

    // 核心查询索引：head -> relation -> tails
    var h2r2tSet = mutableMapOf<Int, MutableMap<Long, MutableSet<Int>>>()

    // 核心查询索引：relation -> head -> tails，不存储逆关系
    var r2h2tSet = mutableMapOf<Long, MutableMap<Int, MutableSet<Int>>>()

    // 性能优化索引：relation -> head entities (避免每次调用keys)
    var r2hSet = mutableMapOf<Long, MutableSet<Int>>()

    // 用于大关系的随机访问优化
    var h2r2tList = mutableMapOf<Int, MutableMap<Long, MutableList<Int>>>()


    // 统一的关系采样缓存：relation -> sampled entities (作为head)
    var r2hSample = mutableMapOf<Long, MutableList<Int>>()
    var r2tSample = mutableMapOf<Long, MutableList<Int>>()

    init {
        filepath?.let {
            readTriples(it, ignore4Plus)
            if (!evaluate) addInverseRelations() // Add inverse relations after reading triples
            indexTriples()
        }
    }

    fun addInverseRelations() {
        println("* Adding inverse relations...")
        
        // First, add inverse relation mappings in IdManager
        IdManager.addInverseRelations()
        
        // Then, create inverse triples for all existing triples
        val originalTriples = this.toList() // Create a copy to avoid concurrent modification
        originalTriples.forEach { triple ->
            val inverseRelationId = IdManager.getInverseRelation(triple.r)
            add(MyTriple(triple.t, inverseRelationId, triple.h))
        }
        
        println("* Added ${originalTriples.size} inverse triples for ${IdManager.originalRelationCount} relations")
    }

    fun addTripleSet(ts: TripleSet) {
        ts.forEach { addTriple(it) }
    }

    fun addTriples(triples: List<MyTriple>) {
        triples.forEach { addTriple(it) }
    }

    fun addTriple(t: MyTriple) {
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
        
        println("* set up index for ${r2tripleSet.keys.size} relations, ${h2tripleList.keys.size} entities")
    }

    fun setupListStructure() {
        print("* set up list structure for randomized access searches during rule learning ... ")

        // 只需要设置一个方向的索引，另一个方向通过逆关系访问
        h2r2tSet.forEach { (entity, relationMap) ->
            h2r2tList[entity] = mutableMapOf()
            relationMap.forEach { (relation, entitySet) ->
                if (entitySet.size > 10) {
                    h2r2tList[entity]!![relation] = entitySet.toMutableList()
                    sampleSubset(h2r2tList[entity]!![relation]!!)
                }
            }
        }
        
        println(" done")
    }

    private fun sampleSubset(list: MutableList<Int>) {
        list.shuffle()
        while (list.size > 5000) {
            list.removeAt(list.lastIndex)
        }
    }

    private fun addTripleToIndex(triple: MyTriple) {
        // 注意这个triple既有原始三元组，也有逆三元组
        val (h, r, t) = triple

        if (h==t) {
            // 注意：我们不把自环事实放入索引中！
            println("Warning: Triple with head equals tail detected: $triple")
            return
        }
        
        // 统一的实体索引：每个实体都索引以它为头的三元组
        h2tripleList.getOrPut(h) { mutableListOf() }.add(triple)
        
        // 关系索引 - 只存储原始关系
        r2tripleSet.getOrPut(r) { mutableSetOf() }.add(triple)

        // 核心查询索引：relation -> head  -> tails
        val htMap = r2h2tSet.getOrPut(key=r) { mutableMapOf() }
        htMap.getOrPut(h) { mutableSetOf() }.add(t)

        // 性能优化索引：relation -> head entities
        r2hSet.getOrPut(r) { mutableSetOf() }.add(h)

        // 核心查询索引：head -> relation -> tails
        val relationMap = h2r2tSet.getOrPut(h) { mutableMapOf() }
        relationMap.getOrPut(r) { mutableSetOf() }.add(t)
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
                    var t: MyTriple? = null
                    if (Settings.SAFE_PREFIX_MODE) {
                        s = (Settings.PREFIX_ENTITY + token[0]).intern()
                        r = (Settings.PREFIX_RELATION + token[1]).intern()
                        o = (Settings.PREFIX_ENTITY + token[2]).intern()
                    } else {
                        s = token[0].intern()
                        r = token[1].intern()
                        o = token[2].intern()
                    }

                    if (token.size == 3) t = MyTriple(s, r, o)
                    if (token.size != 3 && ignore4Plus) t = MyTriple(s, r, o)
                    if (token.size == 4 && !ignore4Plus) {
                        if (token[3] == ".") t = MyTriple(s, r, o)
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
                        t = MyTriple(subject, relation, `object`)
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

    fun getTriplesByHead(head: Int) = h2tripleList[head] ?: mutableListOf()

    /**
     * Get all triples connected to an entity (both as head and tail)
     * This includes original triples where the entity is head, and virtual inverse triples where it's tail
     */
    fun getTriplesByEntity(entityId: Int): MutableList<MyTriple> {
        val result = mutableListOf<MyTriple>()
        
        // Add triples where entity is head (original triples)
        result.addAll(h2tripleList[entityId] ?: mutableListOf())
        
        // Add virtual inverse triples where entity is tail
        // Use the headRelationTails index to efficiently find incoming relations
        val incomingRelations = h2r2tSet[entityId] ?: mutableMapOf()
        incomingRelations.forEach { (relationId, tailEntities) ->
            if (IdManager.isInverseRelation(relationId)) {
                // This is an inverse relation, so entityId is actually tail in the original triple
                tailEntities.forEach { headEntity ->
                    result.add(MyTriple(entityId, relationId, headEntity))
                }
            }
        }
        
        return result
    }

    fun getTriplesByRelation(relation: Long): MutableList<MyTriple> {
        if (IdManager.isInverseRelation(relation)) {
            // For inverse relations, get original triples and create virtual inverse triples
            val originalRelation = IdManager.getInverseRelation(relation)
            val originalTriples = r2tripleSet[originalRelation] ?: mutableSetOf()
            return originalTriples.map { MyTriple(it.t, relation, it.h) }.toMutableList()
        } else {
            // For original relations, return as-is
            return r2tripleSet[relation]?.toMutableList() ?: mutableListOf()
        }
    }

    fun getRandomTripleByRelation(relation: Long): MyTriple? {
        if (IdManager.isInverseRelation(relation)) {
            // For inverse relations, get a random original triple and create virtual inverse triple
            val originalRelation = IdManager.getInverseRelation(relation)
            val originalTriple = r2tripleSet[originalRelation]?.randomOrNull(rand)
            return originalTriple?.let { MyTriple(it.t, relation, it.h) }
        } else {
            // For original relations, return as-is
            return r2tripleSet[relation]?.randomOrNull(rand)
        }
    }


//    fun getNRandomEntitiesByRelation(relation: Long, ifHead: Boolean, n: Int): MutableList<Int> {
//        return r2hSample[relation] ?: computeNRandomEntitiesByRelation(relation, ifHead, n)
//    }

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
        val relationTriples = if (IdManager.isInverseRelation(relation)) {
            // For inverse relations, get original triples
            val originalRelation = IdManager.getInverseRelation(relation)
            r2tripleSet[originalRelation]
        } else {
            r2tripleSet[relation]
        }
        
        if (relationTriples == null) {
            System.err.println("Internal reference to relation ${IdManager.getRelationString(relation)}, which is not indexed")
            System.err.println("Check if rule set and triple set fit together")
            return mutableListOf()
        }

        val entities = if (IdManager.isInverseRelation(relation)) {
            // For inverse relations, swap the meaning of head/tail
            relationTriples.map { triple ->
                if (ifHead) triple.t else triple.h // Swap for inverse relations
            }.distinct().toMutableList()
        } else {
            relationTriples.map { triple ->
                if (ifHead) triple.h else triple.t // Normal for original relations
            }.distinct().toMutableList()
        }

        val sampledEntities = (0 until n).map { entities.random(rand) }.toMutableList()
        
//        r2hSample[relation] = sampledEntities
        val targetMap = if (ifHead) r2hSample else r2tSample
        targetMap[relation] = sampledEntities
        
        return sampledEntities
    }

    val relations: MutableSet<Long> 
        get() {
            val allRelations = r2tripleSet.keys.toMutableSet()
            // Add inverse relations for all original relations
            r2tripleSet.keys.forEach { relationId ->
                if (!IdManager.isInverseRelation(relationId)) {
                    allRelations.add(IdManager.getInverseRelation(relationId))
                }
            }
            return allRelations
        }

    // Get only original relations (excluding inverse relations)
    val originalRelations: MutableSet<Long> 
        get() = r2tripleSet.keys.filter { !IdManager.isInverseRelation(it) }.toMutableSet()

    // Check if a triple exists, considering inverse relations
    fun isTrueWithInverse(head: Int, relation: Long, tail: Int): Boolean {
        // First check the direct relation
        if (isTrue(head, relation, tail)) return true
        
        // Then check using inverse relation
        val inverseRelationId = IdManager.getInverseRelation(relation)
        return if (inverseRelationId != null) {
            isTrue(tail, inverseRelationId, head)
        } else {
            false
        }
    }

    fun getHeadEntities(relation: Long, tail: Int): MutableSet<Int> {
        // 使用逆关系：tail --INVERSE_relation--> heads
        val inverseRelation = IdManager.getInverseRelation(relation)
        return h2r2tSet[tail]?.get(inverseRelation) ?: mutableSetOf()
    }

    fun getTailEntities(relation: Long, head: Int): MutableSet<Int> {
        // 直接查询：head --relation--> tails
        return h2r2tSet[head]?.get(relation) ?: mutableSetOf()
    }

    fun getEntities(relation: Long, value: Int, ifHead: Boolean): MutableSet<Int> =
        if (ifHead) getTailEntities(relation, value) else getHeadEntities(relation, value)

    fun getRandomEntity(relation: Long, value: Int, ifHead: Boolean) =
        if (ifHead) getRandomTailEntity(relation, value) else getRandomHeadEntity(relation, value)

    private fun getRandomHeadEntity(relation: Long, tail: Int): Int? {
        // 使用逆关系查询
        val inverseRelation = IdManager.getInverseRelation(relation)
        val list = h2r2tList[tail]?.get(inverseRelation) ?: run {
            val headSet = h2r2tSet[tail]?.get(inverseRelation)
            if (headSet?.isNotEmpty() == true) headSet.toMutableList() else return null
        }
        return list.randomOrNull(rand)
    }

    private fun getRandomTailEntity(relation: Long, head: Int): Int? {
        // 直接查询
        val list = h2r2tList[head]?.get(relation) ?: run {
            val tailSet = h2r2tSet[head]?.get(relation)
            if (tailSet?.isNotEmpty() == true) tailSet.toMutableList() else return null
        }
        return list.randomOrNull(rand)
    }

    fun isTrue(head: Int, relation: Long, tail: Int) =
        h2r2tSet[head]?.get(relation)?.contains(tail) == true

    fun isTrue(triple: MyTriple) = isTrue(triple.h, triple.r, triple.t)

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
        get() = h2tripleList.keys.toMutableSet()

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
