package tarmorn.data

import tarmorn.Settings
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.io.PrintWriter
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.Random
import java.util.Collections

class TripleSet() {
    val triples: MutableList<Triple>
    private val rand: Random


    var h2List: MutableMap<String, MutableList<Triple>>
    var t2List: MutableMap<String, MutableList<Triple>>
    var r2List: MutableMap<String, MutableList<Triple>>

    var h2r2tSet: MutableMap<String, MutableMap<String, MutableSet<String>>>

    // HashMap<String, HashMap<String, HashSet<String>>> headTail2RelationSet;
    var t2r2hSet: MutableMap<String, MutableMap<String, MutableSet<String>>>

    var h2r2tList: MutableMap<String, MutableMap<String, MutableList<String>>>
    var t2r2hList: MutableMap<String, MutableMap<String, MutableList<String>>>

    var frequentRelations: MutableSet<String> = mutableSetOf<String>()

    var r2hSample: MutableMap<String, MutableList<String>> = mutableMapOf<String, MutableList<String>>()
    var r2tSample: MutableMap<String, MutableList<String>> = mutableMapOf<String, MutableList<String>>()

    var relationCounter: MutableMap<String, Int> = mutableMapOf<String, Int>()


    constructor(filepath: String) : this() {
        this.readTriples(filepath, true)
        this.indexTriples()
        // needs to be called from outside this.setupListStructure();
    }

    constructor(filepath: String, ignore4Plus: Boolean) : this() {
        this.readTriples(filepath, ignore4Plus)
        this.indexTriples()
    }

    init {
        this.rand = Random()

        this.triples = mutableListOf<Triple>()
        this.h2List = mutableMapOf<String, MutableList<Triple>>()
        this.t2List = mutableMapOf<String, MutableList<Triple>>()
        this.r2List = mutableMapOf<String, MutableList<Triple>>()

        this.h2r2tSet = mutableMapOf<String, MutableMap<String, MutableSet<String>>>()
        // this.headTail2RelationSet = new HashMap<String, HashMap<String, HashSet<String>>>();
        this.t2r2hSet = mutableMapOf<String, MutableMap<String, MutableSet<String>>>()

        this.h2r2tList = mutableMapOf<String, MutableMap<String, MutableList<String>>>()
        this.t2r2hList = mutableMapOf<String, MutableMap<String, MutableList<String>>>()
    }

    fun addTripleSet(ts: TripleSet) {
        for (t in ts.triples) {
            this.addTriple(t)
        }
    }

    fun addTriples(triples: MutableList<Triple>) {
        for (t in triples) {
            this.addTriple(t)
        }
    }


    // fix stuff here
    fun addTriple(t: Triple) {
        if (this.isTrue(t)) {
            return
        } else {
            this.triples.add(t)
            //if (this.atriples.containsKey(t)) {
            //	this.atriples.remove(t);
            //}
            //else {
            this.addTripleToIndex(t)
            //}
        }
    }


    private fun indexTriples() {
        var tCounter: Long = 0
        var divisor: Long = 10000
        for (t in triples) {
            tCounter++
            if (tCounter % divisor == 0L) {
                println("* indexed " + tCounter + " triples")
                divisor *= 2
            }
            addTripleToIndex(t)
        }
        println("* set up index for " + this.r2List.keys.size + " relations, " + this.h2List.keys.size + " head entities, and " + this.t2List.keys.size + " tail entities")
    }

    fun setupListStructure() {
        //DecimalFormat df = new DecimalFormat("0.00");
        //println("MEMORY REQUIRED (before setupListStructure): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte at " + System.currentTimeMillis());

        print("* set up list structure for randomized access searches uring rule learning ... ")


        // head -> relation -> tails
        for (head in this.h2r2tSet.keys) {
            this.h2r2tList.put(head, mutableMapOf<String, MutableList<String>>())
            for (relation in this.h2r2tSet.get(head)!!.keys) {
                if (this.h2r2tSet.get(head)!!.get(relation)!!.size > 10) {
                    this.h2r2tList.get(head)!!
                        .put(relation, mutableListOf<String>())
                    this.h2r2tList.get(head)!!.get(relation)!!.addAll(this.h2r2tSet.get(head)!!.get(relation)!!)
                    sampleSubset(this.h2r2tList.get(head)!!.get(relation)!!)
                }
            }
        }
        // tail -> relation -> head
        for (tail in this.t2r2hSet.keys) {
            this.t2r2hList.put(tail, mutableMapOf<String, MutableList<String>>())
            for (relation in this.t2r2hSet.get(tail)!!.keys) {
                if (this.t2r2hSet.get(tail)!!.get(relation)!!.size > 10) {
                    this.t2r2hList.get(tail)!!
                        .put(relation, mutableListOf<String>())
                    this.t2r2hList.get(tail)!!.get(relation)!!.addAll(this.t2r2hSet.get(tail)!!.get(relation)!!)
                    sampleSubset(this.t2r2hList.get(tail)!!.get(relation)!!)
                }
            }
        }
        println(" done")

        //println("MEMORY REQUIRED (after setupListStructure): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte at " + System.currentTimeMillis());
    }

    private fun sampleSubset(list: MutableList<String>) {
        Collections.shuffle(list)
        while (list.size > 5000) {
            list.removeAt(list.size - 1)
        }
    }

    private fun addTripleToIndex(triple: Triple) {
        val h = triple.h
        val t = triple.t
        val r = triple.r
        // index head
        if (!this.h2List.containsKey(h)) this.h2List.put(h, mutableListOf<Triple>())
        this.h2List.get(h)!!.add(triple)
        // index tail
        if (!this.t2List.containsKey(t)) this.t2List.put(t, mutableListOf<Triple>())
        this.t2List.get(t)!!.add(triple)
        // index relation
        if (!this.r2List.containsKey(r)) this.r2List.put(r, mutableListOf<Triple>())
        this.r2List.get(r)!!.add(triple)
        // index head-relation => tail
        if (!this.h2r2tSet.containsKey(h)) this.h2r2tSet.put(h, mutableMapOf<String, MutableSet<String>>())
        if (!this.h2r2tSet.get(h)!!.containsKey(r)) this.h2r2tSet.get(h)!!.put(r, mutableSetOf<String>())
        this.h2r2tSet.get(h)!!.get(r)!!.add(t)
        // index tail-relation => head
        if (!this.t2r2hSet.containsKey(t)) this.t2r2hSet.put(t, mutableMapOf<String, MutableSet<String>>())
        if (!this.t2r2hSet.get(t)!!.containsKey(r)) this.t2r2hSet.get(t)!!.put(r, mutableSetOf<String>())
        this.t2r2hSet.get(t)!!.get(r)!!.add(h)
    }


    private fun readTriples(filepath: String, ignore4Plus: Boolean) {
        val file = (File(filepath)).toPath()
        // Charset charset = Charset.forName("US-ASCII");
        val charset = Charset.forName("UTF8")
        var line: String? = null
        var lineCounter: Long = 0
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
                        this.triples.add(t)
                    }
                }
            }
        } catch (x: IOException) {
            System.err.format("IOException: %s%n", x)
            System.err.format("Error occured for line: " + line + " LINE END")
        }
        // Collections.shuffle(this.triples);
        println("* read " + this.triples.size + " triples")
    }


    fun getTriplesByHead(head: String): MutableList<Triple> {
        return if (this.h2List.containsKey(head)) this.h2List[head]!!
        else mutableListOf<Triple>()
    }

    fun getTriplesByHeadNotTail(headOrTail: String, byHeadNotTail: Boolean): MutableList<Triple> {
        return if (byHeadNotTail) this.getTriplesByHead(headOrTail)
        else this.getTriplesByTail(headOrTail)
    }

    fun getNTriplesByHead(head: String, n: Int): MutableList<Triple> {
        if (this.h2List.containsKey(head)) {
            if (this.h2List[head]!!.size <= n) return this.h2List[head]!!
            else {
                val chosen = mutableListOf<Triple>()
                for (i in 0..<n) {
                    val index = this.rand.nextInt(this.h2List.get(head)!!.size)
                    chosen.add(this.h2List.get(head)!!.get(index))
                }
                return chosen
            }
        } else return mutableListOf<Triple>()
    }


    fun getTriplesByTail(tail: String): MutableList<Triple> {
        return if (this.t2List.containsKey(tail)) this.t2List[tail]!!
        else  mutableListOf<Triple>()
    }

    fun getNTriplesByTail(tail: String, n: Int): MutableList<Triple> {
        if (this.t2List.containsKey(tail)) {
            if (this.t2List[tail]!!.size <= n) return this.t2List[tail]!!
            else {
                val chosen = mutableListOf<Triple>()
                for (i in 0..<n) {
                    val index = this.rand.nextInt(this.t2List.get(tail)!!.size)
                    chosen.add(this.t2List.get(tail)!!.get(index))
                }
                return chosen
            }
        } else return mutableListOf<Triple>()
    }


    fun getTriplesByRelation(relation: String): MutableList<Triple> {
        return if (this.r2List.containsKey(relation)) this.r2List.get(relation)!!
        else  mutableListOf<Triple>()
    }

    fun getRandomTripleByRelation(relation: String): Triple? {
        if (this.r2List.containsKey(relation)) {
            return this.r2List.get(relation)!!.get(this.rand.nextInt(this.r2List.get(relation)!!.size))
        }
        return null
    }


    fun getNRandomEntitiesByRelation(relation: String, ifHead: Boolean, n: Int): MutableList<String> {
        if (ifHead) {
            if (r2hSample.containsKey(relation)) return r2hSample.get(relation)!!
        } else {
            if (r2tSample.containsKey(relation)) return r2tSample.get(relation)!!
        }
        return computeNRandomEntitiesByRelation(relation, ifHead, n)
    }


    fun precomputeNRandomEntitiesPerRelation(n: Int) {
        print("* precomputing random starting points for each relation/direction for the beam search ...")
        for (relation in this.relations) {
            this.computeNRandomEntitiesByRelation(relation, true, n)
            this.computeNRandomEntitiesByRelation(relation, false, n)
        }
    }


    @Synchronized
    private fun computeNRandomEntitiesByRelation(relation: String, ifHead: Boolean, n: Int): MutableList<String> {
        if (this.r2List.containsKey(relation)) {
            val entities = mutableListOf<String>()
            val entitiesAsSet = mutableSetOf<String>()
            for (triple in this.r2List.get(relation)!!) {
                val value = triple.getValue(ifHead)
                if (!entitiesAsSet.contains(value)) {
                    entitiesAsSet.add(value)
                    entities.add(value)
                }
            }
            val sampledEntities = mutableListOf<String>()
            for (i in 0..<n) {
                val entity = entities.get(rand.nextInt(entities.size))
                sampledEntities.add(entity)
            }
            if (ifHead) this.r2hSample.put(relation, sampledEntities)
            else this.r2tSample.put(relation, sampledEntities)
            return sampledEntities
        } else {
            System.err.println("something is strange, internal reference to relation " + relation + ", which is not indexed")
            System.err.println("check if rule set and triple set fit together")
            return MutableList<String>(0) { "" }
        }
    }

    /**
     * Select randomly n entities that appear in head (or tail) position of a triple using a given relation.
     * More frequent entities appear more frequent. This is the difference compared to the method computeNRandomEntitiesByRelation.
     *
     * @param relation
     * @param ifHead
     * @param n
     * @return
     */
    fun selectNRandomEntitiesByRelation(relation: String, ifHead: Boolean, n: Int): MutableList<String>? {
        if (this.r2List.containsKey(relation)) {
            val entities = mutableListOf<String>()
            var j = 0
            for (triple in this.r2List.get(relation)!!) {
                j++
                val value = triple.getValue(ifHead)
                entities.add(value)
                if (j == n) break
            }
            val sampledEntities = mutableListOf<String>()
            for (i in 0..<n) {
                val entity = entities.get(rand.nextInt(entities.size))
                sampledEntities.add(entity)
            }
            return sampledEntities
        } else {
            System.err.println("something is strange, internal reference to relation " + relation + ", which is not indexed")
            System.err.println("check if rule set and triple set fit together")
            return null
        }
    }


    val relations: MutableSet<String>
        get() = this.r2List.keys

    fun getHeadEntities(relation: String, tail: String): MutableSet<String> {
        if (t2r2hSet.get(tail) != null) {
            if (t2r2hSet.get(tail)!!.get(relation) != null) {
                return t2r2hSet.get(tail)!!.get(relation)!!
            }
        }
        return mutableSetOf<String>()
    }

    fun getTailEntities(relation: String, head: String): MutableSet<String> {
        if (h2r2tSet.get(head) != null) {
            if (h2r2tSet.get(head)!!.get(relation) != null) {
                return h2r2tSet.get(head)!!.get(relation)!!
            }
        }
        return mutableSetOf<String>()
    }

    /**
     * Returns those values for which the relation holds for a given value. If the ifHead is
     * set to true, the value is interpreted as head value and the corresponding tails are returned.
     * Otherwise, the corresponding heads are returned.
     *
     * @param relation The specified relation.
     * @param value The value interpreted as given head or tail.
     * @param ifHead Whether to interpret the value as head and not as tail (false interprets as tail).
     * @return The resulting values.
     */
    fun getEntities(relation: String, value: String, ifHead: Boolean): MutableSet<String> {
        if (ifHead) return this.getTailEntities(relation, value)
        else return this.getHeadEntities(relation, value)
    }

    /**
     * Returns a random value for which the relation holds for a given value. If the ifHead is
     * set to true, the value is interpreted as head value and the corresponding tails are returned.
     * Otherwise, the corresponding heads are returned.
     *
     * @param relation The specified relation.
     * @param value The value interpreted as given head or tail.
     * @param ifHead Whether to interpret the value as head and not as tail (false interprets as tail).
     * @return The resulting value or null if no such value exists.
     */
    fun getRandomEntity(relation: String, value: String, ifHead: Boolean): String? {
        if (ifHead) return this.getRandomTailEntity(relation, value)
        else return this.getRandomHeadEntity(relation, value)
    }

    private fun getRandomHeadEntity(relation: String, tail: String): String? {
        if (!t2r2hList.containsKey(tail)) return null
        var list = this.t2r2hList.get(tail)!!.get(relation)
        if (list == null) if (this.t2r2hSet.get(tail)!!.get(relation) != null && this.t2r2hSet.get(tail)!!
                .get(relation)!!.size > 0
        ) {
            list = mutableListOf<String>()
            list.addAll(this.t2r2hSet.get(tail)!!.get(relation)!!)
        } else {
            return null
        }
        return list.get(this.rand.nextInt(list.size))
    }

    private fun getRandomTailEntity(relation: String, head: String): String? {
        if (!h2r2tList.containsKey(head)) return null
        var list = this.h2r2tList.get(head)!!.get(relation)
        if (list == null) {
            if (this.h2r2tSet.get(head)!!.get(relation) != null && this.h2r2tSet.get(head)!!.get(relation)!!.size > 0) {
                list = mutableListOf<String>()
                list.addAll(this.h2r2tSet.get(head)!!.get(relation)!!)
            } else {
                return null
            }
        }
        return list.get(this.rand.nextInt(list.size))
    }

    /*
	public Set<String> getRelations(String head, String tail) {
		if (headTail2RelationSet.get(head) != null) {
			if (headTail2RelationSet.get(head).get(tail) != null) {
				return headTail2RelationSet.get(head).get(tail);
			}
		}
		return new HashSet<String>();
	}
	*/
    fun isTrue(head: String, relation: String, tail: String): Boolean {
        if (t2r2hSet.get(tail) != null) {
            if (t2r2hSet.get(tail)!!.get(relation) != null) {
                return t2r2hSet.get(tail)!!.get(relation)!!.contains(head)
            }
        }
        return false
    }


    fun isTrue(triple: Triple): Boolean {
        return this.isTrue(triple.h, triple.r, triple.t)
    }


    fun compareTo(that: TripleSet, thisId: String, thatId: String) {
        println("* Comparing two triple sets")
        var counter = 0
        for (t in triples) {
            if (that.isTrue(t)) {
                counter++
            }
        }

        println("* size of " + thisId + ": " + this.triples.size)
        println("* size of " + thatId + ": " + that.triples.size)
        println("* size of intersection: " + counter)
    }

    fun getIntersectionWith(that: TripleSet): TripleSet {
        val ts = TripleSet()
        for (t in triples) {
            if (that.isTrue(t)) {
                ts.addTriple(t)
            }
        }
        return ts
    }

    fun minus(that: TripleSet): TripleSet {
        val ts = TripleSet()
        for (t in triples) {
            if (!that.isTrue(t)) {
                ts.addTriple(t)
            }
        }
        return ts
    }

    val numOfEntities: Int
        get() = h2List.keys.size + t2List.keys.size

    fun determineFrequentRelations(coverage: Double) {
        var allCounter = 0
        for (t in this.triples) {
            allCounter++
            val r = t.r
            if (relationCounter.containsKey(r)) {
                relationCounter.put(r, relationCounter.get(r)!! + 1)
            } else {
                relationCounter.put(r, 1)
            }
        }

        val counts = mutableListOf<Int>()
        counts.addAll(relationCounter.values)
        Collections.sort(counts)
        var countUp = 0
        var border = 0
        for (c in counts) {
            countUp = countUp + c
            //println("countUp: " + countUp);
            //println("c: " + c);
            if (((allCounter - countUp).toDouble() / allCounter.toDouble()) < coverage) {
                border = c
                break
            }
        }


        //println("Number of all relations: " + relationCounter.size());
        //println("Relations covering " + coverage + " of all triples");
        for (r in relationCounter.keys) {
            if (relationCounter.get(r)!! > border) {
                frequentRelations.add(r)
                //println(r + " (used in " + relationCounter.get(r) + " triples)");
            }
        }
        //println("Number of frequent (covering " + coverage+ " of all) relations: " + frequentRelations.size());
    }

    fun isFrequentRelation(relation: String): Boolean {
        return this.frequentRelations.contains(relation)
    }

    val entities: MutableSet<String>
        /*
            public boolean existsPath(String x, String y, int pathLength) {
                if (pathLength == 1) {
                    if (this.getRelations(x, y).size() > 0) {
                        return true;
                    }
                    if (this.getRelations(y, x).size() > 0) {
                        return true;
                    }
                    return false;
                }
                if (pathLength == 2) {
                    Set<String> hop1x = new HashSet<String>();
                    for (Triple hx : this.getTriplesByHead(x)) { hop1x.add(hx.getTail()); }
                    for (Triple tx : this.getTriplesByTail(x)) { hop1x.add(tx.getHead()); }
        
                    for (Triple hy : this.getTriplesByHead(y)) {
                        if (hop1x.contains(hy.getTail())) return true;
                    }
                    for (Triple ty : this.getTriplesByTail(y)) {
                        if (hop1x.contains(ty.getHead()))  return true;
                    }
                    return false;
                }
                if (pathLength > 2 ) {
                    System.err.println("checking the existence of a path longer than 2 is so far not supported");
                    System.exit(-1);
                    
                }
                return false;
                
            }
            */
        get() {
            val entities = mutableSetOf<String>()
            entities.addAll(h2List.keys)
            entities.addAll(t2List.keys)
            return entities
        }

    @Throws(FileNotFoundException::class)
    fun write(filepath: String) {
        val pw = PrintWriter(filepath)

        for (t in this.triples) {
            pw.println(t)
        }
        pw.flush()
        pw.close()
    }

    fun size(): Int {
        return this.triples.size
    }


    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            // TripleSet ts = new TripleSet("data/DB500/ftest.txt");
        }
    }
}
