package tarmorn.eval

import tarmorn.data.Triple
import java.io.*
import java.nio.charset.StandardCharsets
import java.util.Collections

// import tarmorn.rescore.AlphaBeta;
class ResultSet : Iterable<CompletionResult> {
    var results: MutableMap<String, CompletionResult>
    var name: String = ""

    private var containsConfidences = false

    constructor(that: ResultSet, relation: String) {
        this.containsConfidences = that.containsConfidences
        this.results = mutableMapOf<String, CompletionResult>()
        for (triple in that.results.keys) {
            if (triple.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[1] == relation) {
                this.results.put(triple, that.results.get(triple)!!)
            }
        }
    }


    constructor() {
        this.results = mutableMapOf<String, CompletionResult>()
    }

    constructor(name: String, containsConfidences: Boolean, k: Int) : this(name, name, containsConfidences, k)

    fun getCompletionResult(triple: String): CompletionResult {
        val cr: CompletionResult = this.results.get(triple)!!
        return cr
    }

    fun getCompletionResult(triple: Triple): CompletionResult? {
        val cr = this.results.get(triple.toString())
        return cr
    }

    val triples: MutableSet<String>
        get() = this.results.keys

    constructor(name: String, filePath: String, containsConfidences: Boolean, k: Int, adjust: Boolean) : this(
        name,
        filePath,
        containsConfidences,
        k
    ) {
        if (adjust) this.adjust()
    }

    @JvmOverloads
    constructor(name: String, filePath: String, containsConfidences: Boolean = false, k: Int = 0) {
        println("* loading result set at " + filePath)
        this.containsConfidences = containsConfidences
        this.name = name
        this.results = mutableMapOf<String, CompletionResult>()
        var counter: Long = 0
        val stepsize: Long = 100000
        var file: File? = null
        try {
            file = File(filePath)

            // FileReader fileReader = new FileReader(file);

            // FileInputStream i = null;
            val bufferedReader = BufferedReader(InputStreamReader(FileInputStream(file), StandardCharsets.UTF_8))
            var tripleLine: String
            while ((bufferedReader.readLine().also { tripleLine = it }) != null) {
                counter++
                if (counter % stepsize == 0L) println("* parsed " + counter + " lines of results file")
                if (tripleLine!!.length < 3) continue
                val cr = CompletionResult(tripleLine)
                var headLine = bufferedReader.readLine()
                var tailLine = bufferedReader.readLine()
                var tempLine = ""
                if (headLine.startsWith("Tails:")) {
                    // println("reversed");
                    tempLine = headLine
                    headLine = tailLine
                    tailLine = tempLine
                }
                if (!applyThreshold) {
                    cr.addHeadResults(getResultsFromLine(headLine.substring(7)), k)
                    cr.addHeadConfidences(getConfidencesFromLine(headLine.substring(7)), k)
                    cr.addTailResults(getResultsFromLine(tailLine.substring(7)), k)
                    cr.addTailConfidences(getConfidencesFromLine(tailLine.substring(7)), k)
                } else {
                    cr.addHeadResults(getThresholdedResultsFromLine(headLine.substring(7)), k)
                    cr.addHeadConfidences(getThresholdedConfidencesFromLine(headLine.substring(7)), k)
                    cr.addTailResults(getThresholdedResultsFromLine(tailLine.substring(7)), k)
                    cr.addTailConfidences(getThresholdedConfidencesFromLine(tailLine.substring(7)), k)
                }
                this.results.put(
                    tripleLine.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()[0],
                    cr
                )
            }
            bufferedReader.close()
        } catch (e: IOException) {
            System.err.println("problem related to file " + file + ".")
            e.printStackTrace()
        }
    }

    fun extendWith(rs: ResultSet, k: Int, factor: Double) {
        for (t in this.results.keys) {
            val thisResult: CompletionResult = this.results.get(t)!!
            val thatResult = rs.results.get(t)
            thisResult.extendWith(thatResult!!, k, factor)
        }
    }

    @Throws(FileNotFoundException::class)
    fun printAsTripleSet(path: String) {
        val pw = PrintWriter(path)
        for (line in this.results.keys) {
            val token = line.split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val cr: CompletionResult = this.results.get(line)!!
            var i = 0
            for (h in cr.heads) {
                pw.println(h + " " + token[1] + " " + token[2])
                i++
            }
            i = 0
            for (t in cr.tails) {
                pw.println(token[0] + " " + token[1] + " " + t)
                i++
            }
            pw.flush()
        }
        pw.close()
    }

    @Throws(FileNotFoundException::class)
    fun printAsWeightedTripleSet(path: String) {
        val pw = PrintWriter(path)
        for (line in this.results.keys) {
            val token = line.split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val cr: CompletionResult = this.results.get(line)!!
            var i = 0
            for (h in cr.heads) {
                val dh = cr.headConfidences.get(i)
                pw.println(h + "\t" + token[1] + "\t" + token[2] + "\t" + dh)
                i++
            }
            i = 0
            for (t in cr.tails) {
                val dt = cr.tailConfidences.get(i)
                pw.println(token[0] + "\t" + token[1] + "\t" + t + "\t" + dt)
                i++
            }
            pw.flush()
        }
        pw.close()
    }


    private fun getThresholdedResultsFromLine(rline: String): Array<String> {
        if (!containsConfidences) {
            return rline.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        } else {
            var t: String = ""
            var cS = ""
            val token = rline.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            // String[] tokenx = new String[token.length / 2];
            val tokenx = mutableListOf<String>()
            for (i in 0..<token.size / 2) {
                t = token[i * 2]
                cS = token[i * 2 + 1]
                val c = cS.toDouble()
                if (c > threshold) {
                    tokenx.add(t)
                } else {
                    break
                }
            }
            val tokenxx = tokenx.toTypedArray<String>() as Array<String>
            return tokenxx
        }
    }

    private fun getThresholdedConfidencesFromLine(rline: String?): Array<Double> {
        if (!containsConfidences) {
            System.err.println("there are no confidences, you cannot retrieve them (line: " + rline + ")")
            return Array<Double>(0, { 0.0 })
        } else {
            val t = ""
            var cS = ""
            val token = rline!!.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            // String[] tokenx = new String[token.length / 2];
            val tokenx = mutableListOf<Double>()
            for (i in 0..<token.size / 2) {
                // t = token[i*2];

                cS = token[i * 2 + 1]
                val c = cS.toDouble()
                if (c > threshold) {
                    tokenx.add(c)
                } else {
                    break
                }
            }
            val tokenxx = tokenx.toTypedArray<Double>()
            return tokenxx
        }
    }

    private fun getResultsFromLine(rline: String): Array<String> {
        if (!containsConfidences) {
            return rline.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        } else {
            val token = rline.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val tokenx = Array(token.size / 2) { i ->
                token[i * 2]
            }
            return tokenx
        }
    }

    private fun getConfidencesFromLine(rline: String?): Array<Double> {
        if (!containsConfidences) {
            System.err.println("there are no confidences, you cannot retrieve them (line: " + rline + ")")
            return Array<Double>(0, { 0.0 })
        } else {
            val token = rline!!.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val tokenx = Array(token.size / 2) { i ->
                token[i * 2 + 1].toDouble()
            }
            return tokenx
        }
    }

    fun getHeadCandidates(triple: String): MutableList<String> {
        try {
            // println("head: " + triple);
            val cr = this.results.get(triple)
            if (cr == null && triple.contains("\t")) triple.replace("\t".toRegex(), " ")
            // else if (cr == null && triple.contains(" ")) triple.replaceAll(" ", "\t");
            // cr = this.results.get(triple);
            if (cr == null) {
                // println("ARGGG");
                // println(triple);
            }
            return cr!!.heads
        } catch (e: RuntimeException) {
            return mutableListOf<String>()
        }
    }


    fun getTailCandidates(triple: String): MutableList<String> {
        // println("tail: " + triple);
        try {
            val cr = this.results.get(triple)
            //if (cr == null && triple.contains("\t")) triple.replaceAll("\t", " ");
            //else if (cr == null && triple.contains(" ")) triple.replaceAll(" ", "\t");
            // cr = this.results.get(triple);
            if (cr == null) {
                System.err.println("cpould not find required triple (" + triple + ")in results set")
                for (k in this.results.keys) {
                    println(k)
                }
                System.exit(0)
            }
            return cr!!.tails
        } catch (e: RuntimeException) {
            return mutableListOf<String>()
        }
    }


    fun getHeadConfidences(triple: String): MutableList<Double> {
        try {
            val cr: CompletionResult = this.results.get(triple)!!
            return cr.headConfidences
        } catch (e: RuntimeException) {
            return mutableListOf<Double>()
        }
    }

    fun getTailConfidences(triple: String): MutableList<Double> {
        try {
            val cr: CompletionResult = this.results.get(triple)!!
            return cr.tailConfidences
        } catch (e: RuntimeException) {
            return mutableListOf<Double>()
        }
    }


    override fun iterator(): MutableIterator<CompletionResult> {
        return this.results.values.iterator()
    }

    @Throws(FileNotFoundException::class)
    fun write(filepath: String) {
        val pw = PrintWriter(filepath)

        for (cr in results.values) {
            cr.write(pw)
        }
        pw.flush()
        pw.close()
    }

    /**
     * Reorders the candidates in this results set by the order of that result set.
     * Each candidate from this result set, which is not specified in that result set is put to the end of the ranking.
     * If there are several such candidates they are are ordered at the end according to their original ordering.
     *
     * The scores that are used for the ordering constructed by by taking equdistant numbers from 1.0 to 0.5
     *
     */
    fun reorder(that: ResultSet) {
        for (tripleAsString in this.results.keys) {
            val thisCr = this.getCompletionResult(tripleAsString)
            val thatCr = that.getCompletionResult(tripleAsString)

            val reorderedCr = CompletionResult(tripleAsString)


            val reorderedHeads = mutableListOf<String>()
            val reorderedHeadConfidences = mutableListOf<Double>()
            reorderLists(reorderedHeads, reorderedHeadConfidences, thisCr.heads, thatCr.heads)
            reorderedCr.heads = reorderedHeads
            reorderedCr.headConfidences = reorderedHeadConfidences

            val reorderedTails = mutableListOf<String>()
            val reorderedTailConfidences = mutableListOf<Double>()
            reorderLists(reorderedTails, reorderedTailConfidences, thisCr.tails, thatCr.tails)
            reorderedCr.tails = reorderedTails
            reorderedCr.tailConfidences = reorderedTailConfidences

            this.results.put(tripleAsString, reorderedCr)
        }
    }

    /**
     * Reorders the candidates in this results by combining the confidences in this and that result set.
     * The confidences in this result set are modified in the following way. For each candidate in this
     * result set, the position in that result set is determined. Let p be that position. Then the confidence
     * of this candidate is multiplied by (1 + alpha)/(p + alpha). The higher alpha, the lower is the
     * impact of the ordering in that result set.
     *
     */
    fun reorderWeighted(that: ResultSet, alpha: Int, beta: Double): ResultSet {
        val rs = ResultSet()
        for (tripleAsString in this.results.keys) {
            val thisCr = this.getCompletionResult(tripleAsString)
            val thatCr = that.getCompletionResult(tripleAsString)

            val reorderedCr = CompletionResult(tripleAsString)


            val reorderedHeads = mutableListOf<String>()
            val reorderedHeadConfidences = mutableListOf<Double>()
            reorderListsWeighted(
                reorderedHeads,
                reorderedHeadConfidences,
                thisCr.heads,
                thisCr.headConfidences,
                thatCr.heads,
                alpha,
                beta
            )
            reorderedCr.heads = reorderedHeads
            reorderedCr.headConfidences = reorderedHeadConfidences

            val reorderedTails = mutableListOf<String>()
            val reorderedTailConfidences = mutableListOf<Double>()
            reorderListsWeighted(
                reorderedTails,
                reorderedTailConfidences,
                thisCr.tails,
                thisCr.tailConfidences,
                thatCr.tails,
                alpha,
                beta
            )
            reorderedCr.tails = reorderedTails
            reorderedCr.tailConfidences = reorderedTailConfidences


            // this.results.put(tripleAsString, reorderedCr);
            rs.results.put(tripleAsString, reorderedCr)
        }
        return rs
    }


    private fun reorderLists(
        reorderedCandidates: MutableList<String>,
        reorderedConfidences: MutableList<Double>,
        thisCandidates: MutableList<String>,
        thatCandidates: MutableList<String>
    ) {
        val reorderedCandidatesHashed = HashSet<String>()
        for (i in thatCandidates.indices) {
            val candidate = thatCandidates.get(i)
            if (thisCandidates.contains(candidate)) {
                reorderedCandidates.add(candidate)
                reorderedCandidatesHashed.add(candidate)
            }
        }
        for (i in thisCandidates.indices) {
            val candidate = thisCandidates.get(i)
            if (!reorderedCandidatesHashed.contains(candidate)) {
                reorderedCandidates.add(candidate)
            }
        }

        val stepsize = 0.5 / reorderedCandidates.size.toDouble()
        for (i in reorderedCandidates.indices) {
            reorderedConfidences.add(1.0 - (i * stepsize))
        }
    }

    private fun reorderListsWeighted(
        reorderedCandidates: MutableList<String>,
        reorderedConfidences: MutableList<Double>,
        thisCandidates: MutableList<String>,
        thisConfidences: MutableList<Double>,
        thatCandidates: MutableList<String>,
        alpha: Int,
        beta: Double
    ) {
        val map = LinkedHashMap<String, Double>()

        for (i in thisCandidates.indices) {
            val candidate = thisCandidates.get(i)
            val givenConfidence: Double = thisConfidences.get(i)!!
            var thatFactor = 0.0
            val indexInThat = thatCandidates.lastIndexOf(candidate)
            var pos = 0
            if (indexInThat == -1) {
                thatFactor = (1.0 + alpha) / (thatCandidates.size + 1 + alpha).toDouble()
                pos = thatCandidates.size + 1
            } else {
                thatFactor = (1.0 + alpha) / (indexInThat + 1 + alpha).toDouble()
                pos = indexInThat + 1
            }
            // println();
            val score = beta * (givenConfidence * thatFactor) + (1.0 - beta) * (1.0 / pos)

            // double score = givenConfidence * thatFactor;
            map.put(candidate, score)
        }
        orderByValueDescending(map)
        for (e in map.entries) {
            reorderedCandidates.add(e.key)
            reorderedConfidences.add(e.value)
        }
    }

    /**
     * Reorders the results set according to the specified confidences.
     */
    fun adjust() {
        for (task in this.results.keys) {
            val cr: CompletionResult = this.results.get(task)!!

            val map = LinkedHashMap<String, Double>()
            for (i in cr.heads.indices) map.put(cr.heads.get(i), cr.headConfidences.get(i))
            orderByValueDescending(map)

            val reorderedCandidates = mutableListOf<String>()
            val reorderedConfidences = mutableListOf<Double>()
            for (e in map.entries) {
                reorderedCandidates.add(e.key)
                reorderedConfidences.add(e.value)
            }
            cr.heads = reorderedCandidates
            cr.headConfidences = reorderedConfidences


            map.clear()
            for (i in cr.tails.indices) map.put(cr.tails.get(i), cr.tailConfidences.get(i))
            orderByValueDescending(map)
            val reorderedCandidates2 = mutableListOf<String>()
            val reorderedConfidences2 = mutableListOf<Double>()
            for (e in map.entries) {
                reorderedCandidates2.add(e.key)
                reorderedConfidences2.add(e.value)
            }
            cr.tails = reorderedCandidates2
            cr.tailConfidences = reorderedConfidences2
        }
    }


    companion object {
        var applyThreshold: Boolean = false
        var threshold: Double = 0.0

        @Throws(FileNotFoundException::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val map = LinkedHashMap<String, Double>()

            map.put("a", 0.8)
            map.put("b", 0.7)
            map.put("c", 0.9)
            map.put("d", 0.1)
            map.put("e", 2.7)
            map.put("f", 0.88)
            map.put("g", 0.1222)


            orderByValueDescending(map)

            for (e in map.entries) {
                println(e)
            }


            // ResultSet rs = new ResultSet("../KBCTestdataGenerator/experiments/anyburl/numbers-d3-08-p08-p04-p00-uc-predictions", true, 10);
            //for (CompletionResult cr : rs) {
            //	println(cr);
            //}
        }


        // is this one still in use?
        /*
	public ResultSet reorderWeighted(ResultSet that, HashMap<String, AlphaBeta> relation2AlphaBeta) {
		ResultSet rs = new ResultSet();
		for (String tripleAsString : this.results.keySet()) {
			
			String r = tripleAsString.split(" ")[1];
			int alpha = 0;
			double beta = 0;
			if (relation2AlphaBeta.containsKey(r)) {
				AlphaBeta ab = relation2AlphaBeta.get(r);
				beta =ab.beta;
			}
			else {
				println(">>> relation " + r + " has no alpha/beta scoring: set to alpha = 0. beta = 1.0");
				alpha = 0;
				beta = 1.0;
			}
			

			
			CompletionResult thisCr = this.getCompletionResult(tripleAsString);
			CompletionResult thatCr = that.getCompletionResult(tripleAsString);
			
			CompletionResult reorderedCr = new CompletionResult(tripleAsString);

			
			ArrayList<String> reorderedHeads = new ArrayList<String>();
			ArrayList<Double> reorderedHeadConfidences = new ArrayList<Double>();
			reorderListsWeighted(reorderedHeads, reorderedHeadConfidences, thisCr.heads, thisCr.headConfidences, thatCr.heads, alpha, beta);
			reorderedCr.heads = reorderedHeads);
			reorderedCr.headConfidences = reorderedHeadConfidences);
			
			ArrayList<String> reorderedTails = new ArrayList<String>();
			ArrayList<Double> reorderedTailConfidences = new ArrayList<Double>();
			reorderListsWeighted(reorderedTails, reorderedTailConfidences, thisCr.tails, thisCr.tailConfidences, thatCr.tails, alpha, beta);
			reorderedCr.tails = reorderedTails);
			reorderedCr.tailConfidences = reorderedTailConfidences);
			
			// this.results.put(tripleAsString, reorderedCr);
			
			rs.results.put(tripleAsString, reorderedCr);
			
		}
		return rs;
	}
	*/
        private fun orderByValueDescending(m: LinkedHashMap<String, Double>) {
            val entries: MutableList<MutableMap.MutableEntry<String, Double>> =
                ArrayList<MutableMap.MutableEntry<String, Double>>(m.entries)


            Collections.sort<MutableMap.MutableEntry<String, Double>>(
                entries,
                object : Comparator<MutableMap.MutableEntry<String, Double>> {
                    override fun compare(
                        lhs: MutableMap.MutableEntry<String, Double>,
                        rhs: MutableMap.MutableEntry<String, Double>
                    ): Int {
                        if (lhs.value!! - rhs.value!! > 0) return -1
                        else {
                            if (lhs.value!! - rhs.value!! == 0.0) return 0
                            else return 1
                        }
                    }
                })

            m.clear()
            for (e in entries) {
                m.put(e.key, e.value)
            }
        }
    }
}