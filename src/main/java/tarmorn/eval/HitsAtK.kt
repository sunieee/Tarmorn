package tarmorn.eval

import tarmorn.data.Triple
import tarmorn.data.TripleSet
import java.text.DecimalFormat
import java.text.NumberFormat
import java.util.Locale

class HitsAtK {
    private val filterSets = mutableListOf<TripleSet>()

    private var hitsADnTail = IntArray(ATKMAX)
    private var hitsADnTailFiltered = IntArray(ATKMAX)
    private var counterTail = 0
    private var counterTailCovered = 0

    private var headRanks = mutableListOf<Int>()
    private var tailRanks = mutableListOf<Int>()

    private var hitsADnHead = IntArray(ATKMAX)
    private var hitsADnHeadFiltered = IntArray(ATKMAX)
    private var counterHead = 0
    private var counterHeadCovered = 0

    private var am: AlternativeMentions? = null

    // private StringBuffer storedResult = new StringBuffer("");
    init {
        df.applyPattern("0.0000")
    }


    fun reset() {
        // reset head
        this.hitsADnHead = IntArray(ATKMAX)
        this.hitsADnHeadFiltered = IntArray(ATKMAX)
        this.counterHead = 0
        this.counterHeadCovered = 0
        // reset tail
        this.hitsADnTail = IntArray(ATKMAX)
        this.hitsADnTailFiltered = IntArray(ATKMAX)
        this.counterTail = 0
        this.counterTailCovered = 0

        this.headRanks = mutableListOf<Int>()
        this.tailRanks = mutableListOf<Int>()
    }

    val approxMRR: String
        get() {
            var mrr = 0.0
            var hk = 0.0
            var hk_prev = 0.0
            var hk_diff = 0.0
            for (k in 0..<ATKMAX) {
                hk =
                    ((hitsADnHeadFiltered[k] + hitsADnTailFiltered[k]).toDouble() / (counterHead + counterTail).toDouble())
                hk_diff = hk - hk_prev
                mrr += hk_diff * (1.0 / (k + 1))
                // println("k=" + (k+1) + " hk=" + hk);
                hk_prev = hk
            }
            // double mrr_up = mrr + (1.0 - hk_prev) * (1.0 / 11.0);
            val mrr_low = mrr
            return "" + f(mrr_low)
        }

    val mRR: Double
        get() {
            var mrr = 0.0
            var hk = 0.0
            var hk_prev = 0.0
            var hk_diff = 0.0
            for (k in 0..<ATKMAX) {
                hk =
                    ((hitsADnHeadFiltered[k] + hitsADnTailFiltered[k]).toDouble() / (counterHead + counterTail).toDouble())
                hk_diff = hk - hk_prev
                mrr += hk_diff * (1.0 / (k + 1))
                // println("k=" + (k+1) + " hk=" + hk);
                hk_prev = hk
            }
            // double mrr_up = mrr + (1.0 - hk_prev) * (1.0 / 11.0);
            return mrr
        }


    val mRRHeads: Double
        get() {
            var mrr = 0.0
            var hk = 0.0
            var hk_prev = 0.0
            var hk_diff = 0.0
            for (k in 0..<ATKMAX) {
                hk = (hitsADnHeadFiltered[k] / counterHead.toDouble())
                hk_diff = hk - hk_prev
                mrr += hk_diff * (1.0 / (k + 1))
                hk_prev = hk
            }
            return mrr
        }


    val mRRTails: Double
        get() {
            var mrr = 0.0
            var hk = 0.0
            var hk_prev = 0.0
            var hk_diff = 0.0
            for (k in 0..<ATKMAX) {
                hk = (hitsADnTailFiltered[k] / counterTail.toDouble())
                hk_diff = hk - hk_prev
                mrr += hk_diff * (1.0 / (k + 1))
                hk_prev = hk
            }
            return mrr
        }

    fun getHitsAtK(k: Int): String {
        // println("getHitsAtK with k = " + k);
        // println("hitsADnHeadFiltered[k]: " + hitsADnHeadFiltered[k]);
        // println("hitsADnTailFiltered[k]: " + hitsADnTailFiltered[k]);
        // for (int i = 0; i < hitsADnTailFiltered.length; i++) {
        // 	println("  >>> " + i + " = " +  hitsADnTailFiltered[i]);
        // }
        // println("counterHead: " + counterHead);
        // println("counterTail: " + counterTail);

        val hitsAtK: String =
            f((hitsADnHeadFiltered[k] + hitsADnTailFiltered[k]).toDouble() / (counterHead + counterTail).toDouble())
        return hitsAtK
    }


    fun getHitsAtKCalculation(k: Int): String {
        // println("getHitsAtK with k = " + k);
        // println("hitsADnHeadFiltered[k]: " + hitsADnHeadFiltered[k]);
        // println("hitsADnTailFiltered[k]: " + hitsADnTailFiltered[k]);
        // for (int i = 0; i < hitsADnTailFiltered.length; i++) {
        // 	println("  >>> " + i + " = " +  hitsADnTailFiltered[i]);
        // }
        // println("counterHead: " + counterHead);
        // println("counterTail: " + counterTail);

        val hitsAtK =
            "" + hitsADnHeadFiltered[k] + " + " + hitsADnTailFiltered[k] + " / " + counterHead + " + " + counterTail
        return hitsAtK
    }

    fun getHitsAtKDouble(k: Int): Double {
        return ((hitsADnHeadFiltered[k] + hitsADnTailFiltered[k]).toDouble() / (counterHead + counterTail).toDouble())
    }

    // call this method when no head candidate has been found
    fun evaluateHead() {
        counterHead++
    }

    // call this method when no tail candidate has been found
    fun evaluateTail() {
        counterTail++
    }

    fun evaluateHead(candidates: MutableList<String>, triple: Triple): Int {
        var foundAt = -1
        counterHead++
        if (candidates.size > 0) counterHeadCovered++

        var filterCount = 0
        var rank = 0
        while (rank < candidates.size && rank < ATKMAX) {
            val candidate = candidates.get(rank)

            if (candidate == triple.h || (this.am != null && this.am!!.sameAs(triple.h, candidate))) {
                var index = rank
                while (index - filterCount < ATKMAX) {
                    if (index < ATKMAX) hitsADnHead[index]++
                    hitsADnHeadFiltered[index - filterCount]++
                    index++
                }
                foundAt = rank + 1
                // if (foundAt == 1) println(triple);
                break
            } else {
                for (filterSet in filterSets) {
                    if (filterSet.isTrue(candidate, triple.r, triple.t)) {
                        filterCount++
                        break
                    }
                }
            }
            rank++
        }

        var counter = 0
        var ranked = false
        for (candidate in candidates) {
            counter++
            if (candidate == triple.h) {
                this.headRanks.add(counter)
                ranked = true
                break
            }
        }
        if (!ranked) this.headRanks.add(-1)
        return foundAt
    }

    fun evaluateTail(candidates: MutableList<String>, triple: Triple): Int {
        var foundAt = -1
        counterTail++
        if (candidates.size > 0) counterTailCovered++
        var filterCount = 0
        var rank = 0
        while (rank < candidates.size && rank < ATKMAX) {
            val candidate = candidates.get(rank)

            if (candidate == triple.t || (this.am != null && this.am!!.sameAs(triple.t, candidate))) {
                var index = rank
                while (index - filterCount < ATKMAX) {
                    if (index < ATKMAX) hitsADnTail[index]++
                    hitsADnTailFiltered[index - filterCount]++
                    index++
                }
                foundAt = rank + 1
                // if (foundAt == 1) println(triple);
                break
            } else {
                for (filterSet in filterSets) {
                    if (filterSet.isTrue(triple.h, triple.r, candidate)) {
                        filterCount++
                        break
                    }
                }
            }
            rank++
        }

        var counter = 0
        var ranked = false
        for (candidate in candidates) {
            counter++
            if (candidate == triple.t) {
                this.tailRanks.add(counter)
                ranked = true
                break
            }
        }
        if (!ranked) this.tailRanks.add(-1)

        return foundAt
    }

    override fun toString(): String {
        val sb = StringBuilder("evaluation result\n")
        sb.append("hits@k\traw\t\t\tfilter\n")
        sb.append("hits@k\ttail\thead\ttotal\ttail\thead\ttotal\n")
        for (i in 0..<ATKMAX) {
            sb.append(i + 1)
            sb.append("\t")
            sb.append(f(hitsADnTail[i].toDouble() / counterTail.toDouble()))
            sb.append("\t")
            sb.append(f(hitsADnHead[i].toDouble() / counterHead.toDouble()))
            sb.append("\t")
            sb.append(f((hitsADnHead[i] + hitsADnTail[i]).toDouble() / (counterHead + counterTail).toDouble()))
            sb.append("\t")
            sb.append(f(hitsADnTailFiltered[i].toDouble() / counterTail.toDouble()))
            sb.append("\t")
            sb.append(f(hitsADnHeadFiltered[i].toDouble() / counterHead.toDouble()))
            sb.append("\t")
            sb.append(
                f(
                    (hitsADnHeadFiltered[i] + hitsADnTailFiltered[i]).toDouble() / (counterHead + counterTail).toDouble()
                )
            )
            sb.append("\n")
        }
        sb.append(
            ("counterHead=" + counterHead + " counterTail=" + counterTail + " hits@10Tail="
                    + hitsADnTail[ATKMAX - 1] + " hits@10Head=" + hitsADnHead[ATKMAX - 1] + "\n")
        )
        sb.append(
            ("counterHead=" + counterHead + " counterTail=" + counterTail + " hits@10TailFiltered="
                    + hitsADnTailFiltered[ATKMAX - 1] + " hits@10HeadFiltered=" + hitsADnHeadFiltered[ATKMAX - 1] + "\n")
        )
        sb.append(
            "fraction of head covered by rules  = " + (counterHeadCovered.toDouble() / counterHead.toDouble()) + "\n"
        )
        sb.append(
            "fraction of tails covered by rules = " + (counterTailCovered.toDouble() / counterTail.toDouble()) + "\n"
        )
        return sb.toString()
    }

    /*
	public static <K, V extends Comparable<? super V>> LinkedHashMap<K, V> sortByValue2222(Map<K, V> map) {
		return map.entrySet().stream().sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
	}
	*/
    fun addFilterTripleSet(tripleSet: TripleSet?) {
        this.filterSets.add(tripleSet!!)
    }

    fun getMRR(numOfInstances: Int): String {
        val headMrr = this.getMRR(numOfInstances, this.headRanks)
        val tailMrr = this.getMRR(numOfInstances, this.tailRanks)
        return f((headMrr + tailMrr) / 2.0)
    }

    private fun getMRR(numOfInstances: Int, numbers: MutableList<Int>): Double {
        var mrr = 0.0
        for (i in numbers.indices) {
            if (numbers.get(i)!! > 0) {
                mrr += 1.0 / numbers.get(i)!!.toDouble()
            } else {
                mrr += 2.0 / numOfInstances.toDouble()
            }
        }
        mrr = mrr / numbers.size.toDouble()
        return mrr
    }

    fun addAlternativeMentions(am: AlternativeMentions?) {
        this.am = am
    }

    companion object {
        private const val ATKMAX = 100

        private val nf: NumberFormat? = NumberFormat.getInstance(Locale.US)
        private val df = nf as DecimalFormat

        fun f(v: Double): String {
            return df.format(v)
        }
    }
}