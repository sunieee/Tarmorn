package tarmorn.eval

import java.io.PrintWriter
import java.util.PriorityQueue


class Candidate(var value: String, var confidence: Double) : Comparable<Candidate> {
    override fun compareTo(that: Candidate): Int {
        if (this.confidence > that.confidence) return -1
        if (this.confidence < that.confidence) return 1
        return 0
    }

    override fun hashCode(): Int {
        return this.value.hashCode()
    }

    override fun equals(that: Any?): Boolean {
        if (that is Candidate) {
            val thatCand = that
            return this.value == thatCand.value
        }
        return false
    }
}

class CompletionResult(/*
	public void supressConnected(TripleSet triples) {
		
		String[] token = triple.split("\\s+");
		String head = token[0];
		String relation = token[1];
		String tail = token[2];
		
		for (int i = 0; i < this.tailResults.size(); i++) {
			if (triples.getRelations(head, this.tailResults.get(i)).size() > 0) {
				println("remove head candidate " + this.tailResults.get(i));
				this.tailResults.remove(i);
				this.tailConfidences.remove(i);
				
			}
		}
		
		for (int i = 0; i < this.headResults.size(); i++) {
			if (triples.getRelations(this.headResults.get(i), tail).size() > 0) {
				println("remove tail candidate " + this.headResults.get(i));
				this.headResults.remove(i);
				this.headConfidences.remove(i);
			}
		}
		
	}
	*/val tripleAsString: String
) {
    var heads: MutableList<String>
    var tails: MutableList<String>

        var headConfidences: MutableList<Double>
        var tailConfidences: MutableList<Double>

    init {
        this.heads = mutableListOf<String>()
        this.tails = mutableListOf<String>()

        this.headConfidences = mutableListOf<Double>()
        this.tailConfidences = mutableListOf<Double>()
    }

    fun addHeadResults(heads: Array<String>, k: Int) {
        if (k > 0) addResults(heads, this.heads, k)
        else addResults(heads, this.heads)
    }

    fun addTailResults(tails: Array<String>, k: Int) {
        if (k > 0) addResults(tails, this.tails, k)
        else addResults(tails, this.tails)
    }


    private fun addResults(candidates: Array<String>, results: MutableList<String>, k: Int) {
        var k = k
        for (c in candidates) {
            if (c != "") {
                results.add(c)
                k--
                if (k == 0) return
            }
        }
    }

    private fun addConfidences(confs: Array<Double>, confidences: MutableList<Double>) {
        for (d in confs) {
            confidences.add(d)
        }
    }

    private fun addConfidences(confs: Array<Double>, confidences: MutableList<Double>, k: Int) {
        var k = k
        for (d in confs) {
            confidences.add(d)
            k--
            if (k == 0) return
        }
    }

    private fun addResults(candidates: Array<String>, results: MutableList<String>) {
        for (c in candidates) {
            if (c != "") {
                results.add(c)
            }
        }
    }


    fun addHeadConfidences(confidences: Array<Double>, k: Int) {
        if (k > 0) this.addConfidences(confidences, this.headConfidences, k)
        else this.addConfidences(confidences, this.headConfidences)
    }


    fun addTailConfidences(confidences: Array<Double>, k: Int) {
        if (k > 0) this.addConfidences(confidences, this.tailConfidences, k)
        else this.addConfidences(confidences, this.tailConfidences)
    }

    fun extendWith(thatResult: CompletionResult, k: Int, factor: Double) {
        val qHeads = PriorityQueue<Candidate>()
        val headsC = mutableMapOf<String, Double>()
        for (i in this.headConfidences.indices) {
            val hc = Candidate(this.heads.get(i), this.headConfidences.get(i)!!)
            qHeads.add(hc)
            headsC.put(this.heads.get(i), this.headConfidences.get(i))
        }

        for (i in thatResult.headConfidences.indices) {
            val hc = Candidate(thatResult.heads.get(i), thatResult.headConfidences.get(i)!! * factor)
            if (headsC.containsKey(thatResult.heads.get(i))) {
                if (hc.confidence > headsC.get(thatResult.heads.get(i))!!) {
                    println(hc.confidence)
                    qHeads.remove(hc) // looks crazy, is not crazy
                    qHeads.add(hc)
                }
            } else qHeads.add(hc)
        }

        this.heads.clear()
        this.headConfidences.clear()
        var j = 0
        while (qHeads.size > 0) {
            val c = qHeads.poll()
            this.heads.add(c.value)
            this.headConfidences.add(c.confidence)
            j++
            if (j == k) break
        }


        val qTails = PriorityQueue<Candidate>()
        val tailsC = mutableMapOf<String, Double>()
        for (i in this.tailConfidences.indices) {
            val hc = Candidate(this.tails.get(i), this.tailConfidences.get(i)!!)
            qTails.add(hc)
            tailsC.put(this.tails.get(i), this.tailConfidences.get(i))
        }

        for (i in thatResult.tailConfidences.indices) {
            val hc = Candidate(thatResult.tails.get(i), thatResult.tailConfidences.get(i)!! * factor)
            if (tailsC.containsKey(thatResult.tails.get(i))) {
                if (hc.confidence > tailsC.get(thatResult.tails.get(i))!!) {
                    qTails.remove(hc) // looks crazy, is not crazy
                    qTails.add(hc)
                }
            } else qTails.add(hc)
        }

        this.tails.clear()
        this.tailConfidences.clear()
        j = 0
        while (qTails.size > 0) {
            val c = qTails.poll()
            this.tails.add(c.value)
            this.tailConfidences.add(c.confidence)
            j++
            if (j == k) break
        }
    }

    fun write(pw: PrintWriter) {
        pw.print(this.toString())
    }

    /**
     * Take care: Will throw an index out of bounds error if applied to a result set that has no confidences
     */
    override fun toString(): String {
        val sb = StringBuilder(this.tripleAsString + "\n")
        sb.append("Heads: ")
        for (i in this.heads.indices) {
            sb.append(this.heads.get(i) + "\t" + this.headConfidences.get(i) + "\t")
        }
        sb.append("\n")
        sb.append("Tails: ")
        for (i in this.tails.indices) {
            sb.append(this.tails.get(i) + "\t" + this.tailConfidences.get(i) + "\t")
        }
        sb.append("\n")
        return sb.toString()
    }

    fun isTrueTail(tail: String): Boolean {
        val token: Array<String>? =
            this.tripleAsString.split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        if (token!![2] == tail) return true
        return false
    }

    fun isTrueHead(head: String): Boolean {
        val token: Array<String>? =
            this.tripleAsString.split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        if (token!![0] == head) return true
        return false
    }
}