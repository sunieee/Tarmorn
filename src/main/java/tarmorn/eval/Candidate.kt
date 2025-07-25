package tarmorn.eval

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
