package tarmorn.data

class SampledPairedResultSet {
    private var valueCounter = 0

    val values: HashMap<String, HashSet<String>>
    private val sampling: Boolean

    private var currentKey: String = ""

    private var chao = 0

    var chaoEstimate: Int
        get() = this.chao
        set(f2) {
            var count = 0
            for (s in values.keys) {
                count += values.get(s)!!.size
            }
            this.chao = this.getChaoEstimate(count, f2 + 1, count + f2 + 1)
        }

    private fun getChaoEstimate(f1: Int, f2: Int, d: Int): Int {
        val c = (d + ((f1 * f1).toDouble() / (2.0 * f2))).toInt()

        //println("chao=" + c + " f1=" + f1 + " f2=" + f2 + " d=" + d);
        return c
    }

    init {
        this.values = HashMap<String, HashSet<String>>()
        this.sampling = false
    }

    fun addKey(key: String) {
        this.currentKey = key
        if (this.values.containsKey(key)) return
        else this.values.put(key, HashSet<String>())
    }

    fun usedSampling(): Boolean {
        return this.sampling
    }

    fun addValue(value: String): Boolean {
        if (this.values.get(currentKey)!!.add(value)) {
            this.valueCounter++
            return true
        }
        return false
    }

    fun size(): Int {
        return this.valueCounter
    }
}
