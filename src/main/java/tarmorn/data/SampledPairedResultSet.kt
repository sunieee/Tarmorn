package tarmorn.data

class SampledPairedResultSet {
    private var valueCounter = 0
    
    val values = mutableMapOf<String, MutableSet<String>>()
    private val sampling = false
    
    private var currentKey = ""
    private var chao = 0

    var chaoEstimate: Int
        get() = chao
        set(f2) {
            val count = values.values.sumOf { it.size }
            chao = getChaoEstimate(count, f2 + 1, count + f2 + 1)
        }

    private fun getChaoEstimate(f1: Int, f2: Int, d: Int): Int {
        return (d + (f1 * f1).toDouble() / (2.0 * f2)).toInt()
    }

    fun addKey(key: String) {
        currentKey = key
        values.putIfAbsent(key, mutableSetOf())
    }

    fun usedSampling() = sampling

    fun addValue(value: String): Boolean {
        return values[currentKey]?.add(value)?.also { 
            if (it) valueCounter++ 
        } ?: false
    }

    fun size() = valueCounter
}
