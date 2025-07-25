package tarmorn.data

/**
 * Optimized Triple representation using integer IDs instead of strings
 * for better performance in large-scale knowledge graphs
 */
data class OptimizedTriple(
    val hId: Int, // subject ID
    val rId: Int, // relation ID  
    val tId: Int  // object ID
) {
    val confidence: Double get() = 1.0
    
    fun getValue(ifHead: Boolean): Int = if (ifHead) hId else tId
    
    fun equals(ifHead: Boolean, subjectId: Int, relId: Int, objectId: Int): Boolean {
        return if (ifHead) {
            hId == subjectId && tId == objectId && rId == relId
        } else {
            hId == objectId && tId == subjectId && rId == relId
        }
    }
    
    companion object {
        fun createTriple(hId: Int, rId: Int, tId: Int, reverse: Boolean = false): OptimizedTriple {
            return if (reverse) OptimizedTriple(tId, rId, hId) else OptimizedTriple(hId, rId, tId)
        }
    }
}
