package tarmorn.playground

import tarmorn.data.Triple
import tarmorn.data.TripleSet

object Functionality {
    @JvmStatic
    fun main(args: Array<String>) {
        val ts = TripleSet("data/FB15-237/train.txt")


        val relations = HashSet<String>()
        relations.addAll(ts.relations)




        for (relation in relations) {
            println("relation: " + relation)
            val rTriples: MutableList<Triple> = ts.getTriplesByRelation(relation)
            val rHeads = HashSet<String>()
            val rTails = HashSet<String>()
            for (rtriple in rTriples) {
                rHeads.add(rtriple.h)
                rTails.add(rtriple.t)
            }
            var tailsPerHeadAll = 0
            var headsPerTailAll = 0
            for (rh in rHeads) {
                val i = ts.getEntities(relation, rh, true)!!.size
                tailsPerHeadAll += i
            }

            for (rt in rTails) {
                val i = ts.getEntities(relation, rt, false)!!.size
                headsPerTailAll += i
            }
            val headsPerTailFraction = headsPerTailAll.toDouble() / rTails.size.toDouble()
            val tailsPerHeadFraction = tailsPerHeadAll.toDouble() / rHeads.size.toDouble()
            println("   headsPerTailFraction: " + headsPerTailFraction)
            println("   tailsPerHeadFraction: " + tailsPerHeadFraction)
        }
    }
}
