package tarmorn.playground

import tarmorn.eval.ResultSet

object OpenBioTests {
    @JvmStatic
    fun main(args: Array<String>) {
        // TripleSet ts = new TripleSet("data/OpenBio/train_sample.csv", true);


        val rs = ResultSet("all", "exp/openbio/pred-C1-100", true, 10)


        var counterAll = 0
        var counterConfidentHeads = 0
        var counterConfidentTails = 0
        for (cr in rs) {
            counterAll += 2
            if (cr!!.heads.size >= 10) {
                val d: Double = cr.headConfidences.get(0)!!
                if (d > 0.5) {
                    counterConfidentHeads++
                }
            }
            if (cr.tails.size >= 10) {
                val d: Double = cr.tailConfidences.get(0)!!
                if (d > 0.5) {
                    counterConfidentTails++
                }
            }
        }

        println("all: " + counterAll)

        println("confidente heads: " + counterConfidentHeads)
        println("confidente tailss: " + counterConfidentTails)
        println("confidente together: " + (counterConfidentHeads + counterConfidentTails))
    }
}
