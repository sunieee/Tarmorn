package tarmorn.eval

import tarmorn.Settings
import tarmorn.data.TripleSet

object DiffRank1 {
    @JvmStatic
    fun main(args: Array<String>) {
        Settings.REWRITE_REFLEXIV = false

        val rs1 = ResultSet("original", "exp/maxgroup/analysis/anyburl-c3-3600-100-test", true, 100)
        val rs2 = ResultSet("reordered", "exp/maxgroup/analysis/fb237-preds-local-o1-n09-woff", true, 100)


        val triples = TripleSet("../AnyBURL/data/FB237/test.txt")


        // GoldStandard goldSymmetry    = gold.getSubset("Subsumption");	
        var deltaPosTail = 0
        var deltaNegTail = 0

        var deltaPosHead = 0
        var deltaNegHead = 0

        val distance = -1.0


        for (t in triples.triples) {
            val triple = t.toString()
            val tt: Array<String> = triple.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            // Triple t = new Triple(tt[0], tt[1], tt[2]);
            val proposedHead1 =
                if (rs1.getHeadCandidates(triple).size > 0) rs1.getHeadCandidates(triple).get(0) else "-"
            val proposedHead2 =
                if (rs2.getHeadCandidates(triple).size > 0) rs2.getHeadCandidates(triple).get(0) else "-"
            if ((proposedHead2 != "-") && (proposedHead1 != "-") && (proposedHead1 != proposedHead2)) {
                if (proposedHead1 != t.h && proposedHead2 == t.h) {
                    //if (rs2.getHeadConfidences(triple).get(0) -  rs1.getHeadConfidences(triple).get(0) > distance) {

                    println(triple)
                    println(">>> head 1: " + proposedHead1 + ", " + (proposedHead1 == t.h))
                    println(">>> head 2: " + proposedHead2 + ", " + (proposedHead2 == t.h))
                    deltaPosHead++
                    println(
                        rs2.name + ": " + rs2.getHeadConfidences(triple)
                            .get(0) + " <- " + rs1.name + ": " + rs1.getHeadConfidences(triple).get(0) + "\n"
                    )

                    //}
                }

                if (proposedHead1 == t.h && proposedHead2 != t.h) {
                    // change to the worse
                    //if (rs2.getHeadConfidences(triple).get(0) -  rs1.getHeadConfidences(triple).get(0) >  distance) {
                    println(triple)
                    println(">>> head 1: " + proposedHead1 + ", " + (proposedHead1 == t.h))
                    println(">>> head 2: " + proposedHead2 + ", " + (proposedHead2 == t.h))
                    println(
                        "    position of hit = " + rs2.getHeadCandidates(triple)
                            .indexOf(t.h) + " size = " + rs2.getHeadCandidates(triple).size
                    )
                    deltaNegHead++
                    println(
                        rs2.name + ": " + rs2.getHeadConfidences(triple)
                            .get(0) + " <- " + rs1.name + ": " + rs1.getHeadConfidences(triple).get(0) + "\n"
                    )


                    //}
                }
            }
            val proposedTail1 =
                if (rs1.getTailCandidates(triple).size > 0) rs1.getTailCandidates(triple).get(0) else "-"
            val proposedTail2 =
                if (rs2.getTailCandidates(triple).size > 0) rs2.getTailCandidates(triple).get(0) else "-"
            if ((proposedTail2 != "-") && (proposedTail1 != "-") && (proposedTail1 != proposedTail2)) {
                if (proposedTail1 != t.t && proposedTail2 == t.t) {
                    //if (rs2.getTailConfidences(triple).get(0) -  rs1.getTailConfidences(triple).get(0) >  distance) {
                    println(triple)
                    println(">>> tail 1: " + proposedTail1 + ", " + (proposedTail1 == t.t))
                    println(">>> tail 2: " + proposedTail2 + ", " + (proposedTail2 == t.t))
                    deltaPosTail++
                    println(
                        rs2.getTailConfidences(triple).get(0).toString() + " <- " + rs1.getTailConfidences(triple)
                            .get(0) + "\n"
                    )
                    //}
                }

                if (proposedTail1 == t.t && proposedTail2 != t.t) {
                    //if (rs2.getTailConfidences(triple).get(0) -  rs1.getTailConfidences(triple).get(0) > distance) {
                    println(triple)
                    println(">>> tail 1: " + proposedTail1 + ", " + (proposedTail1 == t.t))
                    println(">>> tail 2: " + proposedTail2 + ", " + (proposedTail2 == t.t))
                    println(
                        "    position of hit = " + rs2.getTailCandidates(triple)
                            .indexOf(t.t) + " size = " + rs2.getTailCandidates(triple).size
                    )
                    deltaNegTail++
                    println(
                        rs2.getTailConfidences(triple).get(0).toString() + " <- " + rs1.getTailConfidences(triple)
                            .get(0) + "\n"
                    )
                    //}
                }
            }
        }
        println("Delta Head : pos=" + deltaPosHead + " neg=" + deltaNegHead)
        println("Delta Tail : pos=" + deltaPosTail + " neg=" + deltaNegTail)
        println("Delta All  : pos=" + (deltaPosHead + deltaPosTail) + " neg=" + (deltaNegHead + deltaNegTail))
        println("threshold of " + distance + " = " + (100.0 * (((deltaPosHead + deltaPosTail) - (deltaNegHead + deltaNegTail)) / (triples.triples.size.toDouble() * 2.0))) + "%")
    }
}
