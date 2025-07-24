package tarmorn.playground

import tarmorn.data.TripleSet
import tarmorn.eval.ResultSet

object ComparisonResultSets {
    @JvmStatic
    fun main(args: Array<String>) {
        val rs1 = ResultSet("RESCAL", "exp/fb237-analysis/dog-rescal-predictions-20-filtered", true, 20)
        val rs2 = ResultSet("AnyBURL", "exp/zero/fb237-predictions-1000-all-x-F-top200", true, 200)

        val train = TripleSet("data/FB15-237/train.txt")

        val tp2TailCounter = 0


        for (cr1 in rs1) {
            val cr2 = rs2.getCompletionResult(cr1!!.tripleAsString)
            if (cr1.tails.size > 0) {
                val tp = cr1.tails.get(0)
                if (cr1.isTrueTail(tp)) {
                    var foundAt = -1
                    var lastConf = -1.0
                    for (i in cr2.tails.indices) {
                        // System.out.print("(" + cr2.getTailConfidences().get(i) + ")" + cr2.getTails().get(i)  + " ");
                        if (cr2.tails.get(i) == tp) {
                            foundAt = i
                            break
                        }
                        lastConf = cr2.tailConfidences.get(i)!!
                    }

                    if (foundAt < 0) {
                        //String tp2 = cr2.getTails().get(0);

                        println(
                            "correct (in tail pos / in head pos):   " + train.getTriplesByTail(tp)!!.size + " | " + train.getTriplesByHead(
                                tp
                            )!!.size
                        )

                        //println("anyburl#1: " + train.getTriplesByTail(tp2).size());
                        //println("correct:   " + train.getTriplesByHead(tp).size());
                        //println("anyburl#1: " + train.getTriplesByHead(tp2).size());						
                        println(">>>" + cr1.tripleAsString)

                        println("must have confidence lower than: " + lastConf)
                        println("---> ")
                        for (i in cr2.tails.indices) {
                            print("(" + cr2.tailConfidences.get(i) + ")" + cr2.tails.get(i) + " ")
                        }
                        println()
                        println("")
                    }
                }
            }
        }
    }
}
