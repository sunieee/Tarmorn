package tarmorn.playground

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.eval.ResultSet
import tarmorn.io.RuleReader
import tarmorn.structure.Rule
import tarmorn.structure.RuleAcyclic1
import java.io.IOException

object NegationFilter {
    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val inputRSPath = "exp/wave2/negation/fb237-predictions-1000"
        val outputRSPath = "exp/wave2/negation/fb237-50-filtered-hard"


        val rr = RuleReader()

        val train = TripleSet("data/FB15-237/train.txt")

        Settings.READ_THRESHOLD_CONFIDENCE = 0.0
        Settings.READ_THRESHOLD_CORRECT_PREDICTIONS = 0

        val rules = rr.read("exp/wave2/negation/nrules_s2")
        val relation2Rules = mutableMapOf<String, MutableList<Rule>>()


        for (r in rules) {
            if (!relation2Rules.containsKey(r.targetRelation)) relation2Rules.put(r.targetRelation, mutableListOf<Rule>())
            relation2Rules.get(r.targetRelation)!!.add(r)
        }

        println(rules.size)


        val rs = ResultSet("fb237-default", inputRSPath, true, 50)


        // TripleSet test = new TripleSet("data/FB15-237/train.txt");
        var counter = 0
        var counterAllHeadCandidates = 0
        var counterAllTailCandidates = 0
        var filteredHead = 0
        var filteredTail = 0
        var correctlyFilteredHead = 0
        var correctlyFilteredTail = 0

        val ruleToMistakeCount = mutableMapOf<RuleAcyclic1?, Int>()

        for (cr in rs) {
            if (counter % 1000 == 0) println(counter.toString() + " completion tasks have been filtered")

            val tripleAsString = cr!!.tripleAsString
            val token: Array<String> =
                tripleAsString.split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            val triple = Triple(token[0]!!, token[1]!!, token[2]!!)

            var relevantRules = relation2Rules.get(triple.r)
            if (relevantRules == null) relevantRules = mutableListOf<Rule>()


            // println(t);
            counter++

            var hi = 0
            while (hi < cr.heads.size) {
                val h = cr.heads.get(hi)
                counterAllHeadCandidates++
                var filter = false

                for (r in relevantRules) {
                    if (r is RuleAcyclic1) {
                        val rule = r
                        filter = rule.validates(h!!, triple.r, triple.t!!, train)


                        // if (filter && (h.equals(triple.getHead()))) {
                        //if (filter) {
                        //	println(counter + ": filtered out: " + h + " (indexed at pos " + hi + ") as head proposal for " + triple);
                        //	println("due to " + rule);
                        //	println();
                        //}
                        if (filter) break
                    }
                }
                if (filter) {
                    //println("removed: " +  removedElement);
                    //println();


                    val removedElement = cr.heads.removeAt(hi)
                    cr.headConfidences.removeAt(hi)
                    hi--

                    filteredHead++
                    if (h != triple.h) {
                        correctlyFilteredHead++
                    } else {
                    }
                }
                hi++
            }


            var ti = 0
            while (ti < cr.tails.size) {
                val t = cr.tails.get(ti)
                counterAllTailCandidates++
                var filter = false

                for (r in relevantRules) {
                    if (r is RuleAcyclic1) {
                        val rule = r
                        filter = rule.validates(triple.h, triple.r, t!!, train)


                        /*
						if (filter && (t.equals(triple.getHead()))) {
							println(counter + ":");
							println("incorrectly filtered out: " + t + " as head proposal for " + triple);
							println("due to " + rule);
							println();
						}
						*/
                        if (filter) break
                    }
                }
                if (filter) {
                    cr.tails.removeAt(ti)
                    cr.tailConfidences.removeAt(ti)
                    ti--

                    filteredTail++
                    if (t != triple.t) {
                        correctlyFilteredTail++
                    }
                }
                ti++
            }
        }

        rs.write(outputRSPath)

        println("all completion tasks: " + counter)
        println("random selection mistake rate: " + ((2.0 * counter) / (counterAllHeadCandidates + counterAllTailCandidates).toDouble()))
        val removalRate1 =
            (filteredHead + filteredTail).toDouble() / (counterAllHeadCandidates + counterAllTailCandidates).toDouble()
        val removalRate2 = (filteredHead + filteredTail).toDouble() / (counter * 2.0)

        println("removal rate: " + removalRate1 + " (per candidate) or " + removalRate2 + " per task")

        val misHead = 1.0 - correctlyFilteredHead.toDouble() / filteredHead
        val misTail = 1.0 - correctlyFilteredTail.toDouble() / filteredTail
        println("filtered heads: " + filteredHead + " correctly filtered: " + correctlyFilteredHead + " mistake-rate: " + misHead)
        println("filtered tails: " + filteredTail + " correctly filtered: " + correctlyFilteredTail + " mistake-rate: " + misTail)
    }
}
