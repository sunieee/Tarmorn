package tarmorn.playground

import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.structure.Atom
import tarmorn.structure.RuleAcyclic1
import tarmorn.structure.RuleUntyped
import java.io.FileNotFoundException
import java.io.PrintWriter

object NegationLearner {
    // def (no extension): 500, 500, 50, 10 => 900 rules
    // s1: 500, 500, 50, 5 => 1000 rules
    // s2: 500, 500, 10, 10 => 76000 rules
    // only consider relations in negative rules with a minimum number of instantiations
    var MIN_USAGE_RELATION: Int = 500

    // do a sample with a certain size to find the most frequent entities that appear in head and tail poistion
    // w.r.t to a certain relation
    var SAMPLE_SIZE: Int = 500

    // consider only those entities that appear more than a minimum  times in the sample
    var MIN_USAGE_XATOM: Int = 10

    // consider only those pairs of relations to be used in body and head that that result in a minimum number of joins
    // this works as a check that the relations describe the same type of entities
    var MIN_CONNECTEDNESS: Int = 10


    // sometimes there are mitakes in the datasets, which means that 
    var outputPath: String = "exp/wave2/negation/nrules_s2"

    @Throws(FileNotFoundException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val train = TripleSet("data/FB15-237/train.txt")

        val relations: MutableSet<String> = train.relations

        val frequentXAtoms = ArrayList<Atom>()

        val pw = PrintWriter(outputPath)

        for (r in relations) {
            // if (!(r.equals("/people/person/nationality"))) continue;

            if (train.getTriplesByRelation(r)!!.size < MIN_USAGE_RELATION) continue

            val headCount = HashMap<String, Int>()
            val tailCount = HashMap<String, Int>()

            val heads = train.selectNRandomEntitiesByRelation(r, true, SAMPLE_SIZE)
            val tails = train.selectNRandomEntitiesByRelation(r, false, SAMPLE_SIZE)

            for (h in heads!!) {
                if (!headCount.containsKey(h)) headCount.put(h!!, 0)
                headCount.put(h!!, headCount.get(h)!! + 1)
            }
            for (t in tails!!) {
                if (!tailCount.containsKey(t)) tailCount.put(t!!, 0)
                tailCount.put(t!!, tailCount.get(t)!! + 1)
            }


            // println("RELATION: " + r + " => " + train.getTriplesByRelation(r).size());
            for (h in headCount.keys) {
                if (headCount.get(h)!! >= MIN_USAGE_XATOM) {
                    // println(headCount.get(h) + ": " + r + "(" + h + ",?)");
                    val atom = Atom(h, r, "X", true, false)
                    frequentXAtoms.add(atom)
                }
            }

            for (t in tailCount.keys) {
                if (tailCount.get(t)!! >= MIN_USAGE_XATOM) {
                    // println(tailCount.get(t) + ": " + r + "(?, " + t + ")");
                    val atom = Atom("X", r, t, false, true)
                    frequentXAtoms.add(atom)
                }
            }
        }


        // println("==================");
        println("found " + frequentXAtoms.size + " frequent atoms")

        val strippedXAtomsAsSet = HashSet<Atom>()
        val strippedXAtoms = ArrayList<Atom>()

        for (fxa in frequentXAtoms) {
            if (fxa.isRightC) {
                val stripped = Atom(fxa.left, fxa.relation, "something", false, true)
                strippedXAtomsAsSet.add(stripped)
            } else {
                val stripped = Atom("something", fxa.relation, fxa.right, true, false)
                strippedXAtomsAsSet.add(stripped)
            }
        }

        strippedXAtoms.addAll(strippedXAtomsAsSet)
        println("reduced to " + strippedXAtoms.size + " stripped atom patterns")


        val validNegativePatterns = HashMap<Atom?, HashSet<Atom>>()

        var countPotentialNegations = 0
        for (i in 0..<strippedXAtoms.size - 1) {
            for (j in i + 1..<strippedXAtoms.size) {
                val ai = strippedXAtoms.get(i)
                val aj = strippedXAtoms.get(j)

                var counter: Int

                val distinctJoinValues = HashSet<String>()

                counter = 0
                val aiTriples: MutableList<Triple> = train.getTriplesByRelation(ai.relation)
                for (aiT in aiTriples!!) {
                    val joinValue = if (ai.isRightC) aiT.h else aiT.t
                    val result = train.getEntities(aj.relation, joinValue, aj.isRightC)
                    if (result!!.size > 0) {
                        distinctJoinValues.add(joinValue)
                        counter++
                    }
                }
                if (distinctJoinValues.size >= MIN_CONNECTEDNESS) {
                    /*
                                       if (ai.getRelation().equals("/people/person/gender") && (ai.isRightC())) {
                                           if (aj.getRelation().equals("/organization/organization_member/member_of./organization/organization_membership/organization") && aj.isRightC()) {
                                               println("HIT HIT HIT: " +  distinctJoinValues.size());
                                               for (String jv : distinctJoinValues) {
                                                   println("     " +  jv);
                                               }
                                               // println("join value = " + joinValue + " from " + aiT);
                                               // println("results.size() = " +  result.size());
                                           }
                                       }
                                       */



                    if (!validNegativePatterns.containsKey(ai)) {
                        validNegativePatterns.put(ai, HashSet<Atom>())
                    }
                    validNegativePatterns.get(ai)!!.add(aj)

                    if (!validNegativePatterns.containsKey(aj)) {
                        validNegativePatterns.put(aj, HashSet<Atom>())
                    }
                    validNegativePatterns.get(aj)!!.add(ai)
                    countPotentialNegations++
                }
            }
        }
        println("there are potentially " + countPotentialNegations + " patterns for negative rules")

        var countInstValidNegativePatterns = 0
        for (i in 0..<frequentXAtoms.size - 1) {
            for (j in i + 1..<frequentXAtoms.size) {
                // check if its valid

                val ai = frequentXAtoms.get(i)
                val aj = frequentXAtoms.get(j)

                val sai = ai.createCopy()
                sai.replace(ai.constant, "something")
                val saj = aj.createCopy()
                saj.replace(aj.constant, "something")

                if (validNegativePatterns.containsKey(sai) && validNegativePatterns.get(sai)!!.contains(saj)) {
                    countInstValidNegativePatterns++

                    // make the count
                    val xIValues = train.getEntities(ai.relation, ai.constant, ai.isLeftC)
                    val xJValues = train.getEntities(aj.relation, aj.constant, aj.isLeftC)
                    val xIxJValues: MutableSet<String> = HashSet<String>()
                    xIxJValues.addAll(xIValues!!)
                    xIxJValues.retainAll(xJValues!!)

                    if (xIxJValues.size == 0) {
                        val ui = RuleUntyped(xIValues.size, 0, 0.0)
                        ui.head = aj
                        ui.addBodyAtom(ai)
                        val ri = RuleAcyclic1(ui)
                        ri.detachAndPolish()


                        val uj = RuleUntyped(xJValues.size, 0, 0.0)
                        uj.head = ai
                        uj.addBodyAtom(aj)
                        val rj = RuleAcyclic1(uj)
                        rj.detachAndPolish()

                        pw.println(ri)
                        pw.println(rj)

                        pw.flush()


                        // println(ai);

                        //println("relation: "+ sai.getRelation() + ", " + sai.getConstant() + ", " + sai.isLeftC());
                        var c = 0
                        println("SCORES: " + xIValues.size + " | " + xJValues.size)

                        println(ai)
                        for (xi in xIValues) {
                            c++
                            println("   x = " + xi)
                            if (c == 3) break
                        }
                        c = 0
                        println(aj)
                        for (xj in xJValues) {
                            c++
                            println("   x = " + xj)
                            if (c == 3) break
                        }
                        println()
                    }
                }
            }
        }

        println("there are " + countInstValidNegativePatterns + " instantiations of valid negative patterns")
        pw.close()
    }
}
