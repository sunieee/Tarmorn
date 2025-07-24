package tarmorn.algorithm

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.structure.Path
import tarmorn.structure.Rule
import kotlin.random.Random

/**
 * This class is responsible for sampling grounded pathes.
 *
 */
class PathSampler(private val ts: TripleSet) {
    private val rand = Random


    @JvmOverloads
    fun samplePath(steps: Int, cyclic: Boolean, chosenHeadTriple: Triple? = null): Path? {
        return samplePath(steps, cyclic, chosenHeadTriple, null)
    }


    fun samplePath(steps: Int, cyclic: Boolean, chosenHeadTriple: Triple?, ruleToBeExtended: Rule?): Path? {
        // println("sample path");
        // if (ruleToBeExtended == null) return null;
        val nodes = Array(1 + steps * 2) { "" }
        val markers = CharArray(steps)
        val chosenTriples: MutableList<Triple>
        if (Settings.SINGLE_RELATIONS != null) {
            val rdice = this.rand.nextInt(Settings.SINGLE_RELATIONS!!.size)
            val singleRelation = Settings.SINGLE_RELATIONS!![rdice]
            chosenTriples = ts.getTriplesByRelation(singleRelation)
            if (chosenTriples!!.size == 0) {
                System.err.println("chosen a SINGLE_RELATION=" + singleRelation + " that is not instantiated in the training data")
                System.exit(0)
            }
        } else {
            chosenTriples = ts.triples
        }
        var triple: Triple? = null
        if (chosenHeadTriple == null) {
            val dice = this.rand.nextInt(chosenTriples.size)
            triple = chosenTriples.get(dice)
        } else triple = chosenHeadTriple

        // TODO hardcoded test to avoid reflexive relations in the head
        if (triple!!.h == triple.t) return null
        var dice = this.rand.nextDouble()
        if (ruleToBeExtended != null) {
            if (ruleToBeExtended.isXRule) dice = 1.0
            if (ruleToBeExtended.isYRule) dice = 0.0
        }
        if (dice < 0.5) {
            markers[0] = '+'
            nodes[0] = triple.h
            nodes[1] = triple.r
            nodes[2] = triple.t
        } else {
            markers[0] = '-'
            nodes[2] = triple.h
            nodes[1] = triple.r
            nodes[0] = triple.t
        }


        // add next hop
        var index = 1
        while (index < steps) {
            if (this.rand.nextDouble() < 0.5) {
                val candidateTriples: MutableList<Triple>? = ts.getTriplesByHead(nodes[index * 2])
                if (candidateTriples!!.size == 0) return null
                val nextTriple: Triple
                if (cyclic && index + 1 == steps) {
                    val cyclicCandidateTriples = mutableListOf<Triple>()
                    for (t in candidateTriples) {
                        if (t.t == nodes[0]) cyclicCandidateTriples.add(t)
                    }
                    if (cyclicCandidateTriples.size == 0) return null
                    nextTriple = cyclicCandidateTriples.get(this.rand.nextInt(cyclicCandidateTriples.size))
                } else {
                    nextTriple = candidateTriples.get(this.rand.nextInt(candidateTriples.size))
                }
                nodes[index * 2 + 1] = nextTriple.r
                nodes[index * 2 + 2] = nextTriple.t
                markers[index] = '+'
            } else {
                val candidateTriples: MutableList<Triple>? = ts.getTriplesByTail(nodes[index * 2])
                if (candidateTriples!!.size == 0) return null
                val nextTriple: Triple
                if (cyclic && index + 1 == steps) {
                    val cyclicCandidateTriples = mutableListOf<Triple>()
                    for (t in candidateTriples) {
                        if (t.h == nodes[0]) cyclicCandidateTriples.add(t)
                    }
                    if (cyclicCandidateTriples.size == 0) return null
                    nextTriple = cyclicCandidateTriples.get(this.rand.nextInt(cyclicCandidateTriples.size))
                } else {
                    nextTriple = candidateTriples.get(this.rand.nextInt(candidateTriples.size))
                }
                nodes[index * 2 + 1] = nextTriple.r
                nodes[index * 2 + 2] = nextTriple.h
                markers[index] = '-'
            }
            index++
        }
        // if (steps == 1) { println("... arrived and here"); }
        val p = Path(nodes, markers)
        if (steps == 1) return p
        if (!cyclic && p.isCyclic) return null
        // check if path is valid
        return p
    }


    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
        }
    }
}
