package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet

/**
 * A rule of the form h(a,X) <= b1(X,A1), ..., bn(An-1,c) with a constant in the head and in the last body atom.
 *
 */
class RuleAcyclic1(r: RuleUntyped) : RuleAcyclic(r) {
    override val unboundVariable: String?
        get() = null


    // public double getAppliedConfidence() {
    // 	return (double)this.getCorrectlyPredictedHeads() / ((double)this.getPredictedHeads() + Settings.UNSEEN_NEGATIVE_EXAMPLES + Settings.UNSEEN_NEGATIVE_EXAMPLES_ATYPED[2]);
    // }
    /*
	public double getAppliedConfidence() {
		return (double)this.getCorrectlyPredicted() / ((double)this.getPredicted() + Math.pow(Settings.UNSEEN_NEGATIVE_EXAMPLES, this.bodysize()));
	}
	*/
    /**
     * Returns the number of groundings w.r.t the given triple set for the variable in the last atom.
     * @param triples The triples set to check for groundings.
     * @return The number of groundings.
     */
    override fun getGroundingsLastAtom(triples: TripleSet): Int {
        val last = this.body.last
        if (last.isRightC) {
            return triples.getHeadEntities(last.relation, last.right).size
        } else {
            return triples.getTailEntities(last.relation, last.left).size
        }
    }


    override fun isSingleton(triples: TripleSet): Boolean {
        // return false;

        if (this.body.get(0).right == "X" && this.body.get(0).right == "Y") {
            val head = this.body.get(0).left
            val relation = this.body.get(0).relation
            if (triples.getTailEntities(relation, head).size > 1) return false
            else return true
        } else {
            val tail = this.body.get(0).right
            val relation = this.body.get(0).relation
            if (triples.getHeadEntities(relation, tail).size > 1) return false
            else return true
        }
    }


    val isCyclic: Boolean
        get() {
            if (this.head.constant == this.body.last.constant) return true
            return false
        }


    fun toXYString(): String? {
        if (this.head.left == "X") {
            val c = this.head.right
            val sb = StringBuilder()
            sb.append(this.head.toString(c, "Y"))
            for (i in 0..<this.bodysize()) {
                sb.append(this.getBodyAtom(i)!!.toString(c, "Y"))
            }
            val rs = sb.toString()
            return rs
        }
        if (this.head.right == "Y") {
            val c = this.head.left
            val sb = StringBuilder()
            sb.append(this.head.toString(c, "X"))
            for (i in this.bodysize() - 1 downTo 0) {
                sb.append(this.getBodyAtom(i)!!.toString(c, "X"))
            }
            val rs = sb.toString()
            return rs
        }
        System.err.println("toXYString of the following rule not implemented: " + this)
        System.exit(1)
        return null
    }


    fun validates(h: String, relation: String, t: String, ts: TripleSet): Boolean {
        if (this.targetRelation == relation) {
            // this rule is a X rule
            if (this.head.isRightC && this.head.right == t) {
                // could be true if body is true
                val previousValues = HashSet<String>()
                previousValues.add(h)
                previousValues.add(this.head.right)
                return (this.isBodyTrueAcyclic("X", h, 0, previousValues, ts))
            }
            // this rule is a Y rule
            if (this.head.isLeftC && this.head.left == h) {
                // could be true if body is true

                val previousValues = HashSet<String>()
                previousValues.add(t)
                previousValues.add(this.head.left)
                return (this.isBodyTrueAcyclic("Y", t, 0, previousValues, ts))
            }
            return false
        }
        return false
    }

    /**
     * This method computes for an x and y value pair, if there is a body grounding in the given triple set.
     * If this is the case it returns a non empty set of triples, which is the set of triples that has been used
     * to ground the body. It is not allowed to use triples from the set of excludedTriples.
     *
     * The method should be extremly fast, as its restricted to rules of length 1 only.
     *
     * IMPORTANT NOTE: The method is currently in a certain sense hard-coded for rules of length 1.
     * Longer rules are not supported so far.
     *
     * @param xValue The value of the X variable.
     * @param yValue The value of the Y variable.
     * @param excludedGroundings The triples that are forbidden to be used.
     *
     * @return A minimal set of triples that results into a body grounding.
     */
    override fun getTripleExplanation(
        xValue: String,
        yValue: String,
        excludedTriples: HashSet<Triple>,
        triples: TripleSet
    ): HashSet<Triple> {
        if (this.bodysize() != 1) {
            System.err.println("Trying to get a triple explanation for an acyclic rule with constant in head any body of length != 1. This is not yet implemented.")
            System.exit(-1)
        }
        val groundings = HashSet<Triple>()
        var xInHead = false
        if (this.head.left == "X") xInHead = true
        if (xInHead) {
            if (this.head.right == yValue || (this.head.right == Settings.REWRITE_REFLEXIV_TOKEN && xValue == yValue)) {
                val left = this.body.get(0).left
                val right = this.body.get(0).right
                val rel = this.body.get(0).relation
                if (left == "X" && triples.isTrue(xValue, rel, right)) {
                    val t = Triple(xValue, rel, right)
                    if (!excludedTriples.contains(t)) groundings.add(t)
                }
                if (right == "X" && triples.isTrue(left, rel, xValue)) {
                    val t = Triple(left, rel, xValue)
                    if (!excludedTriples.contains(t)) groundings.add(t)
                }
            }
        } else {
            if (this.head.left == xValue || (this.head.left == Settings.REWRITE_REFLEXIV_TOKEN && xValue == yValue)) {
                val left = this.body.get(0).left
                val right = this.body.get(0).right
                val rel = this.body.get(0).relation
                if (left == "Y" && triples.isTrue(yValue, rel, right)) {
                    val t = Triple(yValue, rel, right)
                    if (!excludedTriples.contains(t)) groundings.add(t)
                }
                if (right == "Y" && triples.isTrue(left, rel, yValue)) {
                    val t = Triple(left, rel, yValue)
                    if (!excludedTriples.contains(t)) groundings.add(t)
                }
            }
        }
        return groundings
    }
}
