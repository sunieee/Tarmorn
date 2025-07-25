package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.data.IdManager

/**
 * A rule of the form h(a,X) <= b1(X,A1), ..., bn(An-1,c) with a constant in the head and in the last body atom.
 *
 */
class RuleAcyclic1(r: RuleUntyped) : RuleAcyclic(r) {
    override val unboundVariable: Int?
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

        if (this.body.get(0).right == IdManager.getXId() && this.body.get(0).right == IdManager.getYId()) {
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


    fun toXYString(): String {
        if (this.head.left == IdManager.getXId()) {
            val c = this.head.right
            val sb = StringBuilder()
            sb.append(this.head.toString(c, IdManager.getYId()))
            for (i in 0..<this.bodysize()) {
                sb.append(this.getBodyAtom(i)!!.toString(c, IdManager.getYId()))
            }
            val rs = sb.toString()
            return rs
        }
        if (this.head.right == IdManager.getYId()) {
            val c = this.head.left
            val sb = StringBuilder()
            sb.append(this.head.toString(c, IdManager.getXId()))
            for (i in this.bodysize() - 1 downTo 0) {
                sb.append(this.getBodyAtom(i)!!.toString(c, IdManager.getXId()))
            }
            val rs = sb.toString()
            return rs
        }
        System.err.println("toXYString of the following rule not implemented: " + this)
        System.exit(1)
        return ""
    }


    fun validates(h: String, relation: String, t: String, ts: TripleSet): Boolean {
        val hId = IdManager.getEntityId(h)
        val relationId = IdManager.getRelationId(relation)
        val tId = IdManager.getEntityId(t)
        
        if (this.targetRelationId == relationId) {
            // this rule is a X rule
            if (this.head.isRightC && this.head.right == tId) {
                // could be true if body is true
                val previousValues = HashSet<Int>()
                previousValues.add(hId)
                previousValues.add(this.head.right)
                return (this.isBodyTrueAcyclic(IdManager.getXId(), hId, 0, previousValues, ts))
            }
            // this rule is a Y rule
            if (this.head.isLeftC && this.head.left == hId) {
                // could be true if body is true

                val previousValues = HashSet<Int>()
                previousValues.add(tId)
                previousValues.add(this.head.left)
                return (this.isBodyTrueAcyclic(IdManager.getYId(), tId, 0, previousValues, ts))
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
        xValue: Int,
        yValue: Int,
        excludedTriples: java.util.HashSet<Triple>,
        triples: TripleSet
    ): java.util.HashSet<Triple> {
        if (this.bodysize() != 1) {
            System.err.println("Trying to get a triple explanation for an acyclic rule with constant in head any body of length != 1. This is not yet implemented.")
            System.exit(-1)
        }
        val groundings = HashSet<Triple>()
        val xId = xValue
        val yId = yValue
        var xInHead = false
        if (this.head.left == IdManager.getXId()) xInHead = true
        if (xInHead) {
            if (this.head.right == yId) {
                val left = this.body.get(0).left
                val right = this.body.get(0).right
                val rel = this.body.get(0).relation
                if (left == IdManager.getXId() && triples.isTrue(xId, rel, right)) {
                    val t = Triple.createTriple(xId, rel, right)
                    if (!excludedTriples.contains(t)) groundings.add(t)
                }
                if (right == IdManager.getXId() && triples.isTrue(left, rel, xId)) {
                    val t = Triple.createTriple(left, rel, xId)
                    if (!excludedTriples.contains(t)) groundings.add(t)
                }
            }
        } else {
            if (this.head.left == xId) {
                val left = this.body.get(0).left
                val right = this.body.get(0).right
                val rel = this.body.get(0).relation
                if (left == IdManager.getYId() && triples.isTrue(yId, rel, right)) {
                    val t = Triple.createTriple(yId, rel, right)
                    if (!excludedTriples.contains(t)) groundings.add(t)
                }
                if (right == IdManager.getYId() && triples.isTrue(left, rel, yId)) {
                    val t = Triple.createTriple(left, rel, yId)
                    if (!excludedTriples.contains(t)) groundings.add(t)
                }
            }
        }
        return groundings
    }
}
