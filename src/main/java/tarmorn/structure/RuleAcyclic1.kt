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
        val last = body.last
        return when {
            last.isRightC -> triples.getHeadEntities(last.relation, last.right).size
            else -> triples.getTailEntities(last.relation, last.left).size
        }
    }

    override fun isSingleton(triples: TripleSet): Boolean {
        val firstAtom = body[0]
        return when {
            firstAtom.right == IdManager.getXId() && firstAtom.right == IdManager.getYId() -> {
                val head = firstAtom.left
                val relation = firstAtom.relation
                triples.getTailEntities(relation, head).size <= 1
            }
            else -> {
                val tail = firstAtom.right
                val relation = firstAtom.relation
                triples.getHeadEntities(relation, tail).size <= 1
            }
        }
    }


    val isCyclic: Boolean
        get() = head.constant == body.last.constant


    fun toXYString(): String = when {
        head.left == IdManager.getXId() -> {
            val c = head.right
            buildString {
                append(head.toString(c, IdManager.getYId()))
                repeat(bodySize) { i ->
                    append(getBodyAtom(i)!!.toString(c, IdManager.getYId()))
                }
            }
        }
        head.right == IdManager.getYId() -> {
            val c = head.left
            buildString {
                append(head.toString(c, IdManager.getXId()))
                for (i in bodySize - 1 downTo 0) {
                    append(getBodyAtom(i)!!.toString(c, IdManager.getXId()))
                }
            }
        }
        else -> {
            System.err.println("toXYString of the following rule not implemented: $this")
            System.exit(1)
            ""
        }
    }


    fun validates(h: String, relation: String, t: String, ts: TripleSet): Boolean {
        val hId = IdManager.getEntityId(h)
        val relationId = IdManager.getRelationId(relation)
        val tId = IdManager.getEntityId(t)
        
        return when {
            targetRelationId != relationId -> false
            // this rule is a X rule
            head.isRightC && head.right == tId -> {
                val previousValues = hashSetOf(hId, head.right)
                isBodyTrueAcyclic(IdManager.getXId(), hId, 0, previousValues, ts)
            }
            // this rule is a Y rule
            head.isLeftC && head.left == hId -> {
                val previousValues = hashSetOf(tId, head.left)
                isBodyTrueAcyclic(IdManager.getYId(), tId, 0, previousValues, ts)
            }
            else -> false
        }
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
        excludedTriples: Set<Triple>,
        triples: TripleSet
    ): Set<Triple> {
        if (bodySize != 1) {
            System.err.println("Trying to get a triple explanation for an acyclic rule with constant in head any body of length != 1. This is not yet implemented.")
            System.exit(-1)
        }
        
        val groundings = hashSetOf<Triple>()
        val xInHead = head.left == IdManager.getXId()
        
        if (xInHead) {
            if (head.right == yValue) {
                val bodyAtom = body[0]
                val left = bodyAtom.left
                val right = bodyAtom.right
                val rel = bodyAtom.relation
                
                when {
                    left == IdManager.getXId() && triples.isTrue(xValue, rel, right) -> {
                        val t = Triple.createTriple(xValue, rel, right)
                        if (!excludedTriples.contains(t)) groundings.add(t)
                    }
                    right == IdManager.getXId() && triples.isTrue(left, rel, xValue) -> {
                        val t = Triple.createTriple(left, rel, xValue)
                        if (!excludedTriples.contains(t)) groundings.add(t)
                    }
                }
            }
        } else {
            if (head.left == xValue) {
                val bodyAtom = body[0]
                val left = bodyAtom.left
                val right = bodyAtom.right
                val rel = bodyAtom.relation
                
                when {
                    left == IdManager.getYId() && triples.isTrue(yValue, rel, right) -> {
                        val t = Triple.createTriple(yValue, rel, right)
                        if (!excludedTriples.contains(t)) groundings.add(t)
                    }
                    right == IdManager.getYId() && triples.isTrue(left, rel, yValue) -> {
                        val t = Triple.createTriple(left, rel, yValue)
                        if (!excludedTriples.contains(t)) groundings.add(t)
                    }
                }
            }
        }
        return groundings
    }
}
