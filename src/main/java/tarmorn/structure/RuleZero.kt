package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet

/**
 * A rule with an empty body, that fires only because something is asked.
 * This rule is always a rule of length 0, no specific conditions needs to be checked.
 *
 * gender(X,male) <=
 *
 * This rule say that if you are asked about the gender of something, you should give the answer "mail".
 * Its important to understand that this rule should only fire if its already known from the context of the question,
 * that the question itself makes sense.
 *
 *
 */
class RuleZero(r: RuleUntyped) : Rule(r) {
    val RuleFunctionalityBasicSupportOnly = "RuleFunctionalityBasicSupportOnly Exception (some specific method is called for a rule type that supports (currently) only basic methods)"

    override fun computeScores(ts: TripleSet) {
        val c = this.head.constant
        val tr = this.targetRelation
        val cIsHead = this.head.ishC

        val triplesTR = ts.getTriplesByRelation(tr)
        val triplesTRC = ts.getEntities(tr, c, cIsHead)

        this.predicted = triplesTR.size
        this.correctlyPredicted = triplesTRC.size
        this.confidence = correctlyPredicted.toDouble() / predicted.toDouble()
    }

    override fun computeTailResults(head: Int, ts: TripleSet): Set<Int> {
        val results = hashSetOf<Int>()
        if (this.head.istC) {
            // Convert string to entity ID
            results.add(this.head.t)
        }
        return results
    }

    override fun computeHeadResults(tail: Int, ts: TripleSet): Set<Int> {
        val results = hashSetOf<Int>()
        if (this.head.ishC) {
            // Convert string to entity ID
            results.add(this.head.h)
        }
        return results
    }

    override val appliedConfidence: Double
        get() = Settings.RULE_ZERO_WEIGHT * super.appliedConfidence


    override fun isPredictedX(leftValue: Int, rightValue: Int, forbidden: Triple?, ts: TripleSet): Boolean {
        throw Exception(RuleFunctionalityBasicSupportOnly)
    }

    override fun isRefinable(): Boolean {
        return false
    }


    override fun getRandomValidPrediction(ts: TripleSet): Triple? {
        throw Exception(RuleFunctionalityBasicSupportOnly)
    }


    override fun getRandomInvalidPrediction(ts: TripleSet): Triple? {
        throw Exception(RuleFunctionalityBasicSupportOnly)
    }


    override fun getPredictions(ts: TripleSet): List<Triple>? {
        throw Exception(RuleFunctionalityBasicSupportOnly)
    }

    override fun isSingleton(triples: TripleSet): Boolean {
        throw Exception(RuleFunctionalityBasicSupportOnly)
    }

    /**
     * This method computes usually for an x and y value pair, if there is a body grounding in the given triple set.
     * If this specific case of a zero rule it returns the prediction if the prediction can be made.
     *
     * @param xValue The value of the X variable.
     * @param yValue The value of the Y variable.
     * @param excludedGroundings The triples that are forbidden to be used.
     *
     * @return A set which contains the prediction itself if the prediction can be made. An empty set if the prediction cannot be made.
     */
    override fun getTripleExplanation(
        head: Int,
        tail: Int,
        excludedTriples: Set<Triple>,
        triples: TripleSet
    ): Set<Triple> {
        val groundings = hashSetOf<Triple>()
        val prediction = tarmorn.data.Triple(head, this.targetRelation, tail)
        if (excludedTriples.contains(prediction)) return groundings
        if (this.isXRule && tail == this.head.t) {
            groundings.add(prediction)
            return groundings
        }
        if (this.isYRule && head == this.head.h) {
            groundings.add(prediction)
            return groundings
        }
        return groundings
    }

    /**
     * Does not recompute the scores of the zeor rule, but simply returns the scores of that rule.
     */
    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        return IntArray(2)
    }
}
