package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.exceptions.RuleFunctionalityBasicSupportOnly

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
    override fun computeScores(ts: TripleSet) {
        val c = this.head.constant
        val tr = this.targetRelation
        val cIsHead = this.head.isLeftC

        val triplesTR = ts.getTriplesByRelation(tr)
        val triplesTRC = ts.getEntities(tr, c, cIsHead)

        this.predicted = triplesTR.size
        this.correctlyPredicted = triplesTRC.size
        this.confidence = correctlyPredicted.toDouble() / predicted.toDouble()
    }

    override fun computeTailResults(head: String, ts: TripleSet): HashSet<String> {
        val results = HashSet<String>()
        if (this.head.isRightC) {
            results.add(this.head.right)
        }
        return results
    }

    override fun computeHeadResults(tail: String, ts: TripleSet): HashSet<String> {
        val results = HashSet<String>()
        if (this.head.isLeftC) {
            results.add(this.head.left)
        }
        return results
    }

    override val appliedConfidence: Double
        get() = Settings.RULE_ZERO_WEIGHT * super.appliedConfidence


    override fun isPredictedX(leftValue: String, rightValue: String, forbidden: Triple?, ts: TripleSet): Boolean {
        throw RuleFunctionalityBasicSupportOnly()
    }

    override fun isRefinable(): Boolean {
        return false
    }


    override fun getRandomValidPrediction(ts: TripleSet): Triple? {
        throw RuleFunctionalityBasicSupportOnly()
    }


    override fun getRandomInvalidPrediction(ts: TripleSet): Triple? {
        throw RuleFunctionalityBasicSupportOnly()
    }


    override fun getPredictions(ts: TripleSet): ArrayList<Triple?>? {
        throw RuleFunctionalityBasicSupportOnly()
    }

    override fun isSingleton(triples: TripleSet): Boolean {
        throw RuleFunctionalityBasicSupportOnly()
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
        head: String,
        tail: String,
        excludedTriples: HashSet<Triple>,
        triples: TripleSet
    ): HashSet<Triple> {
        val groundings = HashSet<Triple>()
        val prediction = tarmorn.data.Triple(head, this.targetRelation, tail)
        if (excludedTriples.contains(prediction)) return groundings
        if (this.isXRule && tail == this.head.right) {
            groundings.add(prediction)
            return groundings
        }
        if (this.isYRule && head == this.head.left) {
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
