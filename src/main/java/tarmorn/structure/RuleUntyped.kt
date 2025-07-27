package tarmorn.structure

import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.data.IdManager

class RuleUntyped : Rule {
    constructor(head: Atom, body: Atom? = null) {
        this.head = head
        this.body = body
    }

    constructor() {
        this.body = null
    }

    val isCyclic: Boolean
        get() {
            if (this.head == null) return false
            if (this.head.ishC || this.head.istC) return false
            else return true
        }

    val isAcyclic1: Boolean
        get() {
            if (this.isCyclic || this.isZero) return false
            else {
                return body?.ishC == true || body?.istC == true
            }
        }

    val isAcyclic2: Boolean
        get() {
            if (this.isCyclic || this.isZero) return false
            else {
                return body?.ishC != true && body?.istC != true
            }
        }

    val isZero: Boolean
        get() = body == null

    constructor(predicted: Int, correctlyPredicted: Int, confidence: Double) {
        this.predicted = predicted
        this.correctlyPredicted = correctlyPredicted
        this.confidence = confidence
        this.body = null
    }

    val leftRightGeneralization: RuleUntyped?
        get() {
            val lrG = this.createCopy()
            val leftConstant = lrG.head.h
            val xcount = lrG.replaceByVariable(leftConstant, IdManager.getXId())
            val rightConstant = lrG.head.t
            val ycount = lrG.replaceByVariable(rightConstant, IdManager.getYId())
            if (xcount < 2 || ycount < 2) return null
            return lrG
        }

    val leftGeneralization: RuleUntyped?
        get() {
            val leftG = this.createCopy()
            val leftConstant = leftG.head.h
            val xcount = leftG.replaceByVariable(leftConstant, IdManager.getXId())
            if (this.bodySize == 0) return leftG
            if (xcount < 2) return null
            return leftG
        }

    val rightGeneralization: RuleUntyped?
        get() {
            val rightG = this.createCopy()
            val rightConstant = rightG.head.t
            val ycount = rightG.replaceByVariable(rightConstant, IdManager.getYId())
            if (this.bodySize == 0) return rightG
            if (ycount < 2) return null
            return rightG
        }

    fun createCopy(): RuleUntyped {
        val copy = RuleUntyped(this.head.copy())
        copy.body = this.body?.copy()
        copy.nextFreeVariable = this.nextFreeVariable
        return copy
    }

    protected fun replaceByVariable(constant: Int, variable: Int): Int {
        var count = this.head.replaceByVariable(constant, variable)
        body?.let {
            count += it.replaceByVariable(constant, variable)
        }
        return count
    }

    fun replaceNearlyAllConstantsByVariables() {
        // For single body atom, don't replace anything in the body
        val bodyAtom = body
        if (bodyAtom != null) {
            // Keep body constants as they are - this is for acyclic rules
            // Only head gets variable replacement through generalization
        }
    }

    fun replaceAllConstantsByVariables() {
        val bodyAtom = body
        if (bodyAtom != null) {
            if (bodyAtom.ishC) {
                val c = bodyAtom.h
                this.replaceByVariable(c, variables[this.nextFreeVariable])
                this.nextFreeVariable++
            }
            if (bodyAtom.istC) {
                val c = bodyAtom.t
                this.replaceByVariable(c, variables[this.nextFreeVariable])
                this.nextFreeVariable++
            }
        }
    }

    override fun computeTailResults(head: Int, ts: TripleSet): Set<Int> {
        System.err.println("method not available for an untyped rule")
        return hashSetOf()
    }

    override fun computeHeadResults(tail: Int, ts: TripleSet): Set<Int> {
        System.err.println("method not available for an untyped rule")
        return hashSetOf()
    }

    override fun computeScores(ts: TripleSet) {
        System.err.println("method not available for an untyped rule")
    }

    override fun isPredictedX(leftValue: Int, rightValue: Int, forbidden: Triple?, ts: TripleSet): Boolean {
        System.err.println("method not available for an untyped rule")
        return false
    }

    override fun isRefinable(): Boolean {
        return false
    }

    override fun getRandomValidPrediction(ts: TripleSet): Triple? {
        System.err.println("method not available for an untyped rule")
        return null
    }

    override fun getRandomInvalidPrediction(ts: TripleSet): Triple? {
        System.err.println("method not available for an untyped rule")
        return null
    }

    override fun getPredictions(ts: TripleSet): List<Triple>? {
        System.err.println("method not available for an untyped rule")
        return null
    }

    override fun isSingleton(triples: TripleSet): Boolean {
        System.err.println("method not available for an untyped rule")
        return false
    }

    override fun getTripleExplanation(
        xValue: Int,
        yValue: Int,
        excludedTriples: Set<Triple>,
        triples: TripleSet
    ): Set<Triple> {
        System.err.println("method not available for an untyped rule")
        return hashSetOf()
    }

    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        System.err.println("method not available for an untyped rule")
        return intArrayOf(0, 0)
    }
}
