package tarmorn.structure

import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.data.IdManager

class RuleUntyped : Rule {
    constructor(head: Atom) {
        this.head = head
        this.body = Body()
    }


    constructor() {
        this.body = Body()
    }

    val isCyclic: Boolean
        get() {
            if (this.head == null) return false
            if (this.head.isLeftC || this.head.isRightC) return false
            else return true
        }

    val isAcyclic1: Boolean
        get() {
            if (this.isCyclic || this.isZero) return false
            else {
                if (this.body[this.bodysize() - 1].isLeftC || this.body[this.bodysize() - 1].isRightC) {
                    return true
                }
                return false
            }
        }

    val isAcyclic2: Boolean
        get() {
            if (this.isCyclic || this.isZero) return false
            else {
                if (this.body[this.bodysize() - 1].isLeftC || this.body[this.bodysize() - 1].isRightC) {
                    return false
                }
                return true
            }
        }

    val isZero: Boolean
        get() {
            if (this.bodysize() == 0) return true
            else return false
        }

    constructor(predicted: Int, correctlyPredicted: Int, confidence: Double) {
        this.predicted = predicted
        this.correctlyPredicted = correctlyPredicted
        this.confidence = confidence
        this.body = Body()
    }


    val leftRightGeneralization: RuleUntyped?
        get() {
            val lrG = this.createCopy()
            val leftConstant = lrG.head.left
            val xcount = lrG.replaceByVariable(leftConstant, IdManager.getXId())
            val rightConstant = lrG.head.right
            val ycount = lrG.replaceByVariable(rightConstant, IdManager.getYId())
            if (xcount < 2 || ycount < 2) return null
            return lrG
        }

    val leftGeneralization: RuleUntyped?
        get() {
            val leftG = this.createCopy()
            val leftConstant = leftG.head.left
            val xcount = leftG.replaceByVariable(leftConstant, IdManager.getXId())
            if (this.bodysize() == 0) return leftG
            if (xcount < 2) return null
            return leftG
        }

    val rightGeneralization: RuleUntyped?
        get() {
            val rightG = this.createCopy()
            val rightConstant = rightG.head.right
            val ycount = rightG.replaceByVariable(rightConstant, IdManager.getYId())
            if (this.bodysize() == 0) return rightG
            if (ycount < 2) return null
            return rightG
        }


    fun createCopy(): RuleUntyped {
        val copy = RuleUntyped(this.head.copy())
        for (bodyLiteral in this.body) {
            copy.body.add(bodyLiteral.copy())
        }
        copy.nextFreeVariable = this.nextFreeVariable
        return copy
    }

    protected fun replaceByVariable(constant: Int, variable: Int): Int {
        var count = this.head.replaceByVariable(constant, variable)
        for (batom in this.body) {
            val bcount = batom.replaceByVariable(constant, variable)
            count += bcount
        }
        return count
    }

    fun replaceNearlyAllConstantsByVariables() {
        var counter = 0
        for (atom in body) {
            counter++
            if (counter == body.size) break
            if (atom.isLeftC) {
                val c = atom.left
                this.replaceByVariable(c, variables[this.nextFreeVariable])
                this.nextFreeVariable++
            }
            if (atom.isRightC) {
                val c = atom.right
                this.replaceByVariable(c, variables[this.nextFreeVariable])
                this.nextFreeVariable++
            }
        }
    }


    fun replaceAllConstantsByVariables() {
        for (atom in body) {
            if (atom.isLeftC) {
                val c = atom.left
                this.replaceByVariable(c, variables[this.nextFreeVariable])
                this.nextFreeVariable++
            }
            if (atom.isRightC) {
                val c = atom.right
                this.replaceByVariable(c, variables[this.nextFreeVariable])
                this.nextFreeVariable++
            }
        }
    }


    override fun computeTailResults(head: Int, ts: TripleSet): HashSet<Int> {
        System.err.println("method not available for an untyped rule")
        return HashSet<Int>()
    }


    override fun computeHeadResults(tail: Int, ts: TripleSet): HashSet<Int> {
        System.err.println("method not available for an untyped rule")
        return HashSet<Int>()
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

    override fun getPredictions(ts: TripleSet): ArrayList<Triple>? {
        System.err.println("method not available for an untyped rule")
        return null
    }


    override fun isSingleton(triples: TripleSet): Boolean {
        // does nit really make sense for this type
        return false
    }


    override fun getTripleExplanation(
        xValue: Int,
        yValue: Int,
        excludedTriples: java.util.HashSet<Triple>,
        triples: TripleSet
    ): java.util.HashSet<Triple> {
        System.err.println("Your are asking for a triple explanation using an untyped rule. Such a rule cannot explain anything.")
        return HashSet<Triple>()
    }


    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        System.err.println("method not available for an untyped rule")
        return IntArray(2)
    }
}
