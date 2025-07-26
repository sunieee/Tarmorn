package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.data.IdManager
import java.util.*

abstract class Rule {
    lateinit var head: Atom
    var body = Body()

    // Use lazy initialization for hashcode
    private val hashcode by lazy { Objects.hash(head.toString(), body.toString()) }

    var predicted = 0
        protected set
    var correctlyPredicted = 0
        protected set
    var confidence = 0.0
        protected set

    protected var nextFreeVariable = 0

    constructor(r: RuleUntyped) {
        body = r.body
        head = r.head
        confidence = r.confidence
        correctlyPredicted = r.correctlyPredicted
        predicted = r.predicted
    }

    constructor(head: Atom) {
        this.head = head
    }

    constructor()


    val copy: Rule?
        get() {
            val copy = RuleUntyped(head.copy())
            body.forEach { copy.body.add(it.copy()) }
            copy.nextFreeVariable = nextFreeVariable
            
            return when {
                copy.isCyclic -> RuleCyclic(copy, 0.0)
                copy.isAcyclic1 || copy.isAcyclic2 -> RuleAcyclic(copy) 
                else -> null
            }
        }

    fun addBodyAtom(atom: Atom) = body.add(atom)

    fun getBodyAtom(index: Int) = body[index]

    // Remove redundant getter declarations
    val targetRelation get() = head.relation

    val bodySize get() = body.size

    val isTrivial get() = bodySize == 1 && head == body[0]

    open val appliedConfidence: Double
        get() = correctlyPredicted.toDouble() / (predicted.toDouble() + Settings.UNSEEN_NEGATIVE_EXAMPLES)

    val isXYRule get() = !head.isLeftC && !head.isRightC
    val isXRule get() = !isXYRule && !head.isLeftC
    val isYRule get() = !isXYRule && !head.isRightC

    override fun toString() = buildString {
        append("$predicted\t$correctlyPredicted\t$confidence\t$head <= $body")
    }

    override fun equals(other: Any?) = when {
        this === other -> true
        other !is Rule -> false
        else -> head == other.head && body == other.body
    }

    override fun hashCode() = hashcode


    // *************
    // *** LOGIC ***
    // *************
    /**
     * Computes the scores of the joint rule, i.e., the rule that has the conjunction of this and that rule body as body. If one of the rules
     * uses a constant and the other rule uses two variables, then one of the variables is replaced in both head and body of the rule by
     * that constant.
     *
     * @param that The other rule
     * @param triples The triple set used for computing the scores
     * @return An int[] with two elements. The first element (index 0) represents the all predictions made by the joint rules.
     * The second element (index 1) represents all correct predictions made by the joint rule
     */
    abstract fun computeScores(that: Rule, triples: TripleSet): IntArray

    /**
     *
     * @param ts
     */
    abstract fun computeScores(ts: TripleSet)

    /**
     * Returns the tail results of applying this rule to a given head value.
     */
    abstract fun computeTailResults(head: Int, ts: TripleSet): Set<Int>

    /**
     * Returns the head results of applying this rule to a given tail value.
     */
    abstract fun computeHeadResults(tail: Int, ts: TripleSet): Set<Int>

    /**
     * Checks if the body of the rule is true for the given subject/object pair.
     * This method is called in the context of rule refinement (also called rule extension).
     */
    abstract fun isPredictedX(leftValue: Int, rightValue: Int, forbidden: Triple?, ts: TripleSet): Boolean

    /**
     * Returns true if this rule is refineable.
     */
    abstract fun isRefinable(): Boolean

    /**
     * Returns a randomly chosen triple that is both predicted and valid against the given triple set.
     */
    abstract fun getRandomValidPrediction(ts: TripleSet): Triple?

    /**
     * Returns a randomly chosen triple that is both predicted and not valid against the given triple set.
     */
    abstract fun getRandomInvalidPrediction(ts: TripleSet): Triple?

    /**
     * Retrieves a sample of predictions (correct or incorrect).
     */
    abstract fun getPredictions(ts: TripleSet): List<Triple>?

    /**
     * If the rule body has only one head variable, it is called singleton, if only one entity fulfills the body.
     */
    abstract fun isSingleton(triples: TripleSet): Boolean

    /**
     * Checks if a rule can fire given the observations without the excluded triples.
     * If the rule fires, the triples used to fire are returned.
     */
    abstract fun getTripleExplanation(
        xValue: Int,
        yValue: Int,
        excludedTriples: Set<Triple>,
        triples: TripleSet
    ): Set<Triple>

    open fun materialize(trainingSet: TripleSet): TripleSet? = null


    companion object {
        @JvmStatic
        protected val rand = Random()
        @JvmStatic
        protected var APPLICATION_MODE = false
        
        val variables = arrayOf(
            "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P"
        ).map(IdManager::getEntityId).toTypedArray()
        
        val variables2Indices = variables.mapIndexed { index, variable ->
            variable to index
        }.toMap().toMutableMap()

        fun applicationMode() {
            APPLICATION_MODE = true
        }
    }
}
