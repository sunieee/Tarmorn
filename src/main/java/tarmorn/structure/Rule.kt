package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import java.util.Random

abstract class Rule {
//    protected var head: Atom? = null
    // 欺骗编译器
    lateinit var head: Atom

    // protected ArrayList<Atom> body; 
    var body: Body


    protected var hashcode: Int = 0
    protected var hashcodeInitialized: Boolean = false


    var predicted: Int = 0
        protected set
    var correctlyPredicted: Int = 0
        protected set
    var confidence: Double = 0.0
        protected set

    protected var nextFreeVariable: Int = 0

    constructor(r: RuleUntyped) {
        this.body = r.body
        this.head = r.head
        this.confidence = r.confidence
        this.correctlyPredicted = r.correctlyPredicted
        this.predicted = r.predicted
    }


    constructor(head: Atom) {
        this.head = head
        this.body = Body()
    }

    constructor() {
        this.body = Body()
    }


    val copy: Rule?
        // ***********************
        get() {
            val copy = RuleUntyped(this.head!!.createCopy())
            for (bodyLiteral in this.body) {
                copy.body.add(bodyLiteral.createCopy())
            }
            copy.nextFreeVariable = this.nextFreeVariable // ???
            if (copy.isCyclic) {
                val r = RuleCyclic(copy, 0.0)
                return r
            }
            if (copy.isAcyclic1) {
                val r = RuleAcyclic1(copy)
                return r
            }
            if (copy.isAcyclic2) {
                val r = RuleAcyclic2(copy)
                return r
            }
            return null
        }

    fun addBodyAtom(atom: Atom) {
        this.body.add(atom)
    }

    fun getBodyAtom(index: Int): Atom {
        return this.body.get(index)
    }

    val targetRelation: String
        get() = this.head.relation


    /*
   public double getConfidenceMax() {
       return Math.max(this.confidenceHeads, this.confidenceTails);
   }
   */
    fun bodysize(): Int {
        return this.body.size()
    }

    val isTrivial: Boolean
        get() {
            if (this.bodysize() == 1) {
                if (this.head == this.body.get(0)) return true
            }
            return false
        }

    open val appliedConfidence: Double
        // public abstract double getAppliedConfidenceHeads();
        get() = this.correctlyPredicted.toDouble() / (this.predicted.toDouble() + Settings.UNSEEN_NEGATIVE_EXAMPLES)


    val isXYRule: Boolean
        get() {
            if (this.head.isLeftC || this.head.isRightC) return false
            else return true
        }

    val isXRule: Boolean
        get() {
            if (this.isXYRule) return false
            else {
                if (!this.head.isLeftC) return true
                else return false
            }
        }

    val isYRule: Boolean
        get() {
            if (this.isXYRule) return false
            else {
                if (!this.head.isRightC) return true
                else return false
            }
        }


    // ****************
    // *** TOSTRING ***
    // ****************
    override fun toString(): String {
        val sb = StringBuilder()
        sb.append(this.predicted.toString() + "\t")
        sb.append(this.correctlyPredicted.toString() + "\t")
        sb.append(this.confidence.toString() + "\t")
        sb.append(this.head)
        sb.append(" <= ")
        sb.append(this.body.toString())
        return sb.toString()
    }


    // ********************
    // *** EQUAL + HASH ***
    // ********************
    override fun equals(thatObject: Any?): Boolean {
        if (thatObject is Rule) {
            val that = thatObject
            if (this.head == that.head && this.body == that.body) {
                return true
            }
            return false
        }
        return false
    }

    override fun hashCode(): Int {
        if (!this.hashcodeInitialized) {
            val sb = StringBuilder(this.head.toString())
            for (atom in this.body) {
                sb.append(atom.toString())
            }
            this.hashcode = sb.toString().hashCode()
            // this.hashcode = this.toString().hashCode();
            this.hashcodeInitialized = true
        }

        return this.hashcode
    }


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
     *
     * @param head The given head value.
     * @param ts The triple set used for computing the results.
     * @return An empty set, a set with one value (the constant of the rule) or the set of all body instantiations.
     */
    abstract fun computeTailResults(head: String, ts: TripleSet): HashSet<String>


    /**
     * Returns the head results of applying this rule to a given tail value.
     *
     * @param tail The given tail value.
     * @param ts The triple set used for computing the results.
     * @return An empty set, a set with one value (the constant of the rule) or the set of all body instantiations.
     */
    abstract fun computeHeadResults(tail: String, ts: TripleSet): HashSet<String>

    /**
     * Checks if the body of the rule is true for the given subject/object pair.
     * This method is called in the context of rule refinement (also called rule extension).
     *
     * @param leftValue The subject (or left value).
     * @param rightValue The object (or right value).
     * @param ts The triple set.
     * @return True if the value pair (or one of the values) is predicted.
     */
    // public abstract boolean isPredictedX(String leftValue, String rightValue, TripleSet ts);
    /**
     * Checks if the body of the rule is true for the given subject/object pair, while triviality is avoided by
     * not allowing that the predicted triple is used.
     * This method is called in the context of rule refinement (also called rule extension).
     *
     * @param leftValue The subject (or left value).
     * @param rightValue The object (or right value).
     * @param ts The triple set.
     * @return True if the value pair (or one of the values) is predicted.
     */
    abstract fun isPredictedX(leftValue: String, rightValue: String, forbidden: Triple?, ts: TripleSet): Boolean


    /**
     *
     *
     * @return True, if this rule is refineable. False otherwise.
     */
    abstract fun isRefinable(): Boolean

    /**
     * Returns a randomly chose triples that is both predicted and valid = true against the given triple set.
     *
     * @param ts Triple set deciding the truth of the triples
     * @return The predicted triple.
     */
    abstract fun getRandomValidPrediction(ts: TripleSet): Triple?


    /**
     * Returns a randomly chose triples that is both predicted and not valid = false against the given triple set.
     *
     * @param ts Triple set deciding the truth of the triples
     * @return The predicted triple.
     */
    abstract fun getRandomInvalidPrediction(ts: TripleSet): Triple?

    /**
     * Retrieves a sample of prediction (correct or incorrect).
     *
     * @param ts The triple set used for predicting.
     * @return A list of triples that are predicted,
     */
    abstract fun getPredictions(ts: TripleSet): ArrayList<Triple?>?


    /**
     * If the rule body has only one head variable, it is called singleton, if only one entity full fills the body.
     * @return
     */
    abstract fun isSingleton(triples: TripleSet): Boolean


    /**
     * Checks if a rule can fire given the observations without the excluded triples. If the rule fires, the triples used to fire are returned.
     *
     * @param xValue The value of the subject.
     * @param yValue The value of the object.
     * @param excludedTriples The triples that are not allowed to entail the entailment.
     * @param triples The triple set which is used to fire the rule (= the given observations).
     * @return The set of triples that was used to fire the rule. If null or the empty set is returned, then it was not possible to fire the rule.
     */
    abstract fun getTripleExplanation(
        xValue: String,
        yValue: String,
        excludedTriples: HashSet<Triple>,
        triples: TripleSet
    ): HashSet<Triple>


    open fun materialize(trainingSet: TripleSet): TripleSet? {
        return null
    }


    companion object {
        @JvmStatic
        protected var rand: Random = Random()
        @JvmStatic
        protected var APPLICATION_MODE: Boolean = false
        @JvmField
        val variables: Array<String> =
            arrayOf<String>("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P")
        @JvmField
        var variables2Indices: HashMap<String?, Int?> = HashMap<String?, Int?>()


        // ********************
        // *** CONSTRUCTORS ***
        // ********************
        init {
            for (i in variables.indices) {
                variables2Indices.put(variables[i], i)
            }
        }


        @JvmStatic
        fun applicationMode() {
            APPLICATION_MODE = true
        }
    }
}
