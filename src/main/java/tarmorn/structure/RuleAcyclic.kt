package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.MyTriple
import tarmorn.data.TripleSet
import tarmorn.data.IdManager

class RuleAcyclic(r: RuleUntyped) : Rule(r) {
    private var unboundVariable0: Int? = null

    protected val unboundVariable: Int?
        get() {
            if (unboundVariable0 != null) return unboundVariable0
            
            val counter = mutableMapOf<Int, Int>()
            
            body.forEach { atom ->
                listOf(atom.h, atom.t)
                    .filter { it != IdManager.getXId() && it != IdManager.getYId() && it <= 0 }
                    .forEach { variable -> 
                        counter[variable] = counter.getOrDefault(variable, 0) + 1
                    }
            }
            
            unboundVariable0 = counter.entries.find { it.value == 1 }?.key
            return unboundVariable0
        }

    // Convenience properties to identify rule type
    val isAcyclic1: Boolean
        get() = unboundVariable == null  // RuleAcyclic1 logic: last atom has constant
    
    val isAcyclic2: Boolean
        get() = unboundVariable != null  // RuleAcyclic2 logic: last atom has unbound variable

    override val appliedConfidence: Double
        get() = if (unboundVariable != null) {
            Settings.RULE_AC2_WEIGHT * super.appliedConfidence
        } else {
            super.appliedConfidence
        }

    override fun computeTailResults(head: Int, ts: TripleSet): Set<Int> = when {
        isXRule -> {
            val headRightId = this.head.t
            if (headRightId == head) emptySet()
            else {
                val previousValues = hashSetOf(head, this.head.t)
                if (isBodyTrueAcyclic(IdManager.getXId(), head, 0, previousValues, ts)) {
                    setOf(headRightId)
                } else {
                    emptySet()
                }
            }
        }
        this.head.h == head -> {
            hashSetOf<Int>().apply { computeValuesReversed(IdManager.getYId(), this, ts) }
        }
        else -> emptySet()
    }

    override fun computeHeadResults(tail: Int, ts: TripleSet): Set<Int> = when {
        isYRule -> {
            val headLeftId = head.h
            if (headLeftId == tail) emptySet()
            else {
                val previousValues = hashSetOf(tail, head.h)
                if (isBodyTrueAcyclic(IdManager.getYId(), tail, 0, previousValues, ts)) {
                    setOf(headLeftId)
                } else {
                    emptySet()
                }
            }
        }
        isXRule && head.t == tail -> {
            hashSetOf<Int>().apply { computeValuesReversed(IdManager.getXId(), this, ts) }
        }
        else -> emptySet()
    }


    override fun computeScores(triples: TripleSet) {
        val (targetVariable, targetConstantId) = when {
            isXRule -> IdManager.getXId() to head.t
            else -> IdManager.getYId() to head.h
        }
        val values = hashSetOf<Int>()
        
        computeValuesReversed(targetVariable, values, triples)
        
        val relationId = head.r
        val correctlyPredicted = values.count { valueId ->
            when {
                isXRule -> triples.isTrue(valueId, relationId, targetConstantId)
                else -> triples.isTrue(targetConstantId, relationId, valueId)
            }
        }
        
        this.predicted = values.size
        this.correctlyPredicted = correctlyPredicted
        this.confidence = if (values.isNotEmpty()) correctlyPredicted.toDouble() / values.size else 0.0
    }


    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        val unboundVariable = this.unboundVariable
        
        return if (unboundVariable == null) {
            // RuleAcyclic1 logic
            val (targetVariable, constantValueId) = when {
                isXRule -> IdManager.getXId() to head.t
                else -> IdManager.getYId() to head.h
            }
            val values = hashSetOf<Int>()
            
            computeValuesReversed(targetVariable, values, triples)
            
            val relationId = head.r
            val (predictedBoth, correctlyPredictedBoth) = values.fold(0 to 0) { (predicted, correct), valueId ->
                val explanation = that.getTripleExplanation(
                    if (isXRule) valueId else constantValueId,
                    if (isXRule) constantValueId else valueId,
                    hashSetOf(),
                    triples
                )
                
                if (explanation.isNotEmpty()) {
                    val isCorrect = when {
                        isXRule -> triples.isTrue(valueId, relationId, constantValueId)
                        else -> triples.isTrue(constantValueId, relationId, valueId)
                    }
                    predicted + 1 to if (isCorrect) correct + 1 else correct
                } else {
                    predicted to correct
                }
            }
            
            intArrayOf(predictedBoth, correctlyPredictedBoth)
        } else {
            // RuleAcyclic2 logic: not yet available
            System.err.println("Method not yet available for an untyped rule")
            intArrayOf(0, 0)
        }
    }


    override fun isPredictedX(leftValue: Int, rightValue: Int, forbidden: MyTriple?, ts: TripleSet): Boolean = when {
        forbidden == null -> {
            val (variable, value) = when {
                isXRule -> IdManager.getXId() to leftValue
                else -> IdManager.getYId() to rightValue
            }
            val previousValues = hashSetOf(value)
            isBodyTrueAcyclic(variable, value, 0, previousValues, ts)
        }
        isXRule -> {
            val previousValues = hashSetOf(leftValue)
            isBodyTrueAcyclicX(IdManager.getXId(), leftValue, 0, forbidden, previousValues, ts)
        }
        else -> {
            val previousValues = hashSetOf(rightValue)
            isBodyTrueAcyclicX(IdManager.getYId(), rightValue, 0, forbidden, previousValues, ts)
        }
    }


    protected fun isBodyTrueAcyclic(
        variable: Int,
        value: Int,
        bodyIndex: Int,
        previousValues: HashSet<Int>,
        triples: TripleSet
    ): Boolean {
        val atom = body[bodyIndex]
        val ifHead = atom.h == variable
        
        // Check if this is the last atom
        return if (body.size - 1 == bodyIndex) {
            val constant = if (ifHead) atom.istC else atom.ishC
            
            if (constant) {
                val constantValue = if (ifHead) atom.t else atom.h
                when {
                    previousValues.contains(constantValue) && constantValue != head.constant -> false
                    ifHead -> triples.isTrue(value, atom.r, constantValue)
                    else -> triples.isTrue(constantValue, atom.r, value)
                }
            } else {
                val results = triples.getEntities(atom.r, value, ifHead)
                results.any { !previousValues.contains(it) }
            }
        } else {
            val results = triples.getEntities(atom.r, value, ifHead)
            val nextVariable = if (ifHead) atom.t else atom.h
            
            results.any { nextValueId ->
                val nextValue = nextValueId
                if (!previousValues.contains(nextValue)) {
                    previousValues.add(nextValue)
                    val result = isBodyTrueAcyclic(nextVariable, nextValue, bodyIndex + 1, previousValues, triples)
                    previousValues.remove(nextValue)
                    result
                } else {
                    false
                }
            }
        }
    }

    override fun isRefinable(): Boolean {
        return false
    }

    private fun isBodyTrueAcyclicX(
        variable: Int,
        value: Int,
        bodyIndex: Int,
        forbidden: MyTriple,
        previousValues: HashSet<Int>,
        triples: TripleSet
    ): Boolean {
        val atom = body[bodyIndex]
        val ifHead = atom.h == variable
        
        // Check if this is the last atom
        return if (body.size - 1 == bodyIndex) {
            val constant = if (ifHead) atom.istC else atom.ishC
            
            if (constant) {
                val constantValue = if (ifHead) atom.t else atom.h
                when {
                    previousValues.contains(constantValue) && constantValue != head.constant -> false
                    ifHead -> triples.isTrue(value, atom.r, constantValue)
                    else -> triples.isTrue(constantValue, atom.r, value)
                }
            } else {
                val results = triples.getEntities(atom.r, value, ifHead)
                results.any { !previousValues.contains(it) }
            }
        } else {
            val results = triples.getEntities(atom.r, value, ifHead)
            val nextVariable = if (ifHead) atom.t else atom.h
            
            results.any { nextValueId ->
                val nextValue = nextValueId
                if (!forbidden.equals(ifHead, value, atom.r, nextValueId) &&
                    !previousValues.contains(nextValue)) {
                    previousValues.add(nextValue)
                    val result = isBodyTrueAcyclicX(nextVariable, nextValue, bodyIndex + 1, forbidden, previousValues, triples)
                    previousValues.remove(nextValue)
                    result
                } else {
                    false
                }
            }
        }
    }


    fun computeValuesReversed(targetVariable: Int, targetValues: HashSet<Int>, ts: TripleSet) {
        computeValuesReversedInternal(targetVariable, targetValues, ts)
    }

    private fun computeValuesReversedInternal(targetVariable: Int, targetValues: HashSet<Int>, ts: TripleSet) {
        val atomIndex = body.size - 1
        val lastAtom = body[atomIndex]
        val unboundVariable = this.unboundVariable
        
        if (unboundVariable == null) {
            val nextVarIsLeft = !lastAtom.ishC
            val constant = lastAtom.getLR(!nextVarIsLeft)
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            val values = ts.getEntities(lastAtom.r, constant, !nextVarIsLeft)
            val previousValues = hashSetOf(constant, head.constant)
            
            values.forEachIndexed { counter, valueId ->
                forwardReversed(nextVariable, valueId, atomIndex - 1, targetVariable, targetValues, ts, previousValues)
                
                val shouldStop = when {
                    !APPLICATION_MODE -> targetValues.size >= Settings.SAMPLE_SIZE || counter >= Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS
                    else -> targetValues.size >= Settings.DISCRIMINATION_BOUND
                }
                
                if (shouldStop) {
                    if (APPLICATION_MODE) targetValues.clear()
                    return
                }
            }
        } else {
            val nextVarIsLeft = lastAtom.h != unboundVariable
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            val triples = ts.getTriplesByRelation(lastAtom.r)
            
            triples.forEachIndexed { counter, t ->
                val value = t.getValue(nextVarIsLeft)
                val previousValues = hashSetOf(
                    t.getValue(!nextVarIsLeft),
                    head.constant
                )
                forwardReversed(nextVariable, value, atomIndex - 1, targetVariable, targetValues, ts, previousValues)
                
                val shouldStop = when {
                    !APPLICATION_MODE -> targetValues.size >= Settings.SAMPLE_SIZE || counter >= Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS
                    else -> targetValues.size >= Settings.DISCRIMINATION_BOUND
                }
                
                if (shouldStop) {
                    if (APPLICATION_MODE) targetValues.clear()
                    return
                }
            }
        }
    }


    fun beamValuesReversed(targetVariable: Int, targetValues: HashSet<Int>, ts: TripleSet) {
        val atomIndex = body.size - 1
        val lastAtom = body[atomIndex]
        if (getGroundingsLastAtom(ts) < Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) return

        val unboundVariable = this.unboundVariable
        
        if (unboundVariable == null) {
            val nextVarIsLeft = !lastAtom.ishC
            val constant = lastAtom.getLR(!nextVarIsLeft)
            val nextVariable = lastAtom.getLR(nextVarIsLeft)

            repeat(Settings.SAMPLE_SIZE + 1) {
                val valueId = ts.getRandomEntity(lastAtom.r, constant, !nextVarIsLeft) ?: return
                val previousValues = hashSetOf(constant, head.constant)
                beamForwardReversed(nextVariable, valueId, atomIndex - 1, targetVariable, ts, previousValues)
                    ?.let { targetValues.add(it) }
            }
        } else {
            val nextVarIsLeft = lastAtom.h != unboundVariable
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            
            repeat(Settings.SAMPLE_SIZE + 1) {
                val t = ts.getRandomTripleByRelation(lastAtom.r) ?: return
                val value = t.getValue(nextVarIsLeft)
                val previousValues = hashSetOf(
                    t.getValue(!nextVarIsLeft),
                    head.constant
                )
                beamForwardReversed(nextVariable, value, atomIndex - 1, targetVariable, ts, previousValues)
                    ?.let { targetValues.add(it) }
            }
        }
    }


    private fun forwardReversed(
        variable: Int,
        value: Int,
        bodyIndex: Int,
        targetVariable: Int,
        targetValues: HashSet<Int>,
        ts: TripleSet,
        previousValues: HashSet<Int>
    ) {
        if (previousValues.contains(value)) return
        
        if (bodyIndex < 0) {
            targetValues.add(value)
        } else {
            val currentValues = hashSetOf<Int>().apply {
                add(value)
                addAll(previousValues)
            }
            
            if (!APPLICATION_MODE && targetValues.size >= Settings.SAMPLE_SIZE) return
            
            val atom = body[bodyIndex]
            val nextVarIsLeft = atom.h != variable
            val nextVariable = atom.getLR(nextVarIsLeft)
            val nextValues = ts.getEntities(atom.r, value, !nextVarIsLeft)
            
            nextValues.forEach { nextValueId ->
                forwardReversed(nextVariable, nextValueId, bodyIndex - 1, targetVariable, targetValues, ts, currentValues)
            }
        }
    }

    private fun beamForwardReversed(
        variable: Int,
        value: Int,
        bodyIndex: Int,
        targetVariable: Int,
        ts: TripleSet,
        previousValues: HashSet<Int>
    ): Int? {
        if (previousValues.contains(value)) return null

        return if (bodyIndex < 0) {
            value
        } else {
            previousValues.add(value)
            val atom = body[bodyIndex]
            val nextVarIsLeft = atom.h != variable
            val nextVariable = atom.getLR(nextVarIsLeft)
            val nextValueId = ts.getRandomEntity(atom.r, value, !nextVarIsLeft)
            
            nextValueId?.let { 
                beamForwardReversed(nextVariable, it, bodyIndex - 1, targetVariable, ts, previousValues)
            }
        }
    }

    override fun getRandomValidPrediction(ts: TripleSet): MyTriple? {
        val validPredictions = getPredictions(ts, 1)
        return validPredictions.randomOrNull()
    }

    override fun getRandomInvalidPrediction(ts: TripleSet): MyTriple? {
        val invalidPredictions = getPredictions(ts, -1)
        return invalidPredictions.randomOrNull()
    }

    override fun getPredictions(ts: TripleSet): ArrayList<MyTriple> = getPredictions(ts, 0)

    /**
     * @param valid 1 = valid; -1 = invalid; 0 = valid/invalid does not matter
     */
    protected fun getPredictions(ts: TripleSet, valid: Int): ArrayList<MyTriple> {
        val materialized = arrayListOf<MyTriple>()
        val resultSet = when {
            isXRule -> computeHeadResults(head.t, ts)
            else -> computeTailResults(head.h, ts)
        }
        
        val relationId = head.r
        val headLeftId = head.h
        val headRightId = head.t
        
        resultSet.forEach { vId ->
            val t = when {
                isXRule -> MyTriple(vId, relationId, headRightId)
                else -> MyTriple(headLeftId, relationId, vId)
            }
            
            when (valid) {
                1 -> if (ts.isTrue(t)) materialized.add(t)
                -1 -> if (!ts.isTrue(t)) materialized.add(t)
                else -> materialized.add(t)
            }
        }
        
        return materialized
    }

    fun getGroundingsLastAtom(triples: TripleSet): Int {
        val last = body.last
        val unboundVariable = this.unboundVariable
        
        return if (unboundVariable == null) {
            // RuleAcyclic1 logic: last atom has constant
            when {
                last.istC -> triples.getHeadEntities(last.r, last.t).size
                else -> triples.getTailEntities(last.r, last.h).size
            }
        } else {
            // RuleAcyclic2 logic: last atom has unbound variable
            val values = hashSetOf<Int>()
            val targetTriples = triples.getTriplesByRelation(last.r)
            
            when {
                last.t == unboundVariable -> {
                    targetTriples.forEach { t ->
                        values.add(t.h)
                        if (values.size >= Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) 
                            return values.size
                    }
                    values.size
                }
                else -> {
                    targetTriples.forEach { t ->
                        values.add(t.t)
                        if (values.size >= Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) 
                            return values.size
                    }
                    values.size
                }
            }
        }
    }

    override fun isSingleton(triples: TripleSet): Boolean {
        val unboundVariable = this.unboundVariable
        
        return if (unboundVariable == null) {
            // RuleAcyclic1 logic
            val firstAtom = body[0]
            when {
                firstAtom.t == IdManager.getXId() && firstAtom.t == IdManager.getYId() -> {
                    val head = firstAtom.h
                    val relation = firstAtom.r
                    triples.getTailEntities(relation, head).size <= 1
                }
                else -> {
                    val tail = firstAtom.t
                    val relation = firstAtom.r
                    triples.getHeadEntities(relation, tail).size <= 1
                }
            }
        } else {
            // RuleAcyclic2 logic: always false
            false
        }
    }

    val isCyclic: Boolean
        get() = head.constant == body.last.constant

    override fun getTripleExplanation(
        xValue: Int,
        yValue: Int,
        excludedTriples: Set<MyTriple>,
        triples: TripleSet
    ): Set<MyTriple> {
        val unboundVariable = this.unboundVariable
        
        return if (unboundVariable == null) {
            // RuleAcyclic1 logic
            if (bodySize != 1) {
                System.err.println("Trying to get a triple explanation for an acyclic rule with constant in head any body of length != 1. This is not yet implemented.")
                System.exit(-1)
                return emptySet()
            }
            
            val groundings = hashSetOf<MyTriple>()
            val xInHead = head.h == IdManager.getXId()
            
            if (xInHead) {
                if (head.t == yValue) {
                    val bodyAtom = body[0]
                    val left = bodyAtom.h
                    val right = bodyAtom.t
                    val rel = bodyAtom.r
                    
                    when {
                        left == IdManager.getXId() && triples.isTrue(xValue, rel, right) -> {
                            val t = MyTriple(xValue, rel, right)
                            if (!excludedTriples.contains(t)) groundings.add(t)
                        }
                        right == IdManager.getXId() && triples.isTrue(left, rel, xValue) -> {
                            val t = MyTriple(left, rel, xValue)
                            if (!excludedTriples.contains(t)) groundings.add(t)
                        }
                    }
                }
            } else {
                if (head.h == xValue) {
                    val bodyAtom = body[0]
                    val left = bodyAtom.h
                    val right = bodyAtom.t
                    val rel = bodyAtom.r
                    
                    when {
                        left == IdManager.getYId() && triples.isTrue(yValue, rel, right) -> {
                            val t = MyTriple(yValue, rel, right)
                            if (!excludedTriples.contains(t)) groundings.add(t)
                        }
                        right == IdManager.getYId() && triples.isTrue(left, rel, yValue) -> {
                            val t = MyTriple(left, rel, yValue)
                            if (!excludedTriples.contains(t)) groundings.add(t)
                        }
                    }
                }
            }
            groundings
        } else {
            // RuleAcyclic2 logic: not yet implemented
            System.err.println("You are asking for a triple explanation using an AC2 rule (a.k.a. U_d rule). Triple explanations for this rule are not yet implemented.")
            emptySet()
        }
    }

    fun toXYString(): String = when {
        head.h == IdManager.getXId() -> {
            val c = head.t
            buildString {
                append(head.toString(c, IdManager.getYId()))
                repeat(bodySize) { i ->
                    append(getBodyAtom(i)!!.toString(c, IdManager.getYId()))
                }
            }
        }
        head.t == IdManager.getYId() -> {
            val c = head.h
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
            targetRelation != relationId -> false
            // this rule is a X rule
            head.istC && head.t == tId -> {
                val previousValues = hashSetOf(hId, head.t)
                isBodyTrueAcyclic(IdManager.getXId(), hId, 0, previousValues, ts)
            }
            // this rule is a Y rule
            head.ishC && head.h == hId -> {
                val previousValues = hashSetOf(tId, head.h)
                isBodyTrueAcyclic(IdManager.getYId(), tId, 0, previousValues, ts)
            }
            else -> false
        }
    }

    /**
     * First replaces all atoms by deep copies of these atoms to avoid that references from the outside are affected by follow up changes.
     * Then corrects a rule which uses X in the head at the Y position by replacing X by Y in the head as well as all occurrences
     */
    fun detachAndPolish() {
        head = head.copy()
        body.detach()
        
        if (head.t == IdManager.getXId()) {
            head = head.copy(t = IdManager.getYId())
            repeat(bodySize) { i ->
                getBodyAtom(i).replace(IdManager.getXId(), IdManager.getYId())
            }
        }
    }
}