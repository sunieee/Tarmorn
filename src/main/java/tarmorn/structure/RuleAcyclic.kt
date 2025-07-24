package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet

abstract class RuleAcyclic(r: RuleUntyped) : Rule(r) {
    override fun computeTailResults(head: String, ts: TripleSet): HashSet<String> {
        val resultSet = hashSetOf<String>()
        
        return when {
            isXRule -> {
                if (this.head.right == head) return resultSet
                val previousValues = hashSetOf(head, this.head.right)
                if (isBodyTrueAcyclic("X", head, 0, previousValues, ts)) {
                    resultSet.add(this.head.right)
                }
                resultSet
            }
            this.head.left == head -> {
                computeValuesReversed("Y", resultSet, ts)
                resultSet
            }
            else -> resultSet
        }
    }

    /*
	public PriorityQueue<Candidate> computePTailResults(String head, TripleSet ts) {
		Timer count = new Timer();
		PriorityQueue<Candidate> resultSet = new PriorityQueue<Candidate>();
		if (this.isXRule) {
			if (this.head.getRight().equals(head)) return resultSet;
			HashSet<String> previousValues = new HashSet<String>();
			previousValues.add(head);
			previousValues.add(this.head.getRight());
			// TODO fix P here
			if (this.isBodyTrueAcyclic("X", head, 0, previousValues, ts)) {
				// resultSet.add(this.head.getRight());
				return resultSet;
			}
		}
		else {
			if (this.head.getLeft().equals(head)) {
				this.computePValuesReversed(1.0, "Y", resultSet, ts, count);
				return resultSet;
			}
		}
		return resultSet;
	}
	*/
    override fun computeHeadResults(tail: String, ts: TripleSet): HashSet<String> {
        val resultSet = hashSetOf<String>()
        
        return when {
            isYRule -> {
                if (head.left == tail) return resultSet
                val previousValues = hashSetOf(tail, head.left)
                if (isBodyTrueAcyclic("Y", tail, 0, previousValues, ts)) {
                    resultSet.add(head.left)
                }
                resultSet
            }
            isXRule && head.right == tail -> {
                computeValuesReversed("X", resultSet, ts)
                resultSet
            }
            else -> resultSet
        }
    }


    override fun computeScores(triples: TripleSet) {
        val (targetVariable, targetConstant) = if (isXRule) "X" to head.right else "Y" to head.left
        val values = hashSetOf<String>()
        
        computeValuesReversed(targetVariable, values, triples)
        
        val correctlyPredicted = values.count { value ->
            if (isXRule) {
                triples.isTrue(value, head.relation, targetConstant)
            } else {
                triples.isTrue(targetConstant, head.relation, value)
            }
        }
        
        this.predicted = values.size
        this.correctlyPredicted = correctlyPredicted
        this.confidence = if (values.isNotEmpty()) correctlyPredicted.toDouble() / values.size else 0.0
    }


    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        val (targetVariable, constantValue) = if (isXRule) "X" to head.right else "Y" to head.left
        val values = hashSetOf<String>()
        
        computeValuesReversed(targetVariable, values, triples)
        
        val (predictedBoth, correctlyPredictedBoth) = values.fold(0 to 0) { (predicted, correct), value ->
            val explanation = that.getTripleExplanation(
                if (isXRule) value else constantValue,
                if (isXRule) constantValue else value,
                hashSetOf(),
                triples
            )
            
            if (explanation.isNotEmpty()) {
                val isCorrect = if (isXRule) {
                    triples.isTrue(value, head.relation, constantValue)
                } else {
                    triples.isTrue(constantValue, head.relation, value)
                }
                predicted + 1 to if (isCorrect) correct + 1 else correct
            } else {
                predicted to correct
            }
        }
        
        return intArrayOf(predictedBoth, correctlyPredictedBoth)
    }


    override fun isPredictedX(leftValue: String, rightValue: String, forbidden: Triple?, ts: TripleSet): Boolean {
        return when {
            forbidden == null -> {
                val (variable, value) = if (isXRule) "X" to leftValue else "Y" to rightValue
                val previousValues = hashSetOf(value)
                isBodyTrueAcyclic(variable, value, 0, previousValues, ts)
            }
            isXRule -> {
                val previousValues = hashSetOf(leftValue)
                isBodyTrueAcyclicX("X", leftValue, 0, forbidden, previousValues, ts)
            }
            else -> {
                val previousValues = hashSetOf(rightValue)
                isBodyTrueAcyclicX("Y", rightValue, 0, forbidden, previousValues, ts)
            }
        }
    }


    protected fun isBodyTrueAcyclic(
        variable: String,
        value: String,
        bodyIndex: Int,
        previousValues: HashSet<String>,
        triples: TripleSet
    ): Boolean {
        val atom = body.get(bodyIndex)
        val ifHead = atom.left == variable
        
        // Check if this is the last atom
        if (body.size() - 1 == bodyIndex) {
            val constant = if (ifHead) atom.isRightC else atom.isLeftC
            
            return if (constant) {
                val constantValue = if (ifHead) atom.right else atom.left
                if (previousValues.contains(constantValue) && constantValue != head.constant) {
                    false
                } else {
                    if (ifHead) {
                        triples.isTrue(value, atom.relation, constantValue)
                    } else {
                        triples.isTrue(constantValue, atom.relation, value)
                    }
                }
            } else {
                val results = triples.getEntities(atom.relation, value, ifHead)
                results.any { !previousValues.contains(it) }
            }
        } else {
            val results = triples.getEntities(atom.relation, value, ifHead)
            val nextVariable = if (ifHead) atom.right else atom.left
            
            return results.any { nextValue ->
                if (previousValues.contains(nextValue)) {
                    false
                } else {
                    previousValues.add(nextValue)
                    val result = isBodyTrueAcyclic(nextVariable, nextValue, bodyIndex + 1, previousValues, triples)
                    previousValues.remove(nextValue)
                    result
                }
            }
        }
    }

    private fun isBodyTrueAcyclicX(
        variable: String,
        value: String,
        bodyIndex: Int,
        forbidden: Triple,
        previousValues: HashSet<String>,
        triples: TripleSet
    ): Boolean {
        val atom = body.get(bodyIndex)
        val ifHead = atom.left == variable
        
        // Check if this is the last atom
        if (body.size() - 1 == bodyIndex) {
            val constant = if (ifHead) atom.isRightC else atom.isLeftC
            
            return if (constant) {
                val constantValue = if (ifHead) atom.right else atom.left
                if (previousValues.contains(constantValue) && constantValue != head.constant) {
                    false
                } else {
                    if (ifHead) {
                        triples.isTrue(value, atom.relation, constantValue)
                    } else {
                        triples.isTrue(constantValue, atom.relation, value)
                    }
                }
            } else {
                val results = triples.getEntities(atom.relation, value, ifHead)
                results.any { !previousValues.contains(it) }
            }
        } else {
            val results = triples.getEntities(atom.relation, value, ifHead)
            val nextVariable = if (ifHead) atom.right else atom.left
            
            return results.any { nextValue ->
                if (!forbidden.equals(ifHead, value, atom.relation, nextValue) && 
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


    fun computeValuesReversed(targetVariable: String, targetValues: HashSet<String>, ts: TripleSet) {
        val atomIndex = body.size() - 1
        val lastAtom = body.get(atomIndex)
        val unboundVariable = this.unboundVariable
        
        if (unboundVariable == null) {
            val nextVarIsLeft = !lastAtom.isLeftC
            val constant = lastAtom.getLR(!nextVarIsLeft)
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            val values = ts.getEntities(lastAtom.relation, constant, !nextVarIsLeft)
            val previousValues = hashSetOf(constant, head.constant)
            
            values.forEachIndexed { counter, value ->
                forwardReversed(nextVariable, value, atomIndex - 1, targetVariable, targetValues, ts, previousValues)
                
                val shouldStop = if (!APPLICATION_MODE) {
                    targetValues.size >= Settings.SAMPLE_SIZE || counter >= Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS
                } else {
                    targetValues.size >= Settings.DISCRIMINATION_BOUND
                }
                
                if (shouldStop) {
                    if (APPLICATION_MODE) targetValues.clear()
                    return
                }
            }
        } else {
            val nextVarIsLeft = lastAtom.left != unboundVariable
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            val triples = ts.getTriplesByRelation(lastAtom.relation)
            
            triples.forEachIndexed { counter, t ->
                val value = t.getValue(nextVarIsLeft)
                val previousValues = hashSetOf(
                    t.getValue(!nextVarIsLeft),
                    head.constant
                )
                forwardReversed(nextVariable, value, atomIndex - 1, targetVariable, targetValues, ts, previousValues)
                
                val shouldStop = if (!APPLICATION_MODE) {
                    targetValues.size >= Settings.SAMPLE_SIZE || counter >= Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS
                } else {
                    targetValues.size >= Settings.DISCRIMINATION_BOUND
                }
                
                if (shouldStop) {
                    if (APPLICATION_MODE) targetValues.clear()
                    return
                }
            }
        }
    }


    fun beamValuesReversed(targetVariable: String, targetValues: HashSet<String>, ts: TripleSet) {
        val atomIndex = body.size() - 1
        val lastAtom = body.get(atomIndex)
        if (getGroundingsLastAtom(ts) < Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) return

        val unboundVariable = this.unboundVariable
        
        if (unboundVariable == null) {
            val nextVarIsLeft = !lastAtom.isLeftC
            val constant = lastAtom.getLR(!nextVarIsLeft)
            val nextVariable = lastAtom.getLR(nextVarIsLeft)

            var counter = 0
            while (counter <= Settings.SAMPLE_SIZE) {
                val value = ts.getRandomEntity(lastAtom.relation, constant, !nextVarIsLeft) ?: break
                counter++
                
                val previousValues = hashSetOf(constant, head.constant)
                val targetValue = beamForwardReversed(nextVariable, value, atomIndex - 1, targetVariable, ts, previousValues)
                targetValue?.let { targetValues.add(it) }
            }
        } else {
            val nextVarIsLeft = lastAtom.left != unboundVariable
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            
            var counter = 0
            while (counter <= Settings.SAMPLE_SIZE) {
                val t = ts.getRandomTripleByRelation(lastAtom.relation) ?: break
                counter++
                
                val value = t.getValue(nextVarIsLeft)
                val previousValues = hashSetOf(
                    t.getValue(!nextVarIsLeft),
                    head.constant
                )
                val targetValue = beamForwardReversed(nextVariable, value, atomIndex - 1, targetVariable, ts, previousValues)
                targetValue?.let { targetValues.add(it) }
            }
        }
    }


    private fun forwardReversed(
        variable: String,
        value: String,
        bodyIndex: Int,
        targetVariable: String,
        targetValues: HashSet<String>,
        ts: TripleSet,
        previousValues: HashSet<String>
    ) {
        if (previousValues.contains(value)) return
        
        if (bodyIndex < 0) {
            targetValues.add(value)
        } else {
            val currentValues = hashSetOf<String>().apply {
                add(value)
                addAll(previousValues)
            }
            
            if (!APPLICATION_MODE && targetValues.size >= Settings.SAMPLE_SIZE) return
            
            val atom = body.get(bodyIndex)
            val nextVarIsLeft = atom.left != variable
            val nextVariable = atom.getLR(nextVarIsLeft)
            val nextValues = ts.getEntities(atom.relation, value, !nextVarIsLeft)
            
            nextValues.forEach { nextValue ->
                forwardReversed(nextVariable, nextValue, bodyIndex - 1, targetVariable, targetValues, ts, currentValues)
            }
        }
    }

    private fun beamForwardReversed(
        variable: String,
        value: String,
        bodyIndex: Int,
        targetVariable: String,
        ts: TripleSet,
        previousValues: HashSet<String>
    ): String? {
        if (previousValues.contains(value)) return null

        return if (bodyIndex < 0) {
            value
        } else {
            previousValues.add(value)
            val atom = body.get(bodyIndex)
            val nextVarIsLeft = atom.left != variable
            val nextVariable = atom.getLR(nextVarIsLeft)
            val nextValue = ts.getRandomEntity(atom.relation, value, !nextVarIsLeft)
            
            nextValue?.let { 
                beamForwardReversed(nextVariable, it, bodyIndex - 1, targetVariable, ts, previousValues)
            }
        }
    }


    /*
   private void forwardPReversed(double p, String variable, String value, int bodyIndex, String targetVariable, PriorityQueue<Candidate> targetValues, TripleSet ts, HashSet<String> previousValues) {
       if (previousValues.contains(value)) return;
       if (bodyIndex < 0) {
           Candidate c = new Candidate(value, this.getAppliedConfidence() * p);
           targetValues.add(c);
       }
       else {
           HashSet<String> currentValues = new HashSet<String>();
           currentValues.add(value);
           currentValues.addAll(previousValues); // ADDING THIS SINGLE LINE WAS I SUPER IMPORTANT BUG FIX
           Atom atom = this.body.get(bodyIndex);
           boolean nextVarIsLeft = false;
           if (atom.getLeft().equals(variable)) nextVarIsLeft = false;
           else nextVarIsLeft = true;
           String nextVariable = atom.getLR(nextVarIsLeft);
           HashSet<String> nextValues = new HashSet<String>();			
           if (!Rule.APPLICATION_MODE && targetValues.size() >= Settings.SAMPLE_SIZE) return;
           nextValues.addAll(ts.getEntities(atom.getRelation(), value, !nextVarIsLeft));
           for (String nextValue : nextValues) {
               forwardPReversed(p, nextVariable, nextValue, bodyIndex-1, targetVariable, targetValues, ts, currentValues);
           }
       }
   }
   */
    protected abstract val unboundVariable: String?

    override fun isRefinable(): Boolean = false

    override fun getRandomValidPrediction(ts: TripleSet): Triple? {
        val validPredictions = getPredictions(ts, 1)
        return if (validPredictions.isEmpty()) null else validPredictions[rand.nextInt(validPredictions.size)]
    }

    override fun getRandomInvalidPrediction(ts: TripleSet): Triple? {
        val invalidPredictions = getPredictions(ts, -1)
        return if (invalidPredictions.isEmpty()) null else invalidPredictions[rand.nextInt(invalidPredictions.size)]
    }

    override fun getPredictions(ts: TripleSet): ArrayList<Triple> = getPredictions(ts, 0)

    /**
     * @param valid 1 = valid; -1 = invalid; 0 = valid/invalid does not matter
     */
    protected fun getPredictions(ts: TripleSet, valid: Int): ArrayList<Triple> {
        val materialized = arrayListOf<Triple>()
        val resultSet = if (isXRule) computeHeadResults(head.right, ts) else computeTailResults(head.left, ts)
        
        resultSet.forEach { v ->
            val t = if (isXRule) {
                Triple(v, targetRelation, head.right)
            } else {
                Triple(head.left, targetRelation, v)
            }
            
            when (valid) {
                1 -> if (ts.isTrue(t)) materialized.add(t)
                -1 -> if (!ts.isTrue(t)) materialized.add(t)
                else -> materialized.add(t)
            }
        }
        
        return materialized
    }

    abstract fun getGroundingsLastAtom(triples: TripleSet): Int

    /**
     * First replaces all atoms by deep copies of these atoms to avoid that references from the outside are affected by follow up changes.
     * Then corrects a rule which uses X in the head at the Y position by replacing X by Y in the head as well as all occurrences
     */
    fun detachAndPolish() {
        head = head.copy()
        body.detach()
        
        if (head.right == "X") {
            head.right = "Y"
            repeat(bodysize()) { i ->
                getBodyAtom(i).replace("X", "Y")
            }
        }
    }
}
