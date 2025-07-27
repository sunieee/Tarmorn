package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.SampledPairedResultSet
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.data.IdManager
import kotlin.math.pow

class RuleCyclic(r: RuleUntyped, appliedConfidence1: Double) : Rule(r) {
    init {
        // For single atom body, no canonical form modification needed
        // The original logic was for multi-atom bodies which we no longer support
    }


    override fun materialize(trainingSet: TripleSet): TripleSet? {
        //TripleSet materializedRule = new TripleSet();

        return null
    }

    override fun computeTailResults(head: Int, ts: TripleSet): Set<Int> {
        val results = hashSetOf<Int>()
        //if (Settings.BEAM_NOT_DFS) {
        //	results = this.beamPGBodyCyclic(IdManager.getXId(), IdManager.getYId(), head, 0, true, ts);
        //}
        //else {
        this.getCyclic(IdManager.getXId(), IdManager.getYId(), head, 0, true, ts, HashSet<Int>(), results)
        //}
        return results
    }

    override fun computeHeadResults(tail: Int, ts: TripleSet): Set<Int> {
        val results = hashSetOf<Int>()
        // if (Settings.BEAM_NOT_DFS) {
        //	results = this.beamPGBodyCyclic(IdManager.getYId(), IdManager.getXId(), tail, 0, false, ts);
        //}
        //else {
        this.getCyclic(IdManager.getYId(), IdManager.getXId(), tail, 0, false, ts, HashSet<Int>(), results)
        //}
        return results
    }

    override fun computeScores(triples: TripleSet) {
        println("RuleCyclic.computeScores: Computing scores for rule: $this")
        
        // X is given in first body atom
        val xypairs: SampledPairedResultSet?
        val xypairsReverse: SampledPairedResultSet?

        if (this.body?.contains(IdManager.getXId()) == true) {
            println("RuleCyclic.computeScores: Body contains X variable")
            if (Settings.BEAM_NOT_DFS) {
                if (Settings.BEAM_TYPE_EDIS) {
                    xypairs = beamBodyCyclicEDIS(IdManager.getXId(), IdManager.getYId(), triples)
                    xypairsReverse = beamBodyCyclicReverseEDIS(IdManager.getXId(), IdManager.getYId(), triples)
                } else {
                    xypairs = beamBodyCyclic(IdManager.getXId(), IdManager.getYId(), triples)
                    xypairsReverse = beamBodyCyclicReverse(IdManager.getXId(), IdManager.getYId(), triples)
                }
            } else {
                xypairs = groundBodyCyclic(IdManager.getXId(), IdManager.getYId(), triples)
                xypairsReverse = SampledPairedResultSet()
            }
        } else {
            println("RuleCyclic.computeScores: Body does NOT contain X variable")
            if (Settings.BEAM_NOT_DFS) {
                if (Settings.BEAM_TYPE_EDIS) {
                    xypairs = beamBodyCyclicEDIS(IdManager.getYId(), IdManager.getXId(), triples)
                    xypairsReverse = beamBodyCyclicReverseEDIS(IdManager.getYId(), IdManager.getXId(), triples)
                } else {
                    xypairs = beamBodyCyclic(IdManager.getYId(), IdManager.getXId(), triples)
                    xypairsReverse = beamBodyCyclicReverse(IdManager.getYId(), IdManager.getXId(), triples)
                }
            } else {
                xypairs = groundBodyCyclic(IdManager.getYId(), IdManager.getXId(), triples)
                xypairsReverse = SampledPairedResultSet()
            }
        }

        println("RuleCyclic.computeScores: xypairs size = ${xypairs.size()}, xypairsReverse size = ${xypairsReverse.size()}")

        var predictedAll = 0
        var correctlyPredictedAll = 0
        // body groundings for head prediction	
        var correctlyPredicted = 0
        var predicted = 0
        for (key in xypairsReverse.values.keys) {
            for (value in xypairsReverse.values.get(key)!!) {
                predicted++
                if (triples.isTrue(key, this.head.r, value)) correctlyPredicted++
            }
        }

        predictedAll += predicted
        correctlyPredictedAll += correctlyPredicted

        correctlyPredicted = 0
        predicted = 0

        for (key in xypairs.values.keys) {
            for (value in xypairs.values.get(key)!!) {
                predicted++
                val isCorrect = triples.isTrue(key, this.head.r, value)
                println("RuleCyclic.computeScores: Checking prediction (${IdManager.getEntityString(key)}, ${IdManager.getRelationString(this.head.r)}, ${IdManager.getEntityString(value)}) = $isCorrect")
                if (isCorrect) correctlyPredicted++
            }
        }


        predictedAll += predicted
        correctlyPredictedAll += correctlyPredicted

        println("RuleCyclic.computeScores: Final scores - predictedAll=$predictedAll, correctlyPredictedAll=$correctlyPredictedAll")

        this.predicted = predictedAll
        this.correctlyPredicted = correctlyPredictedAll
        this.confidence = if (predictedAll > 0) this.correctlyPredicted.toDouble() / this.predicted.toDouble() else 0.0
        
        println("RuleCyclic.computeScores: Final rule scores - predicted=${this.predicted}, correctlyPredicted=${this.correctlyPredicted}, confidence=${this.confidence}")
    }


    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        val scores = IntArray(2)
        if (this.targetRelation != that.targetRelation) {
            System.err.print("your are computing the scores of a concjuntion of two rules with different target relations, that does not make sense")
            return scores
        }
        val xypairs: SampledPairedResultSet?
        val xypairsReverse: SampledPairedResultSet?

        if (this.body?.contains(IdManager.getXId()) == true) {
            if (Settings.BEAM_NOT_DFS) {
                if (Settings.BEAM_TYPE_EDIS) {
                    xypairs = beamBodyCyclicEDIS(IdManager.getXId(), IdManager.getYId(), triples)
                    xypairsReverse = beamBodyCyclicReverseEDIS(IdManager.getXId(), IdManager.getYId(), triples)
                } else {
                    xypairs = beamBodyCyclic(IdManager.getXId(), IdManager.getYId(), triples)
                    xypairsReverse = beamBodyCyclicReverse(IdManager.getXId(), IdManager.getYId(), triples)
                }
            } else {
                xypairs = groundBodyCyclic(IdManager.getXId(), IdManager.getYId(), triples)
                xypairsReverse = xypairs
            }
        } else {
            if (Settings.BEAM_NOT_DFS) {
                if (Settings.BEAM_TYPE_EDIS) {
                    xypairs = beamBodyCyclicEDIS(IdManager.getYId(), IdManager.getXId(), triples)
                    xypairsReverse = beamBodyCyclicReverseEDIS(IdManager.getYId(), IdManager.getXId(), triples)
                } else {
                    xypairs = beamBodyCyclic(IdManager.getYId(), IdManager.getXId(), triples)
                    xypairsReverse = beamBodyCyclicReverse(IdManager.getYId(), IdManager.getXId(), triples)
                }
            } else {
                xypairs = groundBodyCyclic(IdManager.getYId(), IdManager.getXId(), triples)
                xypairsReverse = xypairs
            }
        }


        var predictedBoth = 0
        var correctlyPredictedBoth = 0


        for (key in xypairs.values.keys) {
            for (value in xypairs.values.get(key)!!) {
                val explanation = that.getTripleExplanation(key, value, hashSetOf(), triples)
                if (explanation != null && explanation.size > 0) {
                    predictedBoth++
                    if (triples.isTrue(key, this.head.r, value)) correctlyPredictedBoth++
                }
            }
        }
        for (key in xypairsReverse.values.keys) {
            for (value in xypairsReverse.values.get(key)!!) {
                // change this to that
                val explanation = that.getTripleExplanation(key, value, hashSetOf(), triples)
                if (explanation != null && explanation.size > 0) {
                    predictedBoth++
                    if (triples.isTrue(key, this.head.r, value)) correctlyPredictedBoth++
                }
            }
        }



        scores[0] = predictedBoth
        scores[1] = correctlyPredictedBoth


        return scores
    }

    override fun getRandomValidPrediction(triples: TripleSet): Triple? {
        val validPredictions = this.getPredictions(triples, 1)
        if (validPredictions == null || validPredictions.size == 0) return null
        if (validPredictions.size == 0) return null
        val index: Int = Rule.Companion.rand.nextInt(validPredictions.size)
        return validPredictions.get(index)
    }

    override fun getRandomInvalidPrediction(triples: TripleSet): Triple? {
        val validPredictions = this.getPredictions(triples, -1)
        if (validPredictions == null || validPredictions.size == 0) return null
        if (validPredictions.size == 0) return null
        val index: Int = Rule.Companion.rand.nextInt(validPredictions.size)
        return validPredictions.get(index)
    }


    override fun getPredictions(triples: TripleSet): List<Triple> {
        return this.getPredictions(triples, 0)
    }


    /**
     *
     * @param triples
     * @param valid 1= must be valid; -1 = must be invalid; 0 = valid and invalid is okay
     * @return
     */
    protected fun getPredictions(triples: TripleSet, valid: Int): ArrayList<Triple> {
        val xypairs: SampledPairedResultSet?
        if (this.body?.contains(IdManager.getXId()) == true) xypairs = groundBodyCyclic(IdManager.getXId(), IdManager.getYId(), triples)
        else xypairs = groundBodyCyclic(IdManager.getYId(), IdManager.getXId(), triples)
        val predictions = ArrayList<Triple>()
        for (key in xypairs.values.keys) {
            for (value in xypairs.values.get(key)!!) {
                if (valid == 1) {
                    if (triples.isTrue(key, this.head.r, value)) {
                        val validPrediction = Triple(key, this.head.r, value)
                        predictions.add(validPrediction)
                    }
                } else if (valid == -1) {
                    if (!triples.isTrue(key, this.head.r, value)) {
                        val invalidPrediction = Triple(key, this.head.r, value)
                        predictions.add(invalidPrediction)
                    }
                } else {
                    val validPrediction = Triple(key, this.head.r, value)
                    predictions.add(validPrediction)
                }
            }
        }
        return predictions
    }


    override fun isPredictedX(leftValue: Int, rightValue: Int, forbidden: Triple?, ts: TripleSet): Boolean {
        System.err.println("method not YET available for an extended/refinde rule")
        return false
    }


    // *** PRIVATE PLAYGROUND **** 
    private fun getCyclic(
        currentVariable: Int,
        lastVariable: Int,
        value: Int,
        bodyIndex: Int,
        direction: Boolean,
        triples: TripleSet,
        previousValues: HashSet<Int>,
        finalResults: HashSet<Int>
    ) {
        if (Rule.Companion.APPLICATION_MODE && finalResults.size >= Settings.DISCRIMINATION_BOUND) {
            finalResults.clear()
            return
        }
        // XXX if (!Rule.APPLICATION_MODE && finalResults.size() >= Settings.SAMPLE_SIZE) return;
        
        if (body == null) return
        val atom = body!!
        
        // Check if the value has been seen before as grounding of another variable
        if (previousValues.contains(value)) return
        
        println("RuleCyclic.getCyclic: Processing currentVariable=$currentVariable, lastVariable=$lastVariable, value=$value, bodyIndex=$bodyIndex")
        println("RuleCyclic.getCyclic: Atom = $atom")
        
        // For single atom body, we need to handle the case where we process the only atom
        val ifHead = atom.h == currentVariable
        println("RuleCyclic.getCyclic: ifHead=$ifHead (currentVariable is head of atom)")
        
        // Get all possible groundings for this atom
        val groundings = triples.getEntities(atom.r, value, ifHead)
        println("RuleCyclic.getCyclic: Found ${groundings.size} groundings for relation ${atom.r} with value $value")
        
        for (v in groundings) {
            println("RuleCyclic.getCyclic: Checking grounding value $v")
            if (!previousValues.contains(v) && value != v) {
                // Check if this grounding leads to the target variable
                val nextVariable = if (ifHead) atom.t else atom.h
                println("RuleCyclic.getCyclic: Next variable would be $nextVariable, target is $lastVariable")
                
                if (nextVariable == lastVariable) {
                    // We found a complete path to the target variable
                    println("RuleCyclic.getCyclic: Found complete path, adding result $v")
                    finalResults.add(v)
                } else {
                    // Continue search if we haven't reached the target variable
                    val newPreviousValues = HashSet(previousValues)
                    newPreviousValues.add(value)
                    println("RuleCyclic.getCyclic: Continuing search with next variable $nextVariable")
                    getCyclic(nextVariable, lastVariable, v, bodyIndex + 1, direction, triples, newPreviousValues, finalResults)
                }
            }
        }
    }


    private fun groundBodyCyclic(
        firstVariable: Int,
        lastVariable: Int,
        triples: TripleSet,
        samplingOn: Boolean = Settings.DFS_SAMPLING_ON
    ): SampledPairedResultSet {
        val groundings = SampledPairedResultSet()
        if (body == null) return groundings
        
        val atom = body!!
        val ifHead = atom.h == firstVariable
        val rtriples = triples.getTriplesByRelation(atom.r)
        var counter = 0
        for (t in rtriples) {
            counter++
            val lastVariableGroundings = HashSet<Int>()
            // the call itself
            this.getCyclic(
                firstVariable,
                lastVariable,
                t.getValue(ifHead),
                0,
                true,
                triples,
                HashSet<Int>(),
                lastVariableGroundings
            )
            if (lastVariableGroundings.size > 0) {
                if (firstVariable == IdManager.getXId()) {
                    groundings.addKey(t.getValue(ifHead))
                    for (lastVariableValue in lastVariableGroundings) {
                        groundings.addValue(lastVariableValue)
                    }
                } else {
                    for (lastVariableValue in lastVariableGroundings) {
                        groundings.addKey(lastVariableValue)
                        groundings.addValue(t.getValue(ifHead))
                    }
                }
            }
            if ((counter > Settings.SAMPLE_SIZE || groundings.size() > Settings.SAMPLE_SIZE) && samplingOn) {
                break
            }
        }
        return groundings
    }

    private fun beamBodyCyclic(
        firstVariable: Int,
        lastVariable: Int,
        triples: TripleSet
    ): SampledPairedResultSet {
        val groundings = SampledPairedResultSet()
        if (body == null) return groundings
        
        val atom = body!!
        val ifHead = atom.h == firstVariable
        var t: Triple?
        var attempts = 0
        var repetitions = 0
        while ((triples.getRandomTripleByRelation(atom.r).also { t = it }) != null) {
            attempts++
            val lastVarGrounding =
                this.beamCyclic(firstVariable, t!!.getValue(ifHead), 0, true, triples, HashSet<Int>())
            if (lastVarGrounding != null) {
                if (firstVariable == IdManager.getXId()) {
                    groundings.addKey(t.getValue(ifHead))
                    if (groundings.addValue(lastVarGrounding)) repetitions = 0
                    else repetitions++
                } else {
                    groundings.addKey(lastVarGrounding)
                    if (groundings.addValue(t.getValue(ifHead))) repetitions = 0
                    else repetitions++
                }
            }
            if (Settings.BEAM_SAMPLING_MAX_REPETITIONS <= repetitions) break
            if (Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS <= attempts) break
            if (Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS <= groundings.size()) break
        }
        return groundings
    }


    private fun beamBodyCyclicEDIS(
        firstVariable: Int,
        lastVariable: Int,
        triples: TripleSet
    ): SampledPairedResultSet {
        val groundings = SampledPairedResultSet()
        if (body == null) return groundings
        
        val atom = body!!
        val ifHead = atom.h == firstVariable
        var repetitions = 0
        val entities = triples.getNRandomEntitiesByRelation(
            atom.r,
            ifHead,
            Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS
        )
        for (e in entities) {
            val lastVarGrounding = this.beamCyclic(firstVariable, e, 0, true, triples, HashSet<Int>())
            if (lastVarGrounding != null) {
                if (firstVariable == IdManager.getXId()) {
                    groundings.addKey(e)
                    if (groundings.addValue(lastVarGrounding)) repetitions = 0
                    else repetitions++
                } else {
                    groundings.addKey(lastVarGrounding)
                    if (groundings.addValue(e)) repetitions = 0
                    else repetitions++
                }
            }
            if (Settings.BEAM_SAMPLING_MAX_REPETITIONS <= repetitions) break
            if (Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS <= groundings.size()) break
        }
        groundings.chaoEstimate = repetitions
        return groundings
    }

    // see http://www.vldb.org/conf/1995/P311.PDF
    fun getChaoEstimate(f1: Int, f2: Int, d: Int): Int {
        return (d + ((f1 * f1).toDouble() / (2.0 * f2))).toInt()
    }


    private fun beamBodyCyclicReverse(
        firstVariable: Int,
        lastVariable: Int,
        triples: TripleSet
    ): SampledPairedResultSet {
        val groundings = SampledPairedResultSet()
        if (body == null) return groundings
        
        val atom = body!! // For single atom body, this is the only atom
        val ifHead = atom.h == lastVariable
        var t: Triple?
        var attempts = 0
        var repetitions = 0
        while ((triples.getRandomTripleByRelation(atom.r).also { t = it }) != null) {
            attempts++
            val firstVarGrounding = this.beamCyclic(
                lastVariable,
                t!!.getValue(ifHead),
                0, // For single atom body, bodyIndex is always 0
                false,
                triples,
                HashSet<Int>()
            )
            // until here
            if (firstVarGrounding != null) {
                if (firstVariable == IdManager.getXId()) {
                    groundings.addKey(firstVarGrounding)
                    if (groundings.addValue(t.getValue(ifHead))) repetitions = 0
                    else repetitions++
                } else {
                    groundings.addKey(t.getValue(ifHead))
                    if (groundings.addValue(firstVarGrounding)) repetitions = 0
                    else repetitions++
                }
            }
            if (Settings.BEAM_SAMPLING_MAX_REPETITIONS <= repetitions) break
            if (Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS <= attempts) break
            if (Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS <= groundings.size()) break
        }
        return groundings
    }

    private fun beamBodyCyclicReverseEDIS(
        firstVariable: Int,
        lastVariable: Int,
        triples: TripleSet
    ): SampledPairedResultSet {
        val groundings = SampledPairedResultSet()
        if (body == null) return groundings
        
        val atom = body!! // For single atom body, this is the only atom
        val ifHead = atom.h == lastVariable
        var repetitions = 0
        val entities = triples.getNRandomEntitiesByRelation(
            atom.r,
            ifHead,
            Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS
        )
        for (e in entities) {
            // println("e="+ e);
            val firstVarGrounding =
                this.beamCyclic(lastVariable, e, 0, false, triples, HashSet<Int>()) // bodyIndex is 0 for single atom
            if (firstVarGrounding != null) {
                if (firstVariable == IdManager.getXId()) {
                    groundings.addKey(firstVarGrounding)
                    if (groundings.addValue(e)) repetitions = 0
                    else repetitions++
                } else {
                    groundings.addKey(e)
                    if (groundings.addValue(firstVarGrounding)) repetitions = 0
                    else repetitions++
                }
            }
            if (Settings.BEAM_SAMPLING_MAX_REPETITIONS <= repetitions) break
            if (Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS <= groundings.size()) break
        }
        return groundings
    }


    // (String currentVariable, String lastVariable, TripleSet triples,) {
    /*
	private HashSet<Int> beamPGBodyCyclic(String firstVariable, String lastVariable, String value, int bodyIndex, boolean direction, TripleSet triples) {
		HashSet<Int> groundings = new HashSet<Int>();
		Atom atom = this.body.get(bodyIndex);
		boolean ifHead = atom.getLeft().equals(firstVariable);
		int attempts = 0;
		int repetitions = 0;
		// println("startsFine: " + atom.getRelation() + " - " + value + " - " + ifHead);
		boolean startFine = !triples.getEntities(atom.getRelation(), value, ifHead).isEmpty();
		//println("startsFine=" + startFine);
		while (startFine) {
			attempts++;
			String grounding = this.beamCyclic(firstVariable, value, bodyIndex, direction, triples, new HashSet<Int>());
			if (grounding != null) {
				if (groundings.add(grounding)) repetitions = 0;
				else repetitions++;
			}
			if (Settings.BEAM_SAMPLING_MAX_REPETITIONS <= repetitions) break;
			if (Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS <= attempts) break;
			if (Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS <= groundings.size()) break;
		}
		// println(this);
		// println("  => r=" + repetitions + " a=" + attempts + " g=" + groundings.size());
		// println(Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS);
		// println(Settings.BEAM_SAMPLING_MAX_BODY_GROUNDINGS);
		
		return groundings;
	}
	*/
    /**
     * Tries to create a random grounding for a partially grounded body.
     *
     * @param currentVariable The name of the current variable for which a value is given.
     * @param value The value of the current variable.
     * @param bodyIndex The index of the body atom that we are currently concerned with.
     * @param direction The direction to search. True means to search from first to last atom, false the opposite direction.
     * @param triples The data set used for grounding the body
     * @param previousValues The values that were used as groundings for variables visited already.
     * @return A grounding for the last variable (or constants). Null if not a full grounding of the body has been constructed.
     */
    protected fun beamCyclic(
        currentVariable: Int,
        value: Int?,
        bodyIndex: Int,
        direction: Boolean,
        triples: TripleSet,
        previousValues: HashSet<Int>
    ): Int? {
        // println(currentVariable + ", " + value + ", " + bodyIndex +", " +direction + ", " + previousValues.size());
        if (value == null || value == 0) return null
        if (body == null) return null
        
        // check if the value has been seen before as grounding of another variable
        val atom = body!!
        val ifHead = atom.h == currentVariable
        // OI-OFF
        if (previousValues.contains(value)) return null

        // For single atom body, this is always the last (and only) atom
        if (bodyIndex == 0) {
            val finalValue = triples.getRandomEntity(atom.r, value, ifHead)

            // println("Y = " + finalValue + " out of " + triples.getEntities(atom.getRelation(), value, ifHead).size());

            // OI-OFF
            if (previousValues.contains(finalValue)) return null
            // OI-OFF
            if (value == finalValue) return null
            return finalValue
        } else {
            // This shouldn't happen with single atom body
            return null
        }
    }


    override fun isRefinable(): Boolean {
        return true
    }


    override var appliedConfidence: Double = 0.0
        get() {
            val cop = this.correctlyPredicted.toDouble()
            val pred = this.predicted.toDouble()
            val rsize = this.bodySize.toDouble()

            if (Settings.RULE_LENGTH_DEGRADE == 1.0) return cop / (pred + Settings.UNSEEN_NEGATIVE_EXAMPLES)
            else return (cop * Settings.RULE_LENGTH_DEGRADE.pow(rsize - 1.0)) / (pred + Settings.UNSEEN_NEGATIVE_EXAMPLES)
        }


    override fun isSingleton(triples: TripleSet): Boolean {
        return false
    }

    override fun getTripleExplanation(
        xValue: Int,
        yValue: Int,
        excludedTriples: Set<Triple>,
        triples: TripleSet
    ): Set<Triple> {
        val groundings = hashSetOf<Triple>()
        
        // For single atom body, the explanation is straightforward
        if (body == null) return groundings
        
        val atom = body!!
        
        // Check if the body atom connects X and Y values according to the rule
        val relevantTriples = when {
            atom.h == IdManager.getXId() && atom.t == IdManager.getYId() -> {
                // Body is relation(X,Y) - check if relation(xValue, yValue) exists
                if (triples.isTrue(xValue, atom.r, yValue)) {
                    setOf(Triple(xValue, atom.r, yValue))
                } else {
                    emptySet()
                }
            }
            atom.h == IdManager.getYId() && atom.t == IdManager.getXId() -> {
                // Body is relation(Y,X) - check if relation(yValue, xValue) exists  
                if (triples.isTrue(yValue, atom.r, xValue)) {
                    setOf(Triple(yValue, atom.r, xValue))
                } else {
                    emptySet()
                }
            }
            else -> {
                // More complex cases with constants - simplified implementation
                emptySet()
            }
        }
        
        relevantTriples.filterNot { excludedTriples.contains(it) }.forEach { groundings.add(it) }
        return groundings
    }
}
