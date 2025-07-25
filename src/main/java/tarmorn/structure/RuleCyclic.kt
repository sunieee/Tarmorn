package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.SampledPairedResultSet
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.data.IdManager
import kotlin.math.pow

class RuleCyclic(r: RuleUntyped, appliedConfidence1: Double) : Rule(r) {
    init {
        // modify it to its canonical form
        if (this.body[0].contains(IdManager.getYId()) && this.bodysize() > 1) {
            // if (this.bodysize() > 3) println("before: " + this);
            for (i in 0..(this.bodysize() / 2) - 1) {
                val j = (this.bodysize() - i) - 1
                val atom_i = this.body[i]
                val atom_j = this.body[j]
                this.body[i] = atom_j
                this.body[j] = atom_i
            }
            this.body.normalizeVariableNames()
            // if (this.bodysize() > 3) println("after: " + this);
        }
    }


    override fun materialize(trainingSet: TripleSet): TripleSet? {
        //TripleSet materializedRule = new TripleSet();

        return null
    }

    override fun computeTailResults(head: Int, ts: TripleSet): HashSet<Int> {
        val results = HashSet<Int>()
        //if (Settings.BEAM_NOT_DFS) {
        //	results = this.beamPGBodyCyclic(IdManager.getXId(), IdManager.getYId(), head, 0, true, ts);
        //}
        //else {
        this.getCyclic(IdManager.getXId(), IdManager.getYId(), head, 0, true, ts, HashSet<Int>(), results)
        //}
        return results
    }

    override fun computeHeadResults(tail: Int, ts: TripleSet): HashSet<Int> {
        val results = HashSet<Int>()
        // if (Settings.BEAM_NOT_DFS) {
        //	results = this.beamPGBodyCyclic(IdManager.getYId(), IdManager.getXId(), tail, this.bodysize() - 1, false, ts);
        //}
        //else {
        this.getCyclic(IdManager.getYId(), IdManager.getXId(), tail, this.bodysize() - 1, false, ts, HashSet<Int>(), results)
        //}
        return results
    }

    override fun computeScores(triples: TripleSet) {
        // X is given in first body atom
        val xypairs: SampledPairedResultSet?
        val xypairsReverse: SampledPairedResultSet?

        if (this.body[0].contains(IdManager.getXId())) {
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


        var predictedAll = 0
        var correctlyPredictedAll = 0
        // body groundings for head prediction	
        var correctlyPredicted = 0
        var predicted = 0
        for (key in xypairsReverse.values.keys) {
            for (value in xypairsReverse.values.get(key)!!) {
                predicted++
                if (triples.isTrue(key, this.head.relation, value)) correctlyPredicted++
            }
        }

        predictedAll += predicted
        correctlyPredictedAll += correctlyPredicted

        correctlyPredicted = 0
        predicted = 0

        for (key in xypairs.values.keys) {
            for (value in xypairs.values.get(key)!!) {
                predicted++
                if (triples.isTrue(key, this.head.relation, value)) correctlyPredicted++
            }
        }


        predictedAll += predicted
        correctlyPredictedAll += correctlyPredicted



        this.predicted = predictedAll
        this.correctlyPredicted = correctlyPredictedAll
        this.confidence = this.correctlyPredicted.toDouble() / this.predicted.toDouble()
    }


    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        val scores = IntArray(2)
        if (this.targetRelation != that.targetRelation) {
            System.err.print("your are computing the scores of a concjuntion of two rules with different target relations, that does not make sense")
            return scores
        }
        val xypairs: SampledPairedResultSet?
        val xypairsReverse: SampledPairedResultSet?

        if (this.body[0].contains(IdManager.getXId())) {
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
                val explanation = that.getTripleExplanation(key, value, HashSet<Triple>(), triples)
                if (explanation != null && explanation.size > 0) {
                    predictedBoth++
                    if (triples.isTrue(key, this.head.relation, value)) correctlyPredictedBoth++
                }
            }
        }
        for (key in xypairsReverse.values.keys) {
            for (value in xypairsReverse.values.get(key)!!) {
                // change this to that
                val explanation = that.getTripleExplanation(key, value, HashSet<Triple>(), triples)
                if (explanation != null && explanation.size > 0) {
                    predictedBoth++
                    if (triples.isTrue(key, this.head.relation, value)) correctlyPredictedBoth++
                }
            }
        }



        scores[0] = predictedBoth
        scores[1] = correctlyPredictedBoth


        return scores
    }


    /*
	public int estimateAllBodyGroundings() {
		for (int i = 0; i < this.bodysize(); i++) {
			
			Atom atom = this.getBodyAtom(i);
			atom.get
			
			
			
			
		}
		
	}
	*/
    /**
     * The new implementation if the sample based computation of the scores.
     * Samples completely random attempts to create a beam over the body.
     *
     * @param triples
     */
    /*
	public void beamScores(TripleSet triples) {
		long startScoring = System.currentTimeMillis();
		// X is given in first body atom
		SampledPairedResultSet xypairs;
		if (this.body[0].contains(IdManager.getXId())) {
			xypairs = beamBodyCyclic(IdManager.getXId(), IdManager.getYId(), triples);
		}
		else {
			xypairs = beamBodyCyclic(IdManager.getYId(), IdManager.getXId(), triples);
		}
		// body groundings		
		int correctlyPredicted = 0;
		int predicted = 0;
		for (String key : xypairs.getValues().keySet()) {
			for (String value : xypairs.getValues().get(key)) {	
				if (Settings.PREDICT_ONLY_UNCONNECTED) {
					Set<String> links = triples.getRelations(key, value);
					Set<String> invLinks = triples.getRelations(value, key);
					if (invLinks.size() > 0) continue;
					if (!links.contains(this.head.getRelation()) && links.size() > 0) continue;
					if (links.contains(this.head.getRelation()) && links.size() > 1) continue;
				}
				predicted++;
				if (triples.isTrue(key, this.head.getRelation(), value)) {
					correctlyPredicted++;		
				}
			}
		}
		this.predicted = predicted;
		this.correctlyPredicted = correctlyPredicted;
		this.confidence = (double)correctlyPredicted / (double)predicted;
		
	}
	*/
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


    override fun getPredictions(triples: TripleSet): ArrayList<Triple> {
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
        if (this.body[0].contains(IdManager.getXId())) xypairs = groundBodyCyclic(IdManager.getXId(), IdManager.getYId(), triples)
        else xypairs = groundBodyCyclic(IdManager.getYId(), IdManager.getXId(), triples)
        val predictions = ArrayList<Triple>()
        for (key in xypairs.values.keys) {
            for (value in xypairs.values.get(key)!!) {
                if (valid == 1) {
                    if (triples.isTrue(key, this.head.relation, value)) {
                        val validPrediction = Triple(key, this.head.relation, value)
                        predictions.add(validPrediction)
                    }
                } else if (valid == -1) {
                    if (!triples.isTrue(key, this.head.relation, value)) {
                        val invalidPrediction = Triple(key, this.head.relation, value)
                        predictions.add(invalidPrediction)
                    }
                } else {
                    val validPrediction = Triple(key, this.head.relation, value)
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
        // check if the value has been seen before as grounding of another variable
        val atom = this.body.get(bodyIndex)
        val ifHead = atom.left == currentVariable
        if (previousValues.contains(value)) return

        // the current atom is the last
        if ((direction == true && this.body.size - 1 == bodyIndex) || (direction == false && bodyIndex == 0)) {
            // get groundings
            for (v in triples.getEntities(atom.relation, value, ifHead)) {
                if (!previousValues.contains(v) && value != v) finalResults.add(v)
            }
            return
        } else {
            val results = triples.getEntities(atom.relation, value, ifHead)
            if (results.size > Settings.BRANCHINGFACTOR_BOUND && Settings.DFS_SAMPLING_ON == true) return
            val nextVariable = if (ifHead) atom.right else atom.left
            val currentValues = HashSet<Int>()
            currentValues.addAll(previousValues)
            if (Settings.OI_CONSTRAINTS_ACTIVE) currentValues.add(value)

            // int i = 0;
            for (nextValue in results) {
                // XXX if (!Rule.APPLICATION_MODE && i >= Settings.SAMPLE_SIZE) break;
                val updatedBodyIndex = if (direction) bodyIndex + 1 else bodyIndex - 1
                this.getCyclic(
                    nextVariable,
                    lastVariable,
                    nextValue,
                    updatedBodyIndex,
                    direction,
                    triples,
                    currentValues,
                    finalResults
                )
                // i++;
            }
            return
        }
    }


    private fun groundBodyCyclic(
        firstVariable: Int,
        lastVariable: Int,
        triples: TripleSet,
        samplingOn: Boolean = Settings.DFS_SAMPLING_ON
    ): SampledPairedResultSet {
        val groundings = SampledPairedResultSet()
        val atom = this.body.get(0)
        val ifHead = atom.left == firstVariable
        val rtriples = triples.getTriplesByRelation(atom.relation)
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
        val atom = this.body.get(0)
        val ifHead = atom.left == firstVariable
        var t: Triple?
        var attempts = 0
        var repetitions = 0
        while ((triples.getRandomTripleByRelation(atom.relation).also { t = it }) != null) {
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
        val atom = this.body.get(0)
        val ifHead = atom.left == firstVariable
        var repetitions = 0
        val entities = triples.getNRandomEntitiesByRelation(
            atom.relation,
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
        val atom = this.body.last
        val ifHead = atom.left == lastVariable
        var t: Triple?
        var attempts = 0
        var repetitions = 0
        while ((triples.getRandomTripleByRelation(atom.relation).also { t = it }) != null) {
            attempts++
            val firstVarGrounding = this.beamCyclic(
                lastVariable,
                t!!.getValue(ifHead),
                this.bodysize() - 1,
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
        val atom = this.body.last
        val ifHead = atom.left == lastVariable
        var repetitions = 0
        val entities = triples.getNRandomEntitiesByRelation(
            atom.relation,
            ifHead,
            Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS
        )
        for (e in entities) {
            // println("e="+ e);
            val firstVarGrounding =
                this.beamCyclic(lastVariable, e, this.bodysize() - 1, false, triples, HashSet<Int>())
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
        // check if the value has been seen before as grounding of another variable
        val atom = this.body.get(bodyIndex)
        val ifHead = atom.left == currentVariable
        // OI-OFF
        if (previousValues.contains(value)) return null

        // the current atom is the last
        if ((direction == true && this.body.size - 1 == bodyIndex) || (direction == false && bodyIndex == 0)) {
            val finalValue = triples.getRandomEntity(atom.relation, value, ifHead)

            // println("Y = " + finalValue + " out of " + triples.getEntities(atom.getRelation(), value, ifHead).size());

            // OI-OFF
            if (previousValues.contains(finalValue)) return null
            // OI-OFF
            if (value == finalValue) return null
            return finalValue
        } else {
            val nextValue = triples.getRandomEntity(atom.relation, value, ifHead)
            val nextVariable = if (ifHead) atom.right else atom.left
            // OI-OFF
            if (Settings.OI_CONSTRAINTS_ACTIVE) previousValues.add(value)
            val updatedBodyIndex = if (direction) bodyIndex + 1 else bodyIndex - 1
            return this.beamCyclic(nextVariable, nextValue, updatedBodyIndex, direction, triples, previousValues)
        }
    }


    override fun isRefinable(): Boolean {
        return true
    }


    override var appliedConfidence: Double = 0.0
        get() {
            val cop = this.correctlyPredicted.toDouble()
            val pred = this.predicted.toDouble()
            val rsize = this.bodysize().toDouble()

            if (Settings.RULE_LENGTH_DEGRADE == 1.0) return cop / (pred + Settings.UNSEEN_NEGATIVE_EXAMPLES)
            else return (cop * Settings.RULE_LENGTH_DEGRADE.pow(rsize - 1.0)) / (pred + Settings.UNSEEN_NEGATIVE_EXAMPLES)
        }


    override fun isSingleton(triples: TripleSet): Boolean {
        return false
    }

    override fun getTripleExplanation(
        xValue: Int,
        yValue: Int,
        excludedTriples: HashSet<Triple>,
        triples: TripleSet
    ): HashSet<Triple> {
        val groundings = HashSet<Triple>()
        val bodyAtoms = ArrayList<Atom>()
        val variables = ArrayList<Int>()
        for (i in 0..<this.bodysize()) bodyAtoms.add(this.getBodyAtom(i))
        variables.add(IdManager.getXId())
        for (i in 0..<this.bodysize() - 1) variables.add(Rule.Companion.variables[i])
        variables.add(IdManager.getYId())
        val visitedValues = HashSet<Int>()
        visitedValues.add(xValue)
        visitedValues.add(yValue)
        searchTripleExplanation(
            xValue,
            yValue,
            0,
            this.bodysize() - 1,
            variables,
            excludedTriples,
            triples,
            groundings,
            visitedValues
        )
        return groundings
    }


    private fun searchTripleExplanation(
        firstValue: Int,
        lastValue: Int,
        firstIndex: Int,
        lastIndex: Int,
        variables: ArrayList<Int>,
        excludedTriples: HashSet<Triple>,
        triples: TripleSet,
        groundings: HashSet<Triple>,
        visitedValues: HashSet<Int>
    ) {
        val firstVar = variables.get(firstIndex)
        val lastVar = variables.get(lastIndex + 1)

        if (firstIndex == lastIndex) {
            val atom = this.getBodyAtom(firstIndex)
            if (atom.left == firstVar) {
                if (triples.isTrue(firstValue, atom.relation, lastValue)) {
                    val g = Triple(firstValue, atom.relation, lastValue)
                    if (!excludedTriples.contains(g)) {
                        groundings.add(g)
                        // println("Hit! ADDED " +  g + " and extended the groundings to " + groundings.size() + " triples");
                    }
                }
            } else {
                if (triples.isTrue(lastValue, atom.relation, firstValue)) {
                    val g = Triple(lastValue, atom.relation, firstValue)
                    if (!excludedTriples.contains(g)) {
                        groundings.add(g)
                        // println("Hit! ADDED " +  g + " and extended the groundings to " + groundings.size() + " triples");
                    }
                }
            }
            return
        }
        val firstAtom = this.getBodyAtom(firstIndex)
        val lastAtom = this.getBodyAtom(lastIndex)

        var valuesFromFirst: MutableSet<Int>? = null
        val firstValuesAreTails: Boolean
        if (firstAtom.left == firstVar) {
            valuesFromFirst = triples.getTailEntities(firstAtom.relation, firstValue)
            firstValuesAreTails = true
        } else {
            valuesFromFirst = triples.getHeadEntities(firstAtom.relation, firstValue)
            firstValuesAreTails = false
        }
        var valuesFromLast: MutableSet<Int>? = null
        val lastValuesAreTails: Boolean
        if (lastAtom.left == lastVar) {
            valuesFromLast = triples.getTailEntities(lastAtom.relation, lastValue)
            lastValuesAreTails = true
        } else {
            valuesFromLast = triples.getHeadEntities(lastAtom.relation, lastValue)
            lastValuesAreTails = false
        }
        if (valuesFromFirst.size < valuesFromLast.size) {
            for (value in valuesFromFirst) {
                val g: Triple?
                if (firstValuesAreTails) g = Triple(firstValue, firstAtom.relation, value)
                else g = Triple(value, firstAtom.relation, firstValue)
                if (excludedTriples.contains(g)) continue
                if (visitedValues.contains(value)) continue
                groundings.add(g)
                visitedValues.add(value)
                // println("add [" + firstIndex + ","  + lastIndex + "]" + g);
                searchTripleExplanation(
                    value,
                    lastValue,
                    firstIndex + 1,
                    lastIndex,
                    variables,
                    excludedTriples,
                    triples,
                    groundings,
                    visitedValues
                )
                if (groundings.size < this.bodysize()) {
                    // println("removing " + g + " (num of triples in groundings = " + groundings.size() + ")");
                    groundings.remove(g)
                } else break
            }
        } else {
            for (value in valuesFromLast) {
                val g: Triple?
                if (lastValuesAreTails) g = Triple(lastValue, lastAtom.relation, value)
                else g = Triple(value, lastAtom.relation, lastValue)
                if (excludedTriples.contains(g)) continue
                if (visitedValues.contains(value)) continue
                groundings.add(g)
                visitedValues.add(value)
                // println("add [" + firstIndex + ","  + lastIndex + "]" + g);
                searchTripleExplanation(
                    firstValue,
                    value,
                    firstIndex,
                    lastIndex - 1,
                    variables,
                    excludedTriples,
                    triples,
                    groundings,
                    visitedValues
                )
                if (groundings.size < this.bodysize()) {
                    groundings.remove(g)
                    // println("removing " + g + " (num of triples in groundings = " + groundings.size() + ")");
                } else break
            }
        }
    }
}
