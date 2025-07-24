package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.Triple
import tarmorn.data.TripleSet

abstract class RuleAcyclic(r: RuleUntyped) : Rule(r) {
    override fun computeTailResults(head: String, ts: TripleSet): HashSet<String> {
        val resultSet = HashSet<String>()
        if (this.isXRule) {
            if (this.head.right == head) return resultSet
            val previousValues = HashSet<String>()
            previousValues.add(head)
            previousValues.add(this.head.right)
            if (this.isBodyTrueAcyclic("X", head, 0, previousValues, ts)) {
                resultSet.add(this.head.right)
                return resultSet
            }
        } else {
            if (this.head.left == head) {
                this.computeValuesReversed("Y", resultSet, ts)
                return resultSet
            }
        }
        return resultSet
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
        val resultSet = HashSet<String>()
        if (this.isYRule) {
            if (this.head.left == tail) return resultSet
            val previousValues = HashSet<String>()
            previousValues.add(tail)
            previousValues.add(this.head.left)
            if (this.isBodyTrueAcyclic("Y", tail, 0, previousValues, ts)) {
                resultSet.add(this.head.left)
                return resultSet
            }
        } else if (this.isXRule) {
            if (this.head.right == tail) {
                this.computeValuesReversed("X", resultSet, ts)
                return resultSet
            }
        }
        return resultSet
    }


    override fun computeScores(triples: TripleSet) {
        if (this.isXRule) {
            val xvalues = HashSet<String>()
            // if (Settings.BEAM_NOT_DFS) this.beamValuesReversed("X", xvalues, triples);
            //else {
            this.computeValuesReversed("X", xvalues, triples)
            // }
            var predicted = 0
            var correctlyPredicted = 0
            for (xvalue in xvalues) {
                predicted++
                if (triples.isTrue(xvalue, this.head.relation, this.head.right)) correctlyPredicted++
            }
            this.predicted = predicted
            this.correctlyPredicted = correctlyPredicted
            this.confidence = correctlyPredicted.toDouble() / predicted.toDouble()
        } else {
            val yvalues = HashSet<String>()


            // if (Settings.BEAM_NOT_DFS) this.beamValuesReversed("Y", yvalues, triples);
            // else {
            this.computeValuesReversed("Y", yvalues, triples)

            // }
            var predicted = 0
            var correctlyPredicted = 0
            for (yvalue in yvalues) {
                predicted++
                if (triples.isTrue(this.head.left, this.head.relation, yvalue)) correctlyPredicted++
            }
            this.predicted = predicted
            this.correctlyPredicted = correctlyPredicted
            this.confidence = correctlyPredicted.toDouble() / predicted.toDouble()
        }
    }


    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        val scores = IntArray(2)
        var predictedBoth = 0
        var correctlyPredictedBoth = 0
        if (this.isXRule) {
            val xvalues = HashSet<String>()
            val yvalue = this.head.right
            this.computeValuesReversed("X", xvalues, triples)
            for (xvalue in xvalues) {
                // println("... checking " + xvalue + " ~relation~ " + yvalue);
                val explanation = that.getTripleExplanation(xvalue, yvalue, HashSet<Triple>(), triples)
                if (explanation != null && explanation.size > 0) {
                    predictedBoth++
                    if (triples.isTrue(xvalue, this.head.relation, yvalue)) correctlyPredictedBoth++
                }
            }
        } else {
            val yvalues = HashSet<String>()
            val xvalue = this.head.left
            this.computeValuesReversed("Y", yvalues, triples)
            for (yvalue in yvalues) {
                // System.out.print("... checking " + xvalue + " ~relation~ " + yvalue);
                val explanation = that.getTripleExplanation(xvalue, yvalue, HashSet<Triple>(), triples)
                if (explanation != null && explanation.size > 0) {
                    predictedBoth++
                    // println("... BOTH");
                    if (triples.isTrue(xvalue, this.head.relation, yvalue)) correctlyPredictedBoth++
                } else {
                    // println("... not by both");
                }
            }
        }
        scores[0] = predictedBoth
        scores[1] = correctlyPredictedBoth
        return scores
    }


    // the head is not used here (its only about using the body as extension
    override fun isPredictedX(leftValue: String, rightValue: String, forbidden: Triple?, ts: TripleSet): Boolean {
        if (forbidden == null) {
            if (this.isXRule) {
                val previousValues = HashSet<String>()
                previousValues.add(leftValue)
                return this.isBodyTrueAcyclic("X", leftValue, 0, previousValues, ts)
            } else {
                val previousValues = HashSet<String>()
                previousValues.add(rightValue)
                return this.isBodyTrueAcyclic("Y", rightValue, 0, previousValues, ts)
            }
        } else {
            if (this.isXRule) {
                val previousValues = HashSet<String>()
                previousValues.add(leftValue)
                return this.isBodyTrueAcyclicX("X", leftValue, 0, forbidden, previousValues, ts)
            } else {
                val previousValues = HashSet<String>()
                previousValues.add(rightValue)
                return this.isBodyTrueAcyclicX("Y", rightValue, 0, forbidden, previousValues, ts)
            }
        }
    }


    // *** PRIVATE PLAYGROUND **** 
    protected fun isBodyTrueAcyclic(
        variable: String,
        value: String,
        bodyIndex: Int,
        previousValues: HashSet<String>,
        triples: TripleSet
    ): Boolean {
        val atom = this.body.get(bodyIndex)
        val ifHead = atom.left == variable
        // the current atom is the last
        if (this.body.size() - 1 == bodyIndex) {
            val constant = if (ifHead) atom.isRightC else atom.isLeftC
            // get groundings
            // fixed by a constant
            if (constant) {
                val constantValue = if (ifHead) atom.right else atom.left
                if (previousValues.contains(constantValue) && constantValue != this.head.constant) return false
                if (ifHead) {
                    return triples.isTrue(value, atom.relation, constantValue)
                } else {
                    return triples.isTrue(constantValue, atom.relation, value)
                }
            } else {
                val results = triples.getEntities(atom.relation, value, ifHead)
                for (r in results) {
                    if (!previousValues.contains(r)) return true
                }
            }
            return false
        } else {
            val results = triples.getEntities(atom.relation, value, ifHead)
            val nextVariable = if (ifHead) atom.right else atom.left
            for (nextValue in results) {
                if (previousValues.contains(nextValue)) continue
                previousValues.add(nextValue)
                if (isBodyTrueAcyclic(nextVariable, nextValue, bodyIndex + 1, previousValues, triples)) {
                    return true
                }
                previousValues.remove(nextValue)
            }
            return false
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
        val atom = this.body.get(bodyIndex)
        val ifHead = atom.left == variable
        // the current atom is the last
        if (this.body.size() - 1 == bodyIndex) {
            val constant = if (ifHead) atom.isRightC else atom.isLeftC
            // get groundings
            // fixed by a constant
            if (constant) {
                val constantValue = if (ifHead) atom.right else atom.left
                if (previousValues.contains(constantValue) && constantValue != this.head.constant) return false
                if (ifHead) {
                    return triples.isTrue(value, atom.relation, constantValue)
                } else {
                    return triples.isTrue(constantValue, atom.relation, value)
                }
            } else {
                val results = triples.getEntities(atom.relation, value, ifHead)
                for (r in results) {
                    if (!previousValues.contains(r)) return true
                }
            }
            return false
        } else {
            val results = triples.getEntities(atom.relation, value, ifHead)
            val nextVariable = if (ifHead) atom.right else atom.left
            for (nextValue in results) {
                if (!forbidden.equals(ifHead, value, atom.relation, nextValue)) {
                    if (previousValues.contains(nextValue)) continue
                    previousValues.add(nextValue)
                    if (isBodyTrueAcyclicX(
                            nextVariable,
                            nextValue,
                            bodyIndex + 1,
                            forbidden,
                            previousValues,
                            triples
                        )
                    ) {
                        return true
                    }
                    previousValues.remove(nextValue)
                }
            }
            return false
        }
    }


    fun computeValuesReversed(targetVariable: String, targetValues: HashSet<String>, ts: TripleSet) {
        val atomIndex = this.body.size() - 1
        val lastAtom = this.body.get(atomIndex)
        val unboundVariable = this.unboundVariable
        if (unboundVariable == null) {
            val nextVarIsLeft: Boolean
            if (lastAtom.isLeftC) nextVarIsLeft = false
            else nextVarIsLeft = true
            val constant = lastAtom.getLR(!nextVarIsLeft)
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            val values = ts.getEntities(lastAtom.relation, constant, !nextVarIsLeft)
            val previousValues = HashSet<String>()
            previousValues.add(constant)
            previousValues.add(this.head.constant)
            var counter = 0
            for (value in values) {
                counter++
                forwardReversed(nextVariable, value, atomIndex - 1, targetVariable, targetValues, ts, previousValues)
                if (!Rule.Companion.APPLICATION_MODE && (targetValues.size >= Settings.SAMPLE_SIZE || counter >= Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS)) return
                if (Rule.Companion.APPLICATION_MODE && targetValues.size >= Settings.DISCRIMINATION_BOUND) {
                    targetValues.clear()
                    return
                }
            }
        } else {
            val nextVarIsLeft: Boolean
            if (lastAtom.left == unboundVariable) nextVarIsLeft = false
            else nextVarIsLeft = true
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            val triples = ts.getTriplesByRelation(lastAtom.relation)
            var counter = 0
            for (t in triples) {
                counter++
                val value = t.getValue(nextVarIsLeft)
                val previousValues = HashSet<String>()
                val previousValue = t.getValue(!nextVarIsLeft)
                previousValues.add(previousValue)
                previousValues.add(this.head.constant)
                forwardReversed(nextVariable, value, atomIndex - 1, targetVariable, targetValues, ts, previousValues)
                if (!Rule.Companion.APPLICATION_MODE && (targetValues.size >= Settings.SAMPLE_SIZE || counter >= Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS)) return
                if (Rule.Companion.APPLICATION_MODE && targetValues.size >= Settings.DISCRIMINATION_BOUND) {
                    targetValues.clear()
                    return
                }
            }
        }
    }


    fun beamValuesReversed(targetVariable: String, targetValues: HashSet<String>, ts: TripleSet) {
        val atomIndex = this.body.size() - 1
        val lastAtom = this.body.get(atomIndex)
        if (this.getGroundingsLastAtom(ts) < Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) return

        val unboundVariable = this.unboundVariable
        if (unboundVariable == null) {
            val nextVarIsLeft: Boolean
            if (lastAtom.isLeftC) nextVarIsLeft = false
            else nextVarIsLeft = true
            val constant = lastAtom.getLR(!nextVarIsLeft)
            val nextVariable = lastAtom.getLR(nextVarIsLeft)

            var value: String?
            var counter = 0
            while ((ts.getRandomEntity(lastAtom.relation, constant, !nextVarIsLeft).also { value = it }) != null) {
                counter++
                val previousValues = HashSet<String>()
                previousValues.add(constant)
                previousValues.add(this.head.constant)

                val targetValue =
                    beamForwardReversed(nextVariable, value!!, atomIndex - 1, targetVariable, ts, previousValues)
                if (targetValue != null) targetValues.add(targetValue)
                if (counter > Settings.SAMPLE_SIZE) return
            }
        } else {
            val nextVarIsLeft: Boolean
            if (lastAtom.left == unboundVariable) nextVarIsLeft = false
            else nextVarIsLeft = true
            val nextVariable = lastAtom.getLR(nextVarIsLeft)
            var t: Triple?
            var counter = 0
            while ((ts.getRandomTripleByRelation(lastAtom.relation).also { t = it }) != null) {
                counter++
                val value = t!!.getValue(nextVarIsLeft)
                val previousValues = HashSet<String>()
                val previousValue = t.getValue(!nextVarIsLeft)
                previousValues.add(previousValue)
                previousValues.add(this.head.constant)
                val targetValue =
                    beamForwardReversed(nextVariable, value, atomIndex - 1, targetVariable, ts, previousValues)
                if (targetValue != null) targetValues.add(targetValue)
                if (counter > Settings.SAMPLE_SIZE) return
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
            targetValues.add(value!!)
        } else {
            val currentValues = HashSet<String>()
            currentValues.add(value)
            currentValues.addAll(previousValues) // ADDING THIS SINGLE LINE WAS I SUPER IMPORTANT BUG FIX
            val atom = this.body.get(bodyIndex)
            var nextVarIsLeft = false
            if (atom.left == variable) nextVarIsLeft = false
            else nextVarIsLeft = true
            val nextVariable = atom.getLR(nextVarIsLeft)
            val nextValues = HashSet<String>()
            if (!Rule.Companion.APPLICATION_MODE && targetValues.size >= Settings.SAMPLE_SIZE) return
            nextValues.addAll(ts.getEntities(atom.relation, value, !nextVarIsLeft))
            for (nextValue in nextValues) {
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

        if (bodyIndex < 0) return value
        else {
            previousValues.add(value)
            val atom = this.body.get(bodyIndex)
            var nextVarIsLeft = false
            if (atom.left == variable) nextVarIsLeft = false
            else nextVarIsLeft = true
            val nextVariable = atom.getLR(nextVarIsLeft)
            val nextValue = ts.getRandomEntity(atom.relation, value, !nextVarIsLeft)
            if (nextValue != null) {
                return beamForwardReversed(nextVariable, nextValue, bodyIndex - 1, targetVariable, ts, previousValues)
            } else {
                return null
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

    override fun isRefinable(): Boolean {
        return false
    }

    override fun getRandomValidPrediction(ts: TripleSet): Triple? {
        val validPredictions = this.getPredictions(ts, 1)
        if (validPredictions == null || validPredictions.size == 0) return null
        val index: Int = Rule.Companion.rand.nextInt(validPredictions.size)
        return validPredictions.get(index)
    }

    override fun getRandomInvalidPrediction(ts: TripleSet): Triple? {
        val validPredictions = this.getPredictions(ts, -1)
        if (validPredictions == null || validPredictions.size == 0) return null
        val index: Int = Rule.Companion.rand.nextInt(validPredictions.size)
        return validPredictions.get(index)
    }

    override fun getPredictions(ts: TripleSet): ArrayList<Triple> {
        return this.getPredictions(ts, 0)
    }

    /**
     *
     * @param ts
     * @param valid 1 = valid; -1 = invalid; 0 valid/invalid does not matter
     * @return
     */
    protected fun getPredictions(ts: TripleSet, valid: Int): ArrayList<Triple> {
        val materialized = ArrayList<Triple>()
        var resultSet = if (this.isXRule)  this.computeHeadResults(this.head.right, ts)
        else this.computeTailResults(this.head.left, ts)
        for (v in resultSet) {
            val t: Triple?
            if (this.isXRule) {
                t = Triple(v, this.targetRelation, this.head.right)
            } else {
                t = Triple(this.head.left, this.targetRelation, v)
            }
            if (valid == 1) {
                if (ts.isTrue(t)) materialized.add(t)
            } else if (valid == -1) {
                if (!ts.isTrue(t)) materialized.add(t)
            } else {
                materialized.add(t)
            }


            // println(t + " due to: " +  this);
        }
        return materialized
    }

    abstract fun getGroundingsLastAtom(triples: TripleSet): Int

    /**
     * First replaces all atoms by deep copies of these atoms to avoid that references from the outside are affected by follow up changes.
     * Then corrects a rule which uses X in the head at the Y position by replacing X by Y in the head as well as all occurrences
     */
    fun detachAndPolish() {
        val h = this.head.copy()
        this.head = h
        this.body.detach()
        if (this.head.right == "X") {
            this.head.right = "Y"
            for (i in 0..<this.bodysize()) {
                val a = this.getBodyAtom(i)
                a!!.replace("X", "Y")
            }
        }
    }
}
