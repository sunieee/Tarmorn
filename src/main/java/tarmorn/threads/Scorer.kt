package tarmorn.threads

import tarmorn.Learn.active
import tarmorn.Learn.areAllThere
import tarmorn.Learn.heyYouImHere
import tarmorn.Learn.isStored
import tarmorn.Learn.storeRule
import tarmorn.Settings
import tarmorn.data.IdManager
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.structure.*
import kotlin.math.pow
import kotlin.random.Random


/**
 * The worker thread responsible for learning rules in the reinforced learning setting.
 */
class Scorer(val triples: TripleSet, val id: Int) : Thread() {
    private val rand = Random.Default

    // private int entailedCounter = 1;
    private var createdRules = 0
    private var storedRules = 0
    private var producedScore = 0.0

    // Unified rule type representation using Int instead of multiple booleans
    private var ruleType = 1

    private var ready = false

    private var onlyXY = false


    /**
     * Set the search parameters using unified rule type representation
     * @param ruleType The unified rule type (see Dice.RULE_TYPE_* constants)
     */
    fun setSearchParameters(ruleType: Int) {
        require(Dice.isValidRuleType(ruleType)) { "Invalid rule type: $ruleType" }
        this.ruleType = ruleType
        this.ready = true
        this.onlyXY = false
        
        // Validate rule type constraints
        if (ruleType >=1 && ruleType <= 10) {
            val length = ruleType % 10
            if (length > Settings.MAX_LENGTH_GROUNDED_CYCLIC) {
                this.onlyXY = true
            }
        }
    }

    override fun run() {
        while (!areAllThere()) {
            heyYouImHere(this.id)
            try {
                sleep(20)
            } catch (e: InterruptedException) {
                // TODO Auto-generated catch block
                e.printStackTrace()
            }
            // println("THREAD-" + this.id + " waiting for the others");
        }
        println("THREAD-" + this.id + " starts to work with type=" + Dice.getRuleTypeName(ruleType) + " ")


        // outer loop is missing
        val done = false
        while (done == false) {
            if (!active(
                    this.id,
                    this.storedRules,
                    this.createdRules,
                    this.producedScore,
                    ruleType,
                ) || !ready
            ) {
                this.createdRules = 0
                this.storedRules = 0
                this.producedScore = 0.0
                try {
                    sleep(10)
                } catch (e: InterruptedException) {
                    e.printStackTrace()
                }
            } else {
                val start = System.currentTimeMillis()
                
                // Determine rule mining parameters from unified type
                val isZero = ruleType==0
                val isCyclic = ruleType>=1 && ruleType<=10
                val isAcyclic = ruleType>10
                val length = ruleType % 10
                
                // search for zero rules
                if (isZero) {
                    val path = samplePath(length + 1, false)

                    // println("zero (sample with steps=" + (this.mineParamLength+1) + "):" + path);
                    if (path != null) {
                        val learnedRules = getGeneralizations(path, false)
                        if (!active) {
                            try {
                                sleep(10)
                            } catch (e: InterruptedException) {
                                e.printStackTrace()
                            }
                        } else {
                            for (learnedRule in learnedRules) {
                                this.createdRules++
                                if (learnedRule.isTrivial) continue
                                if (isStored(learnedRule)) {
                                    //long t1 = System.currentTimeMillis();

                                    learnedRule.computeScores(this.triples)

                                    //long t2 = System.currentTimeMillis();
                                    //if (t2 - t1 > 500) {
                                    //	println("* elapsed: " + (t2 - t1) + " >>> " + learnedRule);
                                    //}
                                    if (learnedRule.confidence >= Settings.THRESHOLD_CONFIDENCE && learnedRule.correctlyPredicted >= Settings.THRESHOLD_CORRECT_PREDICTIONS && (learnedRule !is RuleZero || learnedRule.correctlyPredicted > Settings.THRESHOLD_CORRECT_PREDICTIONS_ZERO)) {
                                        if (active) {
                                            storeRule(learnedRule)
                                            // println(">>> " +  learnedRule);
                                            this.producedScore += getScoringGain(
                                                learnedRule,
                                                learnedRule.correctlyPredicted,
                                                learnedRule.confidence,
                                                learnedRule.appliedConfidence
                                            )
                                            this.storedRules++
                                        }
                                    }
                                }
                            }
                        }
                    }


                    /*
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					*/
                }


                // search for cyclic rules
                if (isCyclic) {
                    val path = samplePath(length + 1, true)
                    if (path != null && path.isValid) {
                        // println(path);
                        val learnedRules = getGeneralizations(path, this.onlyXY)
                        // println(learnedRules.size());
                        if (!active) {
                            try {
                                sleep(10)
                            } catch (e: InterruptedException) {
                                e.printStackTrace()
                            }
                        } else {
                            for (learnedRule in learnedRules) {
                                this.createdRules++
                                if (learnedRule.isTrivial) continue
                                // if (learnedRule.isRedundantACRule(triples)) continue;
                                // long l2;
                                // long l1 = System.currentTimeMillis();
                                if (isStored(learnedRule)) {
                                    //long t1 = System.currentTimeMillis();

                                    learnedRule.computeScores(this.triples)

                                    //long t2 = System.currentTimeMillis();
                                    //if (t2 - t1 > 500) {
                                    //	println("* elapsed: " + (t2 - t1) + " >>> " + learnedRule);
                                    //}
                                    if (learnedRule.confidence >= Settings.THRESHOLD_CONFIDENCE && learnedRule.correctlyPredicted >= Settings.THRESHOLD_CORRECT_PREDICTIONS && (learnedRule !is RuleZero || learnedRule.correctlyPredicted > Settings.THRESHOLD_CORRECT_PREDICTIONS_ZERO)) {
                                        if (active) {
                                            storeRule(learnedRule)

                                            // this.producedScore += getScoringGain(learnedRule.getCorrectlyPredictedMax(), learnedRule.getConfidenceMax());
                                            this.producedScore += getScoringGain(
                                                learnedRule,
                                                learnedRule.correctlyPredicted,
                                                learnedRule.confidence,
                                                learnedRule.appliedConfidence
                                            )
                                            this.storedRules++
                                        }
                                    }
                                } else {
                                    // l2 = System.currentTimeMillis();
                                }


                                // if (l2 - l1 > 100) println("uppps");
                            }
                        }
                    }
                }
                // search for acyclic rules
                if (isAcyclic) {
                    val path = samplePath(length + 1, false)
                    if (path != null && path.isValid) {
                        val learnedRules = getGeneralizations(path, false)
                        if (!active) {
                            try {
                                sleep(10)
                            } catch (e: InterruptedException) {
                                e.printStackTrace()
                            }
                        } else {
                            for (learnedRule in learnedRules) {
                                this.createdRules++
                                if (learnedRule.isTrivial) continue
                                // long l2;
                                //long l1 = System.currentTimeMillis();
                                if (isStored(learnedRule)) {
                                    // l2 = System.currentTimeMillis();

                                    //long t1 = System.currentTimeMillis();

                                    learnedRule.computeScores(this.triples)


                                    //long t2 = System.currentTimeMillis();
                                    //if (t2 - t1 > 500) {
                                    //	println("* elapsed: " + (t2 - t1) + " >>> " + learnedRule);
                                    //}
                                    if (learnedRule.confidence >= Settings.THRESHOLD_CONFIDENCE && learnedRule.correctlyPredicted >= Settings.THRESHOLD_CORRECT_PREDICTIONS && (learnedRule !is RuleZero || learnedRule.correctlyPredicted > Settings.THRESHOLD_CORRECT_PREDICTIONS_ZERO)) {
                                        if (active) {
                                            storeRule(learnedRule)
                                            this.producedScore += getScoringGain(
                                                learnedRule,
                                                learnedRule.correctlyPredicted,
                                                learnedRule.confidence,
                                                learnedRule.appliedConfidence
                                            )
                                            this.storedRules++
                                        }
                                    }
                                } else {
                                }
                                // if (l2 - l1 > 200) println("uppps");
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Sample a path from the triple set (merged from PathSampler)
     */
    private fun samplePath(steps: Int, cyclic: Boolean, chosenHeadTriple: Triple? = null, ruleToBeExtended: Rule? = null): Path? {
        val entityNodes = IntArray(1 + steps)
        val relationNodes = LongArray(steps)
        val markers = CharArray(steps)
        
        val chosenTriples = Settings.SINGLE_RELATIONS?.let { relations ->
            val singleRelation = relations.random(rand)
            val singleRelationId = IdManager.getRelationId(singleRelation)
            triples.getTriplesByRelation(singleRelationId).also { tripleList ->
                if (tripleList.isEmpty()) {
                    System.err.println("chosen a SINGLE_RELATION=$singleRelation that is not instantiated in the training data")
                    System.exit(0)
                }
            }
        } ?: triples
        
        val triple = chosenHeadTriple ?: chosenTriples.random(rand)

        // TODO hardcoded test to avoid reflexive relations in the head
        if (triple.h == triple.t) return null
        
        val dice = when {
            ruleToBeExtended?.isXRule == true -> 1.0
            ruleToBeExtended?.isYRule == true -> 0.0
            else -> rand.nextDouble()
        }
        
        if (dice < 0.5) {
            markers[0] = '+'
            entityNodes[0] = triple.h
            relationNodes[0] = triple.r
            entityNodes[1] = triple.t
        } else {
            markers[0] = '-'
            entityNodes[1] = triple.h
            relationNodes[0] = triple.r
            entityNodes[0] = triple.t
        }

        // add next hop
        for (index in 1 until steps) {
            val currentNodeId = entityNodes[index]
            val isForward = rand.nextDouble() < 0.5
            
            val candidateTriples = if (isForward) {
                triples.getTriplesByHead(currentNodeId)
            } else {
                triples.getTriplesByTail(currentNodeId)
            }
            
            if (candidateTriples.isEmpty()) return null
            
            val nextTriple = if (cyclic && index + 1 == steps) {
                val targetNodeId = entityNodes[0]
                val cyclicCandidates = candidateTriples.filter { triple ->
                    if (isForward) triple.t == targetNodeId else triple.h == targetNodeId
                }
                if (cyclicCandidates.isEmpty()) return null
                cyclicCandidates.random(rand)
            } else {
                candidateTriples.random(rand)
            }
            
            relationNodes[index] = nextTriple.r
            if (isForward) {
                entityNodes[index + 1] = nextTriple.t
                markers[index] = '+'
            } else {
                entityNodes[index + 1] = nextTriple.h
                markers[index] = '-'
            }
        }
        
        val path = Path(entityNodes, relationNodes, markers)
        return when {
            steps == 1 -> path
            !cyclic && path.isCyclic -> null
            else -> path
        }
    }

    /**
     * Generate rule generalizations from a path (merged from RuleFactory)
     * Optimized to use direct Int/Long arrays instead of String conversion
     */
    private fun getGeneralizations(p: Path, onlyXY: Boolean): ArrayList<Rule> {
        val rv = RuleUntyped()
        rv.body = Body()
        
        // Create rule head from first edge - use direct Int/Long access
        if (p.markers[0] == '+') {
            rv.head = Atom(p.entityNodes[0], p.relationNodes[0], p.entityNodes[1])
        } else {
            rv.head = Atom(p.entityNodes[1], p.relationNodes[0], p.entityNodes[0])
        }
        
        // Create rule body from remaining edges - use direct Int/Long access
        for (i in 1..<p.markers.size) {
            if (p.markers[i] == '+') {
                rv.body.add(Atom(p.entityNodes[i], p.relationNodes[i], p.entityNodes[i + 1]))
            } else {
                rv.body.add(Atom(p.entityNodes[i + 1], p.relationNodes[i], p.entityNodes[i]))
            }
        }
        val generalizations = ArrayList<Rule>()
        val leftright = rv.leftRightGeneralization
        if (leftright != null) {
            leftright.replaceAllConstantsByVariables()
            generalizations.add(RuleCyclic(leftright, 0.0))
        }
        if (onlyXY) return generalizations
        // acyclic rule
        val left = rv.leftGeneralization

        if (left != null) {
            if (left.bodySize == 0) {
                generalizations.add(RuleZero(left))
            } else {
                val leftFree = left.createCopy()
                if (leftright == null) leftFree.replaceAllConstantsByVariables()
                left.replaceNearlyAllConstantsByVariables()
                if (!Settings.EXCLUDE_AC2_RULES) if (leftright == null) generalizations.add(RuleAcyclic(leftFree))
                generalizations.add(RuleAcyclic(left))
            }
        }
        val right = rv.rightGeneralization
        if (right != null) {
            if (right.bodySize == 0) {
                generalizations.add(RuleZero(right))
            } else {
                val rightFree = right.createCopy()
                if (leftright == null) rightFree.replaceAllConstantsByVariables()
                right.replaceNearlyAllConstantsByVariables()
                if (!Settings.EXCLUDE_AC2_RULES) if (leftright == null) generalizations.add(RuleAcyclic(rightFree))
                generalizations.add(RuleAcyclic(right))
            }
        }
        return generalizations
    }

    fun getScoringGain(rule: Rule, correctlyPredicted: Int, confidence: Double, appliedConfidence: Double): Double {
        if (Settings.REWARD == 1) return correctlyPredicted.toDouble()
        if (Settings.REWARD == 2) return correctlyPredicted.toDouble() * confidence
        if (Settings.REWARD == 3) return correctlyPredicted.toDouble() * appliedConfidence
        if (Settings.REWARD == 4) return correctlyPredicted.toDouble() * appliedConfidence * appliedConfidence
        if (Settings.REWARD == 5) return correctlyPredicted.toDouble() * appliedConfidence / 2.0.pow((rule.bodySize - 1).toDouble())
        return 0.0
    }
}
