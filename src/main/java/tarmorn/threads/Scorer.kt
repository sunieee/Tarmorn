package tarmorn.threads

import tarmorn.Learn.active
import tarmorn.Learn.areAllThere
import tarmorn.Learn.heyYouImHere
import tarmorn.Learn.isStored
import tarmorn.Learn.storeRule
import tarmorn.Settings
import tarmorn.structure.PathSampler
import tarmorn.data.TripleSet
import tarmorn.structure.Rule
import tarmorn.structure.RuleFactory
import tarmorn.structure.RuleZero
import tarmorn.structure.Dice
import kotlin.math.pow


/**
 *
 * The worker thread responsible for learning rules in the reinforced learning setting.
 *
 */
class Scorer(private val triples: TripleSet, id: Int) : Thread() {
    private val sampler: PathSampler


    // private int entailedCounter = 1;
    private var createdRules = 0
    private var storedRules = 0
    private var producedScore = 0.0

    private var id = 0

    // Unified rule type representation using Int instead of multiple booleans
    private var ruleType = 1

    private var ready = false

    private var onlyXY = false


    // ***** lets go ******
    init {
        this.sampler = PathSampler(triples)
        this.id = id
    }

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
    
    /**
     * Legacy method for backward compatibility
     * @deprecated Use setSearchParameters(ruleType: Int) instead
     */
    @Deprecated("Use setSearchParameters(ruleType: Int) instead")
    fun setSearchParameters(zero: Boolean, cyclic: Boolean, acyclic: Boolean, len: Int) {
        val type = Dice.encode(zero, cyclic, acyclic, len)
        setSearchParameters(type)
    }

    private val type: String
        get() = Dice.getRuleTypeName(ruleType)


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
                    ruleType==0,
                    ruleType>=1 && ruleType<=10,
                    ruleType>10,
                    ruleType % 10
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
                    val path = sampler.samplePath(length + 1, false)

                    // println("zero (sample with steps=" + (this.mineParamLength+1) + "):" + path);
                    if (path != null) {
                        val learnedRules = RuleFactory.getGeneralizations(path, false)
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
                    val path = sampler.samplePath(length + 1, true)
                    if (path != null && path.isValid) {
                        // println(path);
                        val learnedRules = RuleFactory.getGeneralizations(path, this.onlyXY)
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
                    val path = sampler.samplePath(length + 1, false)
                    if (path != null && path.isValid) {
                        val learnedRules = RuleFactory.getGeneralizations(path, false)
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

    fun getScoringGain(rule: Rule, correctlyPredicted: Int, confidence: Double, appliedConfidence: Double): Double {
        if (Settings.REWARD == 1) return correctlyPredicted.toDouble()
        if (Settings.REWARD == 2) return correctlyPredicted.toDouble() * confidence
        if (Settings.REWARD == 3) return correctlyPredicted.toDouble() * appliedConfidence
        if (Settings.REWARD == 4) return correctlyPredicted.toDouble() * appliedConfidence * appliedConfidence
        if (Settings.REWARD == 5) return correctlyPredicted.toDouble() * appliedConfidence / 2.0.pow((rule.bodySize - 1).toDouble())
        return 0.0
    }
}
