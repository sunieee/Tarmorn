package tarmorn.threads

import tarmorn.Learn.active
import tarmorn.Learn.areAllThere
import tarmorn.Learn.heyYouImHere
import tarmorn.Learn.isStored
import tarmorn.Learn.storeRule
import tarmorn.Settings
import tarmorn.algorithm.PathSampler
import tarmorn.data.TripleSet
import tarmorn.structure.Rule
import tarmorn.structure.RuleFactory
import tarmorn.structure.RuleZero
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


    // this is not really well done, exactly one of them has to be true all the time
    private var mineParamCyclic = true
    private var mineParamAcyclic = false
    private var mineParamZero = false

    private var mineParamLength = 1 // possible values are 1 and 2 (if non-cyclic), or 1, 2, 3, 4, 5 if (cyclic)


    private var ready = false

    private var onlyXY = false


    // ***** lets go ******
    init {
        this.sampler = PathSampler(triples)
        this.id = id
    }

    fun setSearchParameters(zero: Boolean, cyclic: Boolean, acyclic: Boolean, len: Int) {
        this.mineParamZero = zero
        this.mineParamCyclic = cyclic
        this.mineParamAcyclic = acyclic
        this.mineParamLength = len
        this.ready = true
        this.onlyXY = false
        if (this.mineParamCyclic) {
            if (this.mineParamLength > Settings.MAX_LENGTH_GROUNDED_CYCLIC) {
                this.onlyXY = true
            }
        }
        //println("THREAD-" + this.id + " using parameters C=" + this.mineParamCyclic + " L=" + this.mineParamLength);
    }

    private val type: String
        get() {
            if (this.mineParamZero) return "Zero"
            if (this.mineParamCyclic) return "Cyclic"
            if (this.mineParamAcyclic) return "Acyclic"
            return ""
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
        println("THREAD-" + this.id + " starts to work with L=" + this.mineParamLength + " C=" + this.type + " ")


        // outer loop is missing
        val done = false
        while (done == false) {
            if (!active(
                    this.id,
                    this.storedRules,
                    this.createdRules,
                    this.producedScore,
                    this.mineParamZero,
                    this.mineParamCyclic,
                    this.mineParamAcyclic,
                    this.mineParamLength
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
                // search for zero rules
                if (mineParamZero) {
                    val path = sampler.samplePath(this.mineParamLength + 1, false)

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
                if (mineParamCyclic) {
                    val path = sampler.samplePath(this.mineParamLength + 1, true)
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
                if (mineParamAcyclic) {
                    val path = sampler.samplePath(mineParamLength + 1, false)
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
        if (Settings.REWARD == 5) return correctlyPredicted.toDouble() * appliedConfidence / 2.0.pow((rule.bodysize() - 1).toDouble())
        return 0.0
    }
}
