package tarmorn.threads

import tarmorn.Settings
import tarmorn.data.TripleSet
import tarmorn.structure.Rule
import tarmorn.structure.ScoreTree
import java.io.PrintWriter
import java.util.Collections
import kotlin.math.ln
import tarmorn.data.Triple


class RuleConfidenceComparator : Comparator<Rule> {
    override fun compare(o1: Rule, o2: Rule): Int {
        // double prob1;
        // double prob2;
        val prob1 = o1.appliedConfidence
        val prob2 = o2.appliedConfidence


        if (prob1 < prob2) return 1
        else if (prob1 > prob2) return -1
        return 0
    }
}


// import java.text.DecimalFormat;
object RuleEngine {
    private val EPSILON = 0.0001

    private val predictionTasks = mutableListOf<Triple>()
    private var predictionsMade = 0
    private var predictionsWriter: PrintWriter? = null

    private const val DEBUG_TESTSET_SUBSET = 0


    fun materializeRules(rules: MutableList<Rule>, trainingSet: TripleSet, materializedSet: TripleSet) {
        var ruleCounter = 0

        for (rule in rules) {
            ruleCounter++
            if (ruleCounter % (rules.size / 100) == 0) println("* " + (100.0 * (ruleCounter / rules.size.toDouble())) + "% of all rules materialized")
            if (rule.bodysize() > 2) continue
            val materializedRule = rule.materialize(trainingSet)
            if (materializedRule != null) {
                // println(materializedRule.size());
                materializedSet.addTripleSet(materializedRule)
                // println(materializedSet.size());
            }
        }
    }


    fun applyRulesARX(
        rules: MutableList<Rule>,
        testSet: TripleSet,
        trainingSet: TripleSet,
        validationSet: TripleSet,
        k: Int,
        resultsWriter: PrintWriter?
    ) {
        var testSet = testSet
        if (DEBUG_TESTSET_SUBSET > 0) {
            println("* debugging mode, choosing small fraction of testset")
            val testSetReduced = TripleSet()
            for (i in 0..<DEBUG_TESTSET_SUBSET) {
                val t = testSet.triples.get(i)
                testSetReduced.addTriple(t)
            }
            for (i in DEBUG_TESTSET_SUBSET..<testSet.triples.size) {
                val t = testSet.triples.get(i)
                validationSet.addTriple(t)
            }
            testSet = testSetReduced
        }

        println("* applying rules")

        // HashMap<String, HashSet<Rule>> relation2Rules = createRuleIndex(rules);
        val relation2Rules4Prediction = createOrderedRuleIndex(rules)


        // TODO fix something here
        println("* set up index structure covering rules for prediction for " + relation2Rules4Prediction.size + " relations")


        //TripleSet filterSet = new TripleSet();

        // filterSet.addTripleSet(trainingSet);
        // filterSet.addTripleSet(validationSet);
        // filterSet.addTripleSet(testSet);
        // if (materializedSet != null) trainingSet.addTripleSet(materializedSet);


        // println("* constructed filter set with " + filterSet.getTriples().size() + " triples");
        //if (filterSet.getTriples().size() == 0) {
        //	System.err.println("WARNING: using empty filter set!");
        //}
        // prepare the data structures used a s cache for question that are reoccuring
        // HashMap<SimpleImmutableEntry<String, String>, LinkedHashMap<String, Double>> headCandidateCache = new HashMap<SimpleImmutableEntry<String, String>, LinkedHashMap<String, Double>>();
        // HashMap<SimpleImmutableEntry<String, String>, LinkedHashMap<String, Double>> tailCandidateCache = new HashMap<SimpleImmutableEntry<String, String>, LinkedHashMap<String, Double>>();
        // start iterating over the test cases

        // int counter = 0;
        // long startTime = System.currentTimeMillis();
        // long currentTime = 0;
        ScoreTree.LOWER_BOUND = k
        ScoreTree.UPPER_BOUND = ScoreTree.LOWER_BOUND
        ScoreTree.EPSILON = EPSILON


        predictionTasks.addAll(testSet.triples)
        predictionsWriter = resultsWriter

        val predictors = Array(Settings.WORKER_THREADS) {
            Predictor(testSet, trainingSet, validationSet, k, relation2Rules4Prediction)
        }
        print("* creating worker threads ")
        for (threadCounter in 0..<Settings.WORKER_THREADS) {
            print("#$threadCounter ")
            predictors[threadCounter].start()
        }
        println()



        while (alive(predictors)) {
            try {
                Thread.sleep(500)
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
        predictionsWriter!!.flush()
        predictionsWriter!!.close()

        println("* done with rule application")
        predictionsMade = 0
    }

    private fun alive(threads: Array<out Thread>): Boolean {
        for (t in threads) {
            if (t.isAlive) return true
        }
        return false
    }

    @get:Synchronized
    val nextPredictionTask: Triple?
        get() {
            predictionsMade++
            val triple = if (predictionTasks.isNotEmpty()) predictionTasks.removeAt(0) else null
            if (predictionsMade % 100 == 0) {
                if (triple != null) println("* (#" + predictionsMade + ") trying to guess the tail and head of " + triple.toString())
                predictionsWriter!!.flush()
            }
            return triple
        }


    /*

	public static synchronized Rule getNextRuleMaterializationTask()  {
		Rule rule = ruleMaterializationTasksListed.poll();
		if (rule == null) return null;
		// ruleMaterializationTasks.remove(rule);
		return rule;
	}

	public static synchronized void addRuleToBeMaterialized(Rule rule)  {
		if (ruleMaterializationTasks.contains(rule)) return;
		ruleMaterializationTasksListed.add(rule);
		// println("list=" + ruleMaterializationTasksListed.size());
		ruleMaterializationTasks.add(rule);
		// println("set=" + ruleMaterializationTasks.size());
	}

	public static void materializeRule(Rule rule, TripleSet ts) {

		System.err.println("materialize rule: " + rule);
		TripleSet materializedSet = rule.materialize(ts);
		System.err.println("finished");

		synchronized (materializedRules) {
			materializedRules.put(rule, materializedSet);
			ruleMaterializationsMade++;
		}
		System.err.println("... and stored");

	}

	*/
    fun predictMax(
        testSet: TripleSet,
        trainingSet: TripleSet,
        validationSet: TripleSet,
        k: Int,
        relation2Rules4Prediction: MutableMap<String, MutableList<Rule>>,
        triple: tarmorn.data.Triple
    ) {
        //println("=== " + triple + " ===");
        var kTree = ScoreTree()
        val kTailCandidates =
            predictMax(testSet, trainingSet, validationSet, k, relation2Rules4Prediction, triple, false, kTree)
        val kTailTree = kTree
        kTree = ScoreTree()

        val kHeadCandidates =
            predictMax(testSet, trainingSet, validationSet, k, relation2Rules4Prediction, triple, true, kTree)
        val kHeadTree = kTree

        if (Settings.PATH_EXPLANATION != "") writeTopKExplanation(
            triple,
            testSet,
            kHeadCandidates,
            kHeadTree,
            kTailCandidates,
            kTailTree,
            k
        )
        RuleEngine.writeTopKCandidates(triple, testSet, kHeadCandidates, kTailCandidates, predictionsWriter!!, k)
    }


    /*
	private static void replaceSelfByValue_(LinkedHashMap<String, Double> kCandidates, String value) {

		if (kCandidates.containsKey(Settings.REWRITE_REFLEXIV_TOKEN)) {
			double confidence = kCandidates.get(Settings.REWRITE_REFLEXIV_TOKEN);
			kCandidates.remove(Settings.REWRITE_REFLEXIV_TOKEN);
			kCandidates.put(value, confidence);
		}
	}
	*/
    // OLD VERSION: its completely unclear why the previous
    // there is not difference on FB15k between old and new version, thats why new should be preferred
    fun predictMaxOLD(
        testSet: TripleSet,
        trainingSet: TripleSet,
        filterSet: TripleSet,
        k: Int,
        relation2Rules: MutableMap<String, MutableList<Rule>>,
        triple: tarmorn.data.Triple,
        predictHeadNotTail: Boolean,
        kTree: ScoreTree
    ): LinkedHashMap<String, Double> {
        val relation = triple.r
        val head = triple.h
        val tail = triple.t

        if (relation2Rules.containsKey(relation)) {
            val relevantRules: MutableList<Rule> = relation2Rules.get(relation)!!

            var previousRule: Rule? = null
            var candidates: MutableSet<String> = mutableSetOf<String>()
            val fCandidates: MutableSet<String> = mutableSetOf<String>()

            for (rule in relevantRules) {
                // long startTime = System.currentTimeMillis();
                if (previousRule != null) {
                    if (predictHeadNotTail) candidates = previousRule.computeHeadResults(tail!!, trainingSet)
                    else candidates = previousRule.computeTailResults(head, trainingSet)
                    fCandidates.addAll(
                        getFilteredEntities(
                            trainingSet,
                            filterSet,
                            testSet,
                            triple,
                            candidates,
                            !predictHeadNotTail
                        )
                    ) // the negation seems to be okay here
                    if (previousRule.appliedConfidence > rule.appliedConfidence) {
                        if (!kTree.fine()) {
                            if (fCandidates.size > 0) {
                                if (Settings.PATH_EXPLANATION != "") kTree.addValues(
                                    previousRule.appliedConfidence,
                                    fCandidates,
                                    previousRule
                                )
                                else kTree.addValues(previousRule.appliedConfidence, fCandidates, null)
                                fCandidates.clear()
                            }
                        } else break
                    }
                }
                previousRule = rule
            }

            if (!kTree.fine() && previousRule != null) {
                if (predictHeadNotTail) candidates = previousRule.computeHeadResults(tail!!, trainingSet)
                else candidates = previousRule.computeTailResults(head, trainingSet)
                fCandidates.addAll(
                    getFilteredEntities(
                        trainingSet,
                        filterSet,
                        testSet,
                        triple,
                        candidates,
                        !predictHeadNotTail
                    )
                )
                if (Settings.PATH_EXPLANATION != "") kTree.addValues(
                    previousRule.appliedConfidence,
                    fCandidates,
                    previousRule
                )
                else kTree.addValues(previousRule.appliedConfidence, fCandidates, null)
                fCandidates.clear()
            }
        }

        val kCandidates = LinkedHashMap<String, Double>()
        kTree.getAsLinkedList(kCandidates, (if (predictHeadNotTail) tail else head))

        return kCandidates
    }


    // NEW SIMPLIFIED VERSION
    fun predictMax(
        testSet: TripleSet,
        trainingSet: TripleSet,
        validationSet: TripleSet,
        k: Int,
        relation2Rules: MutableMap<String, MutableList<Rule>>,
        triple: tarmorn.data.Triple,
        predictHeadNotTail: Boolean,
        kTree: ScoreTree
    ): LinkedHashMap<String, Double> {
        val relation = triple.r
        val head = triple.h
        val tail = triple.t

        if (relation2Rules.containsKey(relation)) {
            val relevantRules: MutableList<Rule> = relation2Rules.get(relation)!!
            var candidates: MutableSet<String> = mutableSetOf<String>()
            val fCandidates: MutableSet<String> = mutableSetOf<String>()
            for (rule in relevantRules) {
                if (predictHeadNotTail) candidates = rule.computeHeadResults(tail!!, trainingSet)
                else candidates = rule.computeTailResults(head, trainingSet)
                fCandidates.addAll(
                    getFilteredEntities(
                        trainingSet,
                        validationSet,
                        testSet,
                        triple,
                        candidates,
                        !predictHeadNotTail
                    )
                ) // the negation seems to be okay here
                if (!kTree.fine()) {
                    if (fCandidates.size > 0) {
                        if (Settings.PATH_EXPLANATION != "") kTree.addValues(
                            rule.appliedConfidence,
                            fCandidates,
                            rule
                        )
                        else kTree.addValues(rule.appliedConfidence, fCandidates, null)
                        fCandidates.clear()
                    }
                } else break
            }
        }
        val kCandidates = LinkedHashMap<String, Double>()
        kTree.getAsLinkedList(kCandidates, (if (predictHeadNotTail) tail else head))

        return kCandidates
    }


    fun predictNoisyOr(
        testSet: TripleSet,
        trainingSet: TripleSet,
        validationSet: TripleSet,
        k: Int,
        relation2Rules: MutableMap<String, MutableList<Rule>>,
        triple: tarmorn.data.Triple
    ) {
        val relation = triple.r
        val head = triple.h
        val tail = triple.t

        val explainedTailCandidates = mutableMapOf<String, MutableList<Rule>>()
        val explainedHeadCandidates = mutableMapOf<String, MutableList<Rule>>()

        if (relation2Rules.containsKey(relation)) {
            val relevantRules: MutableList<Rule> = relation2Rules.get(relation)!!
            for (rule in relevantRules) {
                val tailCandidates = rule.computeTailResults(head, trainingSet)
                val fTailCandidates = getFilteredEntities(
                    trainingSet, validationSet, testSet, triple,
                    tailCandidates, true
                )
                for (fTailCandidate in fTailCandidates) {
                    if (!explainedTailCandidates.containsKey(fTailCandidate)) explainedTailCandidates.put(
                        fTailCandidate,
                        mutableListOf<Rule>()
                    )
                    explainedTailCandidates.get(fTailCandidate)!!.add(rule)
                }

                val headCandidates = rule.computeHeadResults(tail!!, trainingSet)
                val fHeadCandidates = getFilteredEntities(
                    trainingSet, validationSet, testSet, triple,
                    headCandidates, false
                )
                for (fHeadCandidate in fHeadCandidates) {
                    if (!explainedHeadCandidates.containsKey(fHeadCandidate)) explainedHeadCandidates.put(
                        fHeadCandidate,
                        ArrayList<Rule>()
                    )
                    explainedHeadCandidates.get(fHeadCandidate)!!.add(rule)
                }
            }
        }

        val kTailCandidates = LinkedHashMap<String, Double>()
        val kHeadCandidates = LinkedHashMap<String, Double>()


//        replaceMyselfByEntity(kTailCandidates, head)
//        replaceMyselfByEntity(kHeadCandidates, tail)

        // final sorting
        sortByValue(kTailCandidates)
        sortByValue(kHeadCandidates)

        //  if (Settings.PATH_EXPLANATION != "") writeTopKExplanation(triple, testSet,
        // kHeadCandidates, explainedHeadCandidates, kTailCandidates,
        // explainedTailCandidates, k);
        RuleEngine.writeTopKCandidates(triple, testSet, kHeadCandidates, kTailCandidates, predictionsWriter!!, k)
    }


    private fun computeNoisyOr(
        allCandidates: HashMap<String, ArrayList<Rule>>,
        kCandidates: LinkedHashMap<String, Double>
    ) {
        for (cand in allCandidates.keys) {
            var log_prob_sum = 0.0

            val num_rules: Int
            if (Settings.AGGREGATION_MAX_NUM_RULES_PER_CANDIDATE < 0 || allCandidates.get(cand)!!.size < Settings.AGGREGATION_MAX_NUM_RULES_PER_CANDIDATE) {
                num_rules = allCandidates.get(cand)!!.size
            } else {
                num_rules = Settings.AGGREGATION_MAX_NUM_RULES_PER_CANDIDATE
            }

            var ctr = 1
            for (r in allCandidates.get(cand)!!) {
                log_prob_sum += ln(1 - r.appliedConfidence)
                if (ctr == num_rules) {
                    break
                } else {
                    ctr += 1
                }
            }
            // double score = 1-Math.exp(log_prob_sum);
            val score = -1 * log_prob_sum
            kCandidates.put(cand, score)
        }
    }


    // fun replaceMyselfByEntity(candidates: LinkedHashMap<String, Double>, replacement: String) {
    //     if (candidates.containsKey(Settings.REWRITE_REFLEXIV_TOKEN)) {
    //         val myselfConf: Double = candidates.get(Settings.REWRITE_REFLEXIV_TOKEN)!!
    //         candidates.remove(Settings.REWRITE_REFLEXIV_TOKEN)
    //         candidates.put(replacement, myselfConf)
    //     }
    // }


    /*
	private static void show(LinkedHashMap<String, Double> kCandidates, String headline) {
		println("*** " + headline + " ***");
		for (String candidate : kCandidates.keySet()) {
			double conf = kCandidates.get(candidate);
			println(conf + " = " + candidate);
		}
	}

	private static HashMap<String, HashSet<Rule>> createRuleIndex(List<Rule> rules) {

		// int counterL1C = 0;
		// int counterL2C = 0;
		// int counterL1AC = 0;
		// int counterL1AN = 0;
		// int counterOther = 0;

		HashMap<String, HashSet<Rule>> relation2Rules = new HashMap<String, HashSet<Rule>>();
		for (Rule rule : rules) {



			if (rule.isXYRule()) {
				if (rule.bodysize() == 1)  counterL1C++;
				if (rule.bodysize() == 2)  counterL2C++;
			}
			else {

				if (rule.bodysize() == 1)  {
					if (rule.hasConstantInBody()) counterL1AC++;
					else counterL1AN++;
				}
				else {
					if (rule.hasConstantInBody()) continue;
				}
			}


			String relation = rule.getTargetRelation();
			if (!relation2Rules.containsKey(relation)) relation2Rules.put(relation, new HashSet<Rule>());
			relation2Rules.get(relation).add(rule);


		}
		// println("L1C=" + counterL1C + " L2C=" + counterL2C + " L1AC=" + counterL1AC + " L1AN=" + counterL1AN + " OTHER=" + counterOther);
		return relation2Rules;
	}
	*/
    fun createOrderedRuleIndex(rules: MutableList<Rule>): MutableMap<String, MutableList<Rule>> {
        // String predictionGoal = headNotTailPrediction ? "head" : "tail";
        val relation2Rules = mutableMapOf<String, MutableList<Rule>>()
        var l: Long = 0
        for (rule in rules) {
            if (Settings.THRESHOLD_CORRECT_PREDICTIONS > rule.correctlyPredicted) continue
            if (Settings.THRESHOLD_CONFIDENCE > rule.confidence) continue

            val relation = rule.targetRelation
            if (!relation2Rules.containsKey(relation)) {
                relation2Rules.put(relation, mutableListOf<Rule>())
            }
            relation2Rules.get(relation)!!.add(rule)
            if (l % 100000 == 0L && l > 1) {
                println("* indexed " + l + " rules for prediction")
            }
            l++
        }
        for (relation in relation2Rules.keys) {
            // relation2Rules.get(relation)!!.trimToSize()
            Collections.sort<Rule>(relation2Rules.get(relation), RuleConfidenceComparator())
        }
        println("* indexed and sorted " + l + " rules for using them to make predictions")
        return relation2Rules
    }


    /*
	private static void updateCandidateProbabailities(Rule rule, boolean tailNotHead, String candidate, HashMap<String, Double> candidates2Probabilities) {
		double prob = rule.getAppliedConfidence();
		if (!candidates2Probabilities.containsKey(candidate)) candidates2Probabilities.put(candidate, prob);
		else {
			double previousProb = candidates2Probabilities.get(candidate);
			double newProb = combineProbability(prob, previousProb);
			candidates2Probabilities.put(candidate, newProb);
		}
	}
	*/
    /*
	private static LinkedHashMapK getFilteredCandidates(TripleSet filterSet, TripleSet testSet, Triple t, HashMap<String, Double> candidates, boolean tailNotHead) {
		// LinkedHashMap<String, Double> candidatesSorted = sortByValue(candidates);
		LinkedHashMap<String, Double> kCandidates = new LinkedHashMap<String, Double>();
		int i = 0;
		for (Entry<String, Double> entry : candidates.entrySet()) {
			if (!tailNotHead) {
				if (!filterSet.isTrue(entry.getKey(), t.getRelation(), t.getTail())) {
					kCandidates.put(entry.getKey(), entry.getValue());
					i++;
				}
				if (testSet.isTrue(entry.getKey(), t.getRelation(), t.getTail())) {
					kCandidates.put(entry.getKey(), entry.getValue());
				}
			}
			if (tailNotHead) {
				if (!filterSet.isTrue(t.getHead(), t.getRelation(), entry.getKey())) {
					kCandidates.put(entry.getKey(), entry.getValue());
					i++;
				}
				if (testSet.isTrue(t.getHead(), t.getRelation(), entry.getKey())) {
					kCandidates.put(entry.getKey(), entry.getValue());
				}
			}
		}
		return (new LinkedHashMapK(kCandidates, i));
	}
	*/
    private fun getFilteredEntities(
        trainingSet: TripleSet,
        validationSet: TripleSet,
        testSet: TripleSet,
        t: tarmorn.data.Triple,
        candidateEntities: MutableSet<String>,
        tailNotHead: Boolean
    ): HashSet<String> {
        // LinkedHashMap<String, Double> candidatesSorted = sortByValue(candidates);
        val filteredEntities = HashSet<String>()
        for (entity in candidateEntities) {
            if (!tailNotHead) {
                if (!validationSet.isTrue(entity, t.r, t.t) && !trainingSet.isTrue(entity, t.r, t.t) && !testSet.isTrue(
                        entity,
                        t.r,
                        t.t
                    )
                ) {
                    filteredEntities.add(entity)
                }
                if (testSet.isTrue(entity, t.r, t.t)) {
                    // TAKE CARE, remove to reactivate the possibility of storing previous results
                    if (entity == t.h) filteredEntities.add(entity)
                }
            }
            if (tailNotHead) {
                if (!validationSet.isTrue(t.h, t.r, entity) && !trainingSet.isTrue(t.h, t.r, entity) && !testSet.isTrue(
                        t.h,
                        t.r,
                        entity
                    )
                ) {
                    filteredEntities.add(entity)
                }
                if (testSet.isTrue(t.h, t.r, entity)) {
                    // TAKE CARE, remove to reactivate the possibility of storing previous results
                    if (entity == t.t) filteredEntities.add(entity)
                }
            }
        }
        return filteredEntities
    }

    @Synchronized
    private fun writeTopKCandidates(
        t: tarmorn.data.Triple,
        testSet: TripleSet,
        kHeadCandidates: LinkedHashMap<String, Double>,
        kTailCandidates: LinkedHashMap<String, Double>,
        writer: PrintWriter,
        k: Int
    ) {
        writer.println(t)
        var i = 0
        writer.print("Heads: ")
        for (entry in kHeadCandidates.entries) {
            if (t.h == entry.key || !testSet.isTrue(entry.key, t.r, t.t)) {
                writer.print(entry.key + "\t" + entry.value + "\t")
                i++
            }
            if (i == k) break
        }
        writer.println()
        i = 0
        writer.print("Tails: ")
        for (entry in kTailCandidates.entries) {
            if (t.t == entry.key || !testSet.isTrue(t.h, t.r, entry.key)) {
                writer.print(entry.key + "\t" + entry.value + "\t")
                i++
            }
            if (i == k) break
        }
        writer.println()
        writer.flush()
    }

    @Synchronized
    private fun writeTopKExplanation(
        t: tarmorn.data.Triple?,
        testSet: TripleSet?,
        kHeadCandidates: LinkedHashMap<String, Double>?,
        headTree: ScoreTree?,
        kTailCandidates: LinkedHashMap<String, Double>?,
        tailTree: ScoreTree?,
        k: Int
    ) {
        Settings.EXPLANATION_WRITER!!.println(t)
        Settings.EXPLANATION_WRITER!!.println("Heads:")
        Settings.EXPLANATION_WRITER!!.println(headTree)
        Settings.EXPLANATION_WRITER!!.println("Tails:")
        Settings.EXPLANATION_WRITER!!.println(tailTree)
        Settings.EXPLANATION_WRITER!!.flush()
    }


    /*
	private static synchronized void writeTopKCandidatesPlusExplanation(Triple t, TripleSet testSet, LinkedHashMap<String, Double> kHeadCandidates, ScoreTree allHeadCandidates, LinkedHashMap<String, Double> kTailCandidates, ScoreTree allTailCandidates,	PrintWriter writer, int k) {
		Settings.EXPLANATION_WRITER.println(t);
		Settings.EXPLANATION_WRITER.println("Heads:");
		Settings.EXPLANATION_WRITER.println(allHeadCandidates);
		Settings.EXPLANATION_WRITER.println("Tails:");
		Settings.EXPLANATION_WRITER.println(allTailCandidates);
		Settings.EXPLANATION_WRITER.flush();
	}
	*/
    /*
	private static void processTopKCandidates(TripleSet testSet, Triple t, HashMap<String, Double> tailCandidates, HashMap<String, Double> headCandidates, TripleSet filterSet, int k, PrintWriter writer, HashMap<String, Double> kTailCandidates, HashMap<String, Double> kHeadCandidates) {
		LinkedHashMap<String, Double> tailCandidatesSorted = sortByValue(tailCandidates);
		LinkedHashMap<String, Double> headCandidatesSorted = sortByValue(headCandidates);
		writer.println(t);
		writer.print("Heads: ");
		int i = 0;
		for (Entry<String, Double> entry : headCandidatesSorted.entrySet()) {
			if (i < k) {
				if (!filterSet.isTrue(entry.getKey(), t.getRelation(), t.getTail()) || t.getHead().equals(entry.getKey())) {
					writer.print(entry.getKey() + "\t" + entry.getValue() + "\t");
					kHeadCandidates.put(entry.getKey(), entry.getValue());
					i++;
				}
				if (testSet.isTrue(entry.getKey(), t.getRelation(), t.getTail())) {
					kHeadCandidates.put(entry.getKey(), entry.getValue());
				}
			}
		}
		writer.println();
		writer.print("Tails: ");
		int j = 0;
		for (Entry<String, Double> entry : tailCandidatesSorted.entrySet()) {
			if (j < k) {
				if (!filterSet.isTrue(t.getHead(), t.getRelation(), entry.getKey())
						|| t.getTail().equals(entry.getKey())) {
					writer.print(entry.getKey() + "\t" + entry.getValue() + "\t");
					kTailCandidates.put(entry.getKey(), entry.getValue());
					j++;
				}
				if (testSet.isTrue(t.getHead(), t.getRelation(), entry.getKey())) {
					kTailCandidates.put(entry.getKey(), entry.getValue());
				}
			}
		}
		writer.println();
		writer.flush();
	}
	*/
    /*
	private static double combineProbability(double prob, double previousProb) {
		double newProb;
		switch (COMBINATION_RULE_ID) {
		case 1: // multiplication
			newProb = 1.0 - ((1.0 - previousProb) * (1.0 - prob));
			break;
		case 2: // maxplus
			newProb = Math.max(previousProb, prob) + EPSILON;
			break;
		case 3: // max
		default:
			newProb = Math.max(previousProb, prob);
			break;
		}
		return newProb;
	}
	*/
    /*
	private static <K, V extends Comparable<? super V>> LinkedHashMap<K, V> sortByValue(Map<K, V> map) {
		return map.entrySet().stream().sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
	}
	*/
    fun sortByValue(m: LinkedHashMap<String, Double>) {
        val entries: MutableList<MutableMap.MutableEntry<String, Double>> =
            ArrayList<MutableMap.MutableEntry<String, Double>>(m.entries)
        Collections.sort<MutableMap.MutableEntry<String, Double>>(
            entries,
            object : Comparator<MutableMap.MutableEntry<String, Double>> {
                override fun compare(
                    lhs: MutableMap.MutableEntry<String, Double>,
                    rhs: MutableMap.MutableEntry<String, Double>
                ): Int {
                    if (lhs.value!! < rhs.value!!) return 1
                    else if (lhs.value!! > rhs.value!!) return -1
                    else return 0
                }
            })

        m.clear()
        for (e in entries) {
            m.put(e.key, e.value)
        }
    }
}