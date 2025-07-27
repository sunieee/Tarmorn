package tarmorn

import tarmorn.Settings.load
import tarmorn.threads.RuleEngine
import tarmorn.data.TripleSet
import tarmorn.data.IdManager
import tarmorn.threads.RuleReader
import tarmorn.structure.Rule
import tarmorn.structure.RuleAcyclic
import tarmorn.structure.RuleCyclic
import tarmorn.eval.HitsAtK
import tarmorn.eval.ResultSet
import java.io.File
import java.io.IOException
import java.io.PrintWriter

object Apply {
    private var PW_JOINT_FILE: String = ""

    /**
     * Filter the rule set prior to applying it. Removes redundant rules which do not have any impact (or no desired impact).
     */
    // public static boolean FILTER = true;	
    /**
     * Always should be set to false. The TILDE results are based on a setting where this is set to true.
     * This parameter is sued to check in how far this setting increases the quality of the results.
     */
    var USE_VALIDATION_AS_BK: Boolean = false

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size == 1) {
            PW_JOINT_FILE = args[0]
            println("* using pairwise joint file: $PW_JOINT_FILE")
        }

        Rule.applicationMode()
        load()
        assert(Settings.PREDICTION_TYPE == "aRx")

        // Load datasets once for both prediction and evaluation
        val trainingSet = TripleSet(Settings.PATH_TRAINING)
        val validationSet = TripleSet(Settings.PATH_VALID, evaluate = true)
        val testSet = TripleSet(Settings.PATH_TEST, evaluate = true)

        println("=== STEP 1: RULE APPLICATION AND PREDICTION ===")
        val predictionFiles = runPrediction(trainingSet, validationSet, testSet)
        
        println("\n=== STEP 2: EVALUATION ===")
        evaluatePredictions(predictionFiles, trainingSet, validationSet, testSet)
    }

    /**
     * Run rule application and generate predictions
     * @return List of prediction output files
     */
    @Throws(IOException::class)
    private fun runPrediction(trainingSet: TripleSet, validationSet: TripleSet, testSet: TripleSet): List<String> {
        val values = getMultiProcessing(Settings.PATH_RULES)
        val predictionFiles = mutableListOf<String>()

        println("Prediction Logfile")
        println("==================\n")
        
        val ruleReader = RuleReader()
        var baseRules = mutableListOf<Rule>()

        if (Settings.PATH_RULES_BASE != "") {
            println("* Reading additional base rules from: ${Settings.PATH_RULES_BASE}")
            baseRules = ruleReader.read(Settings.PATH_RULES_BASE)
        }

        // Handle both single file and multi-processing cases
        val processValues = if (values.isEmpty()) arrayOf(null as String?) else values.map { it }.toTypedArray()

        for (value in processValues) {
            val startTime = System.currentTimeMillis()

            val (outputPath, rulesPath) = getProcessingPaths(value)
            predictionFiles.add(outputPath)

            println("Processing:")
            println("  Rules:  $rulesPath")
            println("  Output: $outputPath")
            

            val predictionWriter = PrintWriter(File(outputPath))

            if (Settings.PATH_EXPLANATION != "") {
                Settings.EXPLANATION_WRITER = PrintWriter(File(Settings.PATH_EXPLANATION))
            }

            println("* Writing predictions to: $outputPath")

            // Prepare training set (add validation if configured)
            val effectiveTrainingSet = TripleSet(Settings.PATH_TRAINING)
            var effectiveValidationSet = TripleSet(Settings.PATH_VALID)

            if (USE_VALIDATION_AS_BK) {
                effectiveTrainingSet.addTripleSet(effectiveValidationSet)
                effectiveValidationSet = TripleSet()
            }

            // Load and filter rules
            println("* Loading rules from: $rulesPath")
            val allRules = ruleReader.read(rulesPath)
            allRules.addAll(baseRules)

            val filteredRules = filterRulesByConfidence(allRules)
            println("* Loaded ${allRules.size} rules, filtered to ${filteredRules.size} rules")
            
            // Performance analysis
            if (filteredRules.size > 100000) {
                println("* WARNING: Large number of rules (${filteredRules.size}) may cause slow performance")
                println("* Consider increasing THRESHOLD_CONFIDENCE (current: ${Settings.THRESHOLD_CONFIDENCE})")
                println("* PyClause uses C++ backend and is typically faster for large rule sets")
            }
            
            // Rule type analysis for debugging
            analyzeRuleTypes(filteredRules)

            // Apply rules and generate predictions
            val applicationStartTime = System.currentTimeMillis()
            RuleEngine.applyRulesARX(filteredRules, testSet, effectiveTrainingSet, effectiveValidationSet, Settings.TOP_K_OUTPUT, predictionWriter)

            val endTime = System.currentTimeMillis()
            val totalTime = endTime - startTime
            val applicationTime = endTime - applicationStartTime

            println("* Evaluated ${allRules.size} rules for ${testSet.size * 2} completion tasks")
            println("* Finished prediction in ${totalTime}ms (application: ${applicationTime}ms)")

            println("Prediction completed in ${applicationTime / 1000}s")
            println("Total time including loading: ${totalTime / 1000}s")
            println()
            
        }

        return predictionFiles
    }

    /**
     * Evaluate predictions and compute metrics
     */
    private fun evaluatePredictions(predictionFiles: List<String>, trainingSet: TripleSet, validationSet: TripleSet, testSet: TripleSet) {
        println("* Starting evaluation of ${predictionFiles.size} prediction file(s)")

        val hitsAtK = HitsAtK()
        hitsAtK.addFilterTripleSet(trainingSet)
        hitsAtK.addFilterTripleSet(validationSet)
        hitsAtK.addFilterTripleSet(testSet)

        for ((index, predictionFile) in predictionFiles.withIndex()) {
            println("\n--- Evaluating: $predictionFile ---")
            
            try {
                println("* Loading result set from: $predictionFile")
                val resultSet = ResultSet(predictionFile, true, Settings.TOP_K_OUTPUT)
                println("* Successfully loaded ${resultSet.triples.size} prediction results")
                
                // Reset metrics for each file
                hitsAtK.reset()
                
                // Compute scores
                var evaluatedTriples = 0
                var successfulEvaluations = 0
                
                println("* Starting evaluation of ${testSet.size} test triples...")
                
                for ((tripleIndex, triple) in testSet.withIndex()) {
                    if (tripleIndex % 10000 == 0 && tripleIndex > 0) {
                        println("* Evaluated $tripleIndex/${testSet.size} triples...")
                    }
                    
                    val tripleString = "${IdManager.getEntityString(triple.h)} ${IdManager.getRelationString(triple.r)} ${IdManager.getEntityString(triple.t)}"
                    
                    try {
                        val headCandidates = resultSet.getHeadCandidates(tripleString)
                        val tailCandidates = resultSet.getTailCandidates(tripleString)
                        
                        hitsAtK.evaluateHead(headCandidates, triple)
                        hitsAtK.evaluateTail(tailCandidates, triple)
                        successfulEvaluations++
                    } catch (e: Exception) {
                        // Skip this triple if it's not found in results
                        if (evaluatedTriples < 5) { // Only show first few errors
                            println("Warning: Could not find predictions for triple: $tripleString")
                        }
                    }
                    
                    evaluatedTriples++
                }

                // Print results
                println("\nEvaluation Results:")
                println("==================")
                println("Total test triples: ${testSet.size}")
                println("Successfully evaluated: $successfulEvaluations")
                println("Coverage: ${String.format("%.2f", (successfulEvaluations.toDouble() / testSet.size) * 100)}%")
                println()
                println("Metrics:")
                println("--------")
                println("Hits@1:  ${hitsAtK.getHitsAtK(0)}")
                println("Hits@3:  ${hitsAtK.getHitsAtK(2)}")
                println("Hits@10: ${hitsAtK.getHitsAtK(9)}")
                println("MRR:     ${hitsAtK.approxMRR}")
                
                if (predictionFiles.size > 1) {
                    println("Summary: ${hitsAtK.getHitsAtK(0)} | ${hitsAtK.getHitsAtK(2)} | ${hitsAtK.getHitsAtK(9)} | ${hitsAtK.approxMRR}")
                }

            } catch (e: Exception) {
                println("Error evaluating $predictionFile: ${e.message}")
                println("Stack trace:")
                e.printStackTrace()
                
                // Try to give more specific error information
                val file = File(predictionFile)
                if (!file.exists()) {
                    println("File does not exist: $predictionFile")
                } else {
                    println("File exists but has ${file.length()} bytes")
                    // Show first few lines to debug format issues
                    try {
                        file.bufferedReader().use { reader ->
                            println("First few lines of the file:")
                            repeat(5) {
                                val line = reader.readLine()
                                if (line != null) {
                                    println("  $it: $line")
                                } else {
                                    println("  $it: <null>")
                                    return@repeat
                                }
                            }
                        }
                    } catch (readError: Exception) {
                        println("Could not read file: ${readError.message}")
                    }
                }
            }
        }

        println("\n=== EVALUATION COMPLETE ===")
    }

    /**
     * Get processing paths for rules and output files
     */
    private fun getProcessingPaths(value: String?): Pair<String, String> {
        return if (value == null) {
            Pair(Settings.PATH_OUTPUT!!, Settings.PATH_RULES!!)
        } else {
            val outputPath = Settings.PATH_OUTPUT!!.replaceFirst("\\|.*\\|".toRegex(), value)
            val rulesPath = Settings.PATH_RULES!!.replaceFirst("\\|.*\\|".toRegex(), value)
            Pair(outputPath, rulesPath)
        }
    }

    /**
     * Filter rules by confidence threshold
     */
    private fun filterRulesByConfidence(rules: MutableList<Rule>): MutableList<Rule> {
        if (Settings.THRESHOLD_CONFIDENCE <= 0.0) {
            return rules
        }

        val filteredRules = mutableListOf<Rule>()
        for (rule in rules) {
            if (rule.confidence > Settings.THRESHOLD_CONFIDENCE) {
                filteredRules.add(rule)
            }
        }

        println("* Applied confidence threshold ${Settings.THRESHOLD_CONFIDENCE}: ${rules.size} → ${filteredRules.size} rules")
        return filteredRules
    }

    /**
     * Analyze rule types for performance debugging
     */
    private fun analyzeRuleTypes(rules: List<Rule>) {
        var cyclicCount = 0
        var acyclic1Count = 0
        var acyclic2Count = 0
        var otherCount = 0
        
        val bodySizeDistribution = mutableMapOf<Int, Int>()
        
        for (rule in rules) {
            when {
                rule is RuleCyclic -> cyclicCount++
                rule is RuleAcyclic && rule.isAcyclic1 -> acyclic1Count++
                rule is RuleAcyclic && rule.isAcyclic2 -> acyclic2Count++
                else -> otherCount++
            }
            
            bodySizeDistribution[rule.bodySize] = bodySizeDistribution.getOrDefault(rule.bodySize, 0) + 1
        }
        
        println("* Rule type distribution:")
        println("  - Cyclic: $cyclicCount")
        println("  - Acyclic1: $acyclic1Count") 
        println("  - Acyclic2: $acyclic2Count")
        println("  - Other: $otherCount")
        println("* Body size distribution: $bodySizeDistribution")
    }


    /**
     * Parse multi-processing path pattern
     * Format: "path|value1,value2,value3|"
     */
    fun getMultiProcessing(path: String): Array<String> {
        val tokens = path.split("\\|".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        return if (tokens.size < 2) {
            arrayOf() // Return empty array for single file processing
        } else {
            tokens[1].split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        }
    }

    /**
     * Filter rules by type (kept for compatibility but simplified)
     */
    private fun filterTSA(TSA: Array<String>, TSAindex: Int, rulesThresholded: MutableList<Rule>, rule: Rule) {
        when (TSA[TSAindex]) {
            "ALL" -> rulesThresholded.add(rule)
            "C-1" -> if (rule is RuleCyclic && rule.bodySize == 1) rulesThresholded.add(rule)
            "C-2" -> if (rule is RuleCyclic && rule.bodySize == 2) rulesThresholded.add(rule)
            "C-3" -> if (rule is RuleCyclic && rule.bodySize == 3) rulesThresholded.add(rule)
            "AC1-1" -> if (rule is RuleAcyclic && rule.isAcyclic1 && rule.bodySize == 1) rulesThresholded.add(rule)
            "AC1-2" -> if (rule is RuleAcyclic && rule.isAcyclic1 && rule.bodySize == 2) rulesThresholded.add(rule)
            "AC2-1" -> if (rule is RuleAcyclic && rule.isAcyclic2 && rule.bodySize == 1) rulesThresholded.add(rule)
            "AC2-2" -> if (rule is RuleAcyclic && rule.isAcyclic2 && rule.bodySize == 2) rulesThresholded.add(rule)
            "N-C-1" -> if (!(rule is RuleCyclic && rule.bodySize == 1)) rulesThresholded.add(rule)
            "N-C-2" -> if (!(rule is RuleCyclic && rule.bodySize == 2)) rulesThresholded.add(rule)
            "N-C-3" -> if (!(rule is RuleCyclic && rule.bodySize == 3)) rulesThresholded.add(rule)
            "N-AC1-1" -> if (!(rule is RuleAcyclic && rule.isAcyclic1 && rule.bodySize == 1)) rulesThresholded.add(rule)
            "N-AC1-2" -> if (!(rule is RuleAcyclic && rule.isAcyclic1 && rule.bodySize == 2)) rulesThresholded.add(rule)
            "N-AC2-1" -> if (!(rule is RuleAcyclic && rule.isAcyclic2 && rule.bodySize == 1)) rulesThresholded.add(rule)
            "N-AC2-2" -> if (!(rule is RuleAcyclic && rule.isAcyclic2 && rule.bodySize == 2)) rulesThresholded.add(rule)
        }
    }
}