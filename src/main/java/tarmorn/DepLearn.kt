package tarmorn

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import tarmorn.data.TripleSet
import tarmorn.eval.HitsAtK
import tarmorn.eval.ResultSet
import tarmorn.structure.TLearn.DepAtom
import tarmorn.structure.TLearn.Metric
import tarmorn.structure.TLearn.RuleParser
import tarmorn.data.MyTriple
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.io.PrintWriter
import java.nio.charset.StandardCharsets
import java.util.Locale
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

/**
 * DepLearn - Dependency-based learning algorithm
 * 
 * This class reads a TripleSet and rule file, builds indexes and converts rules to H2B2metric using DepAtom.
 * 
 * Key features:
 * 1. Builds indexes from TripleSet:
 *    - r2h2tSet: relation -> head -> tails mapping
 *    - r2instanceSet: relation -> set of (head, tail) pairs as Long
 *    - r2tSet: relation -> array of tail entities
 * 
 * 2. Reads rules from PATH_RULES (excludes complex rules with &&)
 *    Rule format: bodySize\tsupport\tconfidence\thead <= body1 body2 ...
 * 
 * 3. Converts rules to H2B2metric structure:
 *    - Uses DepAtom instead of MyAtom (no instance storage, direct index lookup)
 *    - H2B2metric: ConcurrentHashMap<DepAtom, ConcurrentHashMap<DepAtom, Metric>>
 *    - headSize is obtained directly from indexes (no need to store in DepAtom)
 * 
 * Usage:
 *   mvn compile
 *   mvn exec:java -Dexec.mainClass="tarmorn.DepLearn"
 * 
 * @see DepAtom for atom representation without instance storage
 * @see TLearn for comparison with the full learning algorithm
 */
object DepLearn {
    
    // Core data structures from TripleSet
    lateinit var ts: TripleSet
    lateinit var r2h2tSet: Map<Long, Map<Int, Set<Int>>>
    lateinit var r2instanceSet: Map<Long, Set<Long>>
    lateinit var r2tSet: Map<Long, IntArray>
    
    // Rule metric structure: head -> body -> ruleId
    val H2B2ID = ConcurrentHashMap<DepAtom, ConcurrentHashMap<DepAtom, Int>>()
    val ID2metric = ConcurrentHashMap<Int, Metric>()
    val depAdj2metric = ConcurrentHashMap<Int, ConcurrentHashMap<Int, Metric>>()
    val ruleId2HeadRelationId = ConcurrentHashMap<Int, Long>()
    
    // Statistics variables
    var totalRules = 0
    // Lift statistics for composition phase
    val unaryPositiveLift = java.util.concurrent.atomic.AtomicInteger(0)
    val unaryNegativeLift = java.util.concurrent.atomic.AtomicInteger(0)
    val binaryPositiveLift = java.util.concurrent.atomic.AtomicInteger(0)
    val binaryNegativeLift = java.util.concurrent.atomic.AtomicInteger(0)
    val mixPositiveLift = java.util.concurrent.atomic.AtomicInteger(0)
    val mixNegativeLift = java.util.concurrent.atomic.AtomicInteger(0)
    val thread0Attempts = java.util.concurrent.atomic.AtomicInteger(0)
    
    // Constants from TLearn
    const val MIN_SURPRISAL_LIFT = 0.05
    const val TOP_K_RULE_COMBO = 300
    private val FEATURE_KEYS = listOf(
        "num_rules",
        "w_1",
        "w_2",
        "w_3",
        "w_4",
        "w_5",
        "w25",
        "w50",
        "w75",
        "w_std",
        "num_neg_edges",
        "num_rules_with_neg_incoming",
        "num_rules_with_neg_outgoing",
        "max_neg_indegree",
        "mean_neg_indegree",
        "max_neg_outdegree",
        "num_neg_components",
        "num_neg_component",
        "largest_neg_component_size",
        "num_pos_edges",
        "num_rules_with_pos_edges",
        "num_pos_components",
        "largest_pos_component_size",
        "max_neg_outdegree_minus_indegree",
        "max_pos_outdegree",
        "outdegree_of_top_rule",
        "indegree_of_top_rule",
        "score_noisyor",
        "score_maxplus",
        "score_expdecay_tau_0.25",
        "score_expdecay_tau_0.5",
        "score_expdecay_tau_1",
        "score_expdecay_tau_2",
        "score_expdecay_tau_4",
        "num_lift_pos_gt_1",
        "num_lift_neg_gt_0.5",
        "max_lift_neg",
        "max_lift_pos",
        "sum_lift_pos",
        "sum_lift_neg",
        "sum_top3_lift_pos",
        "sum_top3_lift_neg",
    )

    data class TripleKey(val head: String, val relation: String, val tail: String)
    data class AppliedItem(
        val ifHead: Int,
        val relation: String,
        val constant: String,
        val candidate: String,
        val ruleIds: IntArray
    )

    data class QueryKey(
        val ifHead: Int,
        val relation: String,
        val constant: String
    )

    private fun format5(value: Double): String {
        val formatted = String.format(java.util.Locale.US, "%.5f", value)
        return formatted.trimEnd('0').trimEnd('.')
    }

    private fun packBinaryInstance(head: Int, tail: Int): Long {
        return (head.toLong() shl 32) or (tail.toLong() and 0xFFFFFFFFL)
    }

    private fun storeDependency(
        ruleId1: Int,
        ruleId2: Int,
        metric: Metric,
        metric1: Metric,
        metric2: Metric,
        positiveCounter: java.util.concurrent.atomic.AtomicInteger,
        negativeCounter: java.util.concurrent.atomic.AtomicInteger
    ) {
        val lift = metric.surprisal - metric1.surprisal - metric2.surprisal
        val maxSurprisal = maxOf(metric1.surprisal, metric2.surprisal)
        if (lift <= 0 && metric.surprisal >= maxSurprisal) {
            return
        }

        val conf1 = metric1.adjustedConfidence
        val conf2 = metric2.adjustedConfidence
        if (conf1 == conf2) {
            return
        }

        val srcId: Int
        val dstId: Int
        if (conf1 > conf2) {
            srcId = ruleId1
            dstId = ruleId2
        } else {
            srcId = ruleId2
            dstId = ruleId1
        }

        metric.lift = if (lift > 0) lift else metric.surprisal - maxSurprisal
        val inner = depAdj2metric.computeIfAbsent(srcId) { ConcurrentHashMap() }
        inner[dstId] = metric

        if (metric.lift > 0) positiveCounter.incrementAndGet()
        else negativeCounter.incrementAndGet()
    }

    private fun escapeJson(value: String): String {
        return value.replace("\\", "\\\\").replace("\"", "\\\"")
    }

    private fun buildBodyList(bodyMap: ConcurrentHashMap<DepAtom, Int>): List<Triple<DepAtom, Int, Metric>> {
        return bodyMap.entries
            .mapNotNull { entry ->
                val metric = ID2metric[entry.value] ?: return@mapNotNull null
                // metric.surprisal >= MIN_SURPRISAL_LIFT &&  不限制最小值
                if (metric.surprisal < Settings.MAX_SURPRISAL && metric.support >= Settings.MIN_SUPP) {
                    Triple(entry.key, entry.value, metric)
                } else null
            }
            .sortedByDescending { it.third.confidence }
            .take(TOP_K_RULE_COMBO)
    }

    private fun precomputeBodyLists(
        threadPool: java.util.concurrent.ExecutorService
    ): ConcurrentHashMap<DepAtom, List<Triple<DepAtom, Int, Metric>>> {
        val bodyListMap = ConcurrentHashMap<DepAtom, List<Triple<DepAtom, Int, Metric>>>()
        val futures = H2B2ID.entries.map { (headAtom, bodyMap) ->
            threadPool.submit {
                val headSupport = getAtomSize(headAtom)
                if (headSupport < Settings.MIN_SUPP) {
                    bodyListMap[headAtom] = emptyList()
                    return@submit
                }
                val bodyList = buildBodyList(bodyMap)
                bodyListMap[headAtom] = bodyList

                bodyList.forEach { (bodyAtom, _, _) ->
                    if (bodyAtom.isBinary && !bodyAtom.isL1Atom && !bodyAtom.hasBeenSampled) {
                        synchronized(bodyAtom) {
                            if (!bodyAtom.hasBeenSampled && !bodyAtom.isL1Atom) {
                                bodyAtom.sampleBinaryInstancesEDIS()
                            }
                        }
                    }
                }
            }
        }
        futures.forEach { it.get() }
        return bodyListMap
    }

    
    /**
     * Main entry point
     */
    @JvmStatic
    fun main(args: Array<String>) {
        Settings.load()
        println("DepLearn - Dependency-based learning algorithm")
        println("=".repeat(60))
        
        val startTime = System.currentTimeMillis()
        
        // Step 1: Load triple set and build indexes
        println("\n=== Step 1: Loading Triple Set ===")
        println("Loading triple set from: ${Settings.PATH_TRAINING}")
        loadTripleSet()

        readRules(Settings.PATH_RULES)

        // saveMetricToJson(
        //     metricMap = H2B2ID,
        //     outputPath = Settings.PATH_H2B2metric,
        //     appendMode = false,
        //     isFormulaMap = false
        // )

        try {
            compositionPhase()
        } catch (e: Exception) {
            println("Error during composition phase: ${e.message}")
            e.printStackTrace()
        }
        printStatistics()

        saveDependencyToFile(Settings.PATH_DEPENDENCY)
        buildDependencyGraphFromAppliedRules(Settings.PATH_APPLIED_RULES, Settings.PATH_DEPENDENCY_GRAPH, Settings.PATH_TEST)
        buildDependencyGraphFromAppliedRules(Settings.PATH_APPLIED_RULES_VALID, Settings.PATH_DEPENDENCY_GRAPH_VALID, Settings.PATH_VALID)

        val endTime = System.currentTimeMillis()
        val elapsedSeconds = (endTime - startTime) / 1000.0
        
        println("\n" + "=".repeat(60))
        println("DepLearn completed in ${"%.2f".format(elapsedSeconds)}s")
    }
    
    /**
     * Load triple set and build indexes
     */
    private fun loadTripleSet() {
        ts = TripleSet(Settings.PATH_TRAINING, true)
        
        // Build r2h2tSet: relation -> head -> tails
        r2h2tSet = ts.r2h2tSet
        
        // Build r2instanceSet: relation -> set of (h,t) pairs as Long
        r2instanceSet = r2h2tSet.mapValues { (_, h2tSet) ->
            h2tSet.flatMap { (head, tails) ->
                tails.map { tail -> 
                    (head.toLong() shl 32) or (tail.toLong() and 0xFFFFFFFFL) 
                }
            }.toSet()
        }
        
        // Build r2tSet: relation -> array of tail entities (for inverse relation)
        r2tSet = r2h2tSet.keys.associateWith { r ->
            val inv = RelationPath.getInverseRelation(r)
            val keys = r2h2tSet[inv]?.keys ?: emptySet()
            keys.toIntArray()
        }
        
        println("Loaded triple set: ${ts.size} triples")
        println("Relations: ${r2h2tSet.keys.size}")
        println("Indexed r2h2tSet, r2instanceSet, r2tSet")
    }
    
    /**
     * Read rules from file and convert to H2B2metric
     * Rule format: bodySize\tsupport\tconfidence\thead <= body1 body2 ...
     * We only process rules without && (no complex rules)
     */
    private fun readRules(filepath: String) {
        println("\n=== Step 2: Reading Rules ===")
        println("Reading rules from: $filepath")
        val file = File(filepath)
        if (!file.exists()) {
            println("Warning: Rule file not found: $filepath")
            return
        }
        
        val startTime = System.currentTimeMillis()
        
        // First pass: read all lines into memory
        val allLines = mutableListOf<Pair<Int, String>>()
        BufferedReader(InputStreamReader(FileInputStream(file), StandardCharsets.UTF_8)).use { reader ->
            var lineNumber = 0
            reader.forEachLine { line ->
                lineNumber++
                if (line.isNotBlank() && !line.startsWith("#")) {
                    val tokens = line.split("\t")
                    if (tokens.size >= 4) {
                        allLines.add(lineNumber to line)
                    }
                }
            }
        }
        
        println("Read ${allLines.size} valid rules, starting parallel parsing...")
        
        // Concurrent counters
        val parsedRules = java.util.concurrent.atomic.AtomicInteger(0)
        val errors = java.util.concurrent.atomic.AtomicInteger(0)
        
        // Create thread pool
        val threadPool = java.util.concurrent.Executors.newFixedThreadPool(Settings.WORKER_THREADS)
        
        try {
            // Chunk the lines for better load balancing
            val chunkSize = maxOf(1000, allLines.size / (Settings.WORKER_THREADS * 4))
            val chunks = allLines.chunked(chunkSize)
            
            val futures = chunks.map { chunk ->
                threadPool.submit {
                    chunk.forEach { (lineNumber, line) ->
                        try {
                            parseAndAddRule(line, lineNumber)
                            val count = parsedRules.incrementAndGet()
                            if (count % 100000 == 0) {
                                println("Parsed $count/${allLines.size} rules...")
                            }
                        } catch (e: Exception) {
                            val errorCount = errors.incrementAndGet()
                            if (errorCount <= 10) {
                                synchronized(System.out) {
                                    println("Error parsing rule: ${e.message}")
                                    println("  Line: $line")
                                }
                            }
                        }
                    }
                }
            }
            
            // Wait for all tasks to complete
            futures.forEach { it.get() }
            
        } finally {
            threadPool.shutdown()
        }
        
        val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
        println("\nRule loading completed:")
        println("  Total lines processed: ${allLines.size}")
        println("  Successfully parsed: ${parsedRules.get()}")
        println("  Errors: ${errors.get()}")
        println("  Time: %.2f seconds".format(elapsed))
        println("  Speed: %.0f rules/sec".format(parsedRules.get() / elapsed))
    }

    fun setH2B2ID(headAtom: DepAtom, bodyAtom: DepAtom, ruleId: Int, metric: Metric) {
        val B2id = H2B2ID.computeIfAbsent(headAtom) { ConcurrentHashMap() }
        B2id[bodyAtom] = ruleId
        ID2metric[ruleId] = metric
        totalRules++
    }
    
    /**
     * Parse a single rule line and add to H2B2metric
     * Format: bodySize\tsupport\tconfidence\thead <= body
     * 
     * Note: body is a relation path string like "r1*r2(const)" or "r1*INVERSE_r2"
     * It will be parsed as a single DepAtom with encoded relationId
     */
    private fun parseAndAddRule(line: String, ruleId: Int) {
        val tokens = line.split("\t")
        if (tokens.size < 4) {
            throw IllegalArgumentException("Invalid rule format: expected at least 4 tokens")
        }
        
        val bodySize = tokens[0].toInt()
        val support = tokens[1].toDouble()
        val confidence = tokens[2].toDouble()
        val ruleString = tokens[3]

        if (ruleString.contains("&&")) {
            return
        }

        val (headAtom, bodyAtom) = RuleParser.parseRule(ruleString)
        

        val headSize = getAtomSize(headAtom)
        val metric = Metric(support, headSize, bodySize)

        if (bodyAtom != null) {
            require(headAtom.isBinary == bodyAtom.isBinary) {
                "Head/body arity mismatch: head=$headAtom, body=$bodyAtom"
            }
            setH2B2ID(headAtom, bodyAtom, ruleId, metric)
            ruleId2HeadRelationId[ruleId] = headAtom.relationId
        }
    }
    
    /**
     * Get the size (support) of an atom from indexes
     */
    private fun getAtomSize(atom: DepAtom): Int {
        return when {
            // Binary atom: relationId with Y
            atom.entityId == IdManager.getYId() -> {
                r2instanceSet[atom.relationId]?.size ?: 0
            }
            // Loop atom: relationId with X
            atom.entityId == IdManager.getXId() -> {
                ts.r2loopSet[atom.relationId]?.size ?: 0
            }
            // Existence atom: relationId with *
            atom.entityId == 0 -> {
                r2h2tSet[atom.relationId]?.keys?.size ?: 0
            }
            // Constant atom: relationId with specific entity
            else -> {
                r2h2tSet[atom.relationId]?.get(atom.entityId)?.size ?: 0
            }
        }
    }
    
    /**
     * Composition Phase - combine frequent atom sets using Eclat algorithm
     * Called after reading rules, builds formulas based on H2B2metric
     */
    fun compositionPhase() {
        println("\n=== Composition Phase ===")
        println("Starting Composition Phase...")
        
        val processedTasks = java.util.concurrent.atomic.AtomicInteger(0)
        val polledTasks = java.util.concurrent.atomic.AtomicInteger(0)
        val taskErrors = java.util.concurrent.atomic.AtomicInteger(0)
        val threadPool = java.util.concurrent.Executors.newFixedThreadPool(Settings.WORKER_THREADS)
        val compositionActiveThreadCount = java.util.concurrent.atomic.AtomicInteger(0)
        val compositionThreadMonitorLock = Object()
        
        try {
            val bodyListMap = precomputeBodyLists(threadPool)
            val workQueue = ConcurrentLinkedQueue<Pair<DepAtom, Int>>()
            var totalTasks = 0
            bodyListMap.forEach { (headAtom, bodyList) ->
                for (i in bodyList.indices) {
                    workQueue.add(headAtom to i)
                    totalTasks++
                }
            }
            println("Composition tasks queued: $totalTasks")

            val futures = (0 until Settings.WORKER_THREADS).map { _ ->
                threadPool.submit {
                    compositionActiveThreadCount.incrementAndGet()
                    try {
                        while (true) {
                            val task = workQueue.poll() ?: break
                            polledTasks.incrementAndGet()
                            val headAtom = task.first
                            val i = task.second
                            val bodyList = bodyListMap[headAtom].orEmpty()
                            if (bodyList.isEmpty()) continue

                            try {
                                if (headAtom.isBinary) {
                                    processBinaryHeadAtom(headAtom, bodyList, i)
                                } else {
                                    val binaryHeadAtom = headAtom.getBinaryAtom()
                                    val binaryBodyList = bodyListMap[binaryHeadAtom].orEmpty()
                                    processUnaryHeadAtom(headAtom, bodyList, binaryBodyList, i)
                                }
                            } catch (e: Exception) {
                                val errCnt = taskErrors.incrementAndGet()
                                if (errCnt <= 10) {
                                    synchronized(System.out) {
                                        println("Composition task error (#$errCnt): ${e.message}")
                                        println("  headAtom=$headAtom, index=$i, bodyListSize=${bodyList.size}")
                                    }
                                }
                                continue
                            }

                            val cnt = processedTasks.incrementAndGet()
                            if (cnt % 10000 == 0) {
                                println("Processed $cnt/$totalTasks tasks...")
                            }
                        }
                    } finally {
                        val activeCount = compositionActiveThreadCount.decrementAndGet()
                        synchronized(compositionThreadMonitorLock) {
                            compositionThreadMonitorLock.notifyAll()
                        }
                    }
                }
            }

            // Monitor thread activity
            var lastActiveCount = 0
            while (true) {
                val activeCount: Int
                synchronized(compositionThreadMonitorLock) {
                    // Wait for thread count changes
                    while (compositionActiveThreadCount.get() == lastActiveCount && !futures.all { it.isDone }) {
                        compositionThreadMonitorLock.wait(1000)
                    }
                    activeCount = compositionActiveThreadCount.get()
                    lastActiveCount = activeCount
                }
                
                if (futures.all { it.isDone }) {
                    println("All composition tasks completed")
                    break
                }
                
                if (activeCount > 0 && activeCount < Settings.WORKER_THREADS - 5) {
                    println("Composition thread count: $activeCount/${Settings.WORKER_THREADS} active")
                }
                
                if (activeCount < 4 && activeCount > 0) {
                    println("FORCING SHUTDOWN: Less than 4 threads remaining in composition phase")
                    futures.forEach { it.cancel(true) }
                    threadPool.shutdownNow()
                    break
                }
            }
            
        } catch (e: Exception) {
            println("Error in composition phase monitoring: ${e.message}")
            threadPool.shutdownNow()
        } finally {
            threadPool.shutdown()
            threadPool.awaitTermination(1, java.util.concurrent.TimeUnit.HOURS)
        }
        
        println("Composition Phase completed. Total dependencies: ${depAdj2metric.values.sumOf { it.size }}")
        println("Composition tasks polled: ${polledTasks.get()}, processed: ${processedTasks.get()}, errors: ${taskErrors.get()}")
    }

    private fun buildDependencyGraphFromAppliedRules(
        appliedRulesPath: String,
        outputPath: String,
        labelPath: String
    ) {
        val appliedRulesFile = File(appliedRulesPath)
        if (!appliedRulesFile.exists()) {
            println("Skip dependency_graph.csv: applied_rules not found: $appliedRulesPath")
            return
        }

        println("\n=== Dependency Graph Feature Extraction ===")
        println("Reading applied_rules: $appliedRulesPath")
        println("Output CSV: $outputPath")

        val gson = Gson()
        val type = object : TypeToken<Map<String, Any>>() {}.type
        val root: Map<String, Any> = appliedRulesFile.reader().use { reader ->
            gson.fromJson(reader, type)
        }

        val items = parseAppliedRules(root)
        println("Total applied rule entries: ${items.size}")

        val testSet = loadTestSet(labelPath)
        println("Loaded label set: ${testSet.size} triples")

        val depAdj = depAdj2metric

        val outputFile = File(outputPath)
        outputFile.parentFile?.mkdirs()
        PrintWriter(outputFile, StandardCharsets.UTF_8).use { writer ->
            writer.println(csvHeader())

            val pool = Executors.newFixedThreadPool(Settings.WORKER_THREADS)
            val lock = Any()
            val processed = AtomicInteger(0)

            val itemsByQuery = items.groupBy { QueryKey(it.ifHead, it.relation, it.constant) }
            val queryEntries = itemsByQuery.entries.toList()

            try {
                val chunkSize = 200
                queryEntries.chunked(chunkSize)
                    .forEach { chunk ->
                        pool.submit {
                            val localBuilder = StringBuilder()
                            for ((_, groupItems) in chunk) {
                                val scoredItems = groupItems.map { item ->
                                    item to computeScoreNoisyor(item.ruleIds)
                                }
                                val topK = scoredItems
                                    .sortedByDescending { it.second }
                                    // .take(Settings.CANDIDATE_TOPK)

                                for ((item, _) in topK) {
                                    val label = if (item.ifHead == 1) {
                                        testSet.contains(TripleKey(item.candidate, item.relation, item.constant))
                                    } else {
                                        testSet.contains(TripleKey(item.constant, item.relation, item.candidate))
                                    }

                                    val features = computeFeatures(item.ruleIds, depAdj)
                                    val line = buildCsvLine(item, label, features)
                                    localBuilder.append(line).append('\n')

                                    val count = processed.incrementAndGet()
                                    if (count % 10000 == 0) {
                                        println("Processed $count/${items.size} applied entries...")
                                    }
                                }
                            }
                            synchronized(lock) {
                                writer.print(localBuilder.toString())
                            }
                        }
                    }
            } finally {
                pool.shutdown()
                while (!pool.isTerminated) {
                    Thread.sleep(200)
                }
            }
        }

        println("Dependency graph CSV saved: $outputPath")
    }

    private fun parseAppliedRules(root: Map<String, Any>): List<AppliedItem> {
        val items = mutableListOf<AppliedItem>()
        val sections = listOf("head" to 1, "tail" to 0)
        for ((sectionKey, ifHead) in sections) {
            val section = root[sectionKey] as? Map<*, *> ?: continue
            for ((relationKey, constantObj) in section) {
                val relation = relationKey?.toString() ?: continue
                val constantMap = constantObj as? Map<*, *> ?: continue
                for ((constantKey, candidateObj) in constantMap) {
                    val constant = constantKey?.toString() ?: continue
                    val candidateMap = candidateObj as? Map<*, *> ?: continue
                    for ((candidateKey, ruleObj) in candidateMap) {
                        val candidate = candidateKey?.toString() ?: continue
                        val ruleList = ruleObj as? List<*> ?: emptyList<Any>()
                        val ruleIds = ruleList.mapNotNull { rid ->
                            when (rid) {
                                is Number -> rid.toInt()
                                is String -> rid.toIntOrNull()
                                else -> null
                            }
                        }.toIntArray()
                        items.add(AppliedItem(ifHead, relation, constant, candidate, ruleIds))
                    }
                }
            }
        }
        return items
    }

    private fun loadTestSet(path: String): HashSet<TripleKey> {
        val set = HashSet<TripleKey>()
        val file = File(path)
        if (!file.exists()) return set
        BufferedReader(InputStreamReader(FileInputStream(file), StandardCharsets.UTF_8)).use { reader ->
            reader.forEachLine { line ->
                val trimmed = line.trim()
                if (trimmed.isEmpty()) return@forEachLine
                val parts = trimmed.split("\t", " ")
                    .filter { it.isNotEmpty() }
                if (parts.size < 3) return@forEachLine
                set.add(TripleKey(parts[0], parts[1], parts[2]))
            }
        }
        return set
    }

    private fun computeScoreNoisyor(ruleIds: IntArray): Double {
        val validRuleIds = ruleIds.filter { ID2metric.containsKey(it) }
        if (validRuleIds.isEmpty()) return 0.0

        val numUnseen = Settings.UNSEEN_NEGATIVE_EXAMPLES.toDouble()
        var sumSurprisal = 0.0
        for (rid in validRuleIds) {
            val metric = ID2metric[rid] ?: continue
            val bodySize = metric.bodySize.toDouble()
            val denom = metric.support + numUnseen
            val ratio = if (denom > 0) bodySize / denom else 0.0
            val clipped = when {
                ratio <= 0.0 -> 0.0
                ratio >= 1.0 -> 1.0 - 1e-12
                else -> ratio
            }
            val surprisal = if (clipped <= 0.0) 0.0 else -kotlin.math.ln(1.0 - clipped)
            sumSurprisal += surprisal
        }
        return if (sumSurprisal > 0) 1.0 - kotlin.math.exp(-sumSurprisal) else 0.0
    }


    private fun computeFeatures(ruleIds: IntArray, depAdj: Map<Int, Map<Int, Metric>>): Map<String, Any> {
        val validRuleIds = ruleIds.filter { ID2metric.containsKey(it) }
        if (validRuleIds.isEmpty()) {
            return emptyFeatureMap()
        }

        val numUnseen = Settings.UNSEEN_NEGATIVE_EXAMPLES.toDouble()
        val surprisalList = validRuleIds.mapNotNull { rid ->
            val metric = ID2metric[rid] ?: return@mapNotNull null
            val bodySize = metric.bodySize.toDouble()
            val denom = metric.support + numUnseen
            val ratio = if (denom > 0) bodySize / denom else 0.0
            val clipped = when {
                ratio <= 0.0 -> 0.0
                ratio >= 1.0 -> 1.0 - 1e-12
                else -> ratio
            }
            val surprisal = if (clipped <= 0.0) 0.0 else -kotlin.math.ln(1.0 - clipped)
            rid to surprisal
        }.sortedByDescending { it.second }

        val sortedSurprisals = surprisalList.map { it.second }
        val wValues = (sortedSurprisals + List(5) { 0.0 }).take(5)
        val w1 = wValues[0]
        val w2 = wValues[1]
        val w3 = wValues[2]
        val w4 = wValues[3]
        val w5 = wValues[4]

        val w25 = percentile(sortedSurprisals, 0.25)
        val w50 = percentile(sortedSurprisals, 0.50)
        val w75 = percentile(sortedSurprisals, 0.75)
        val wStd = stdDev(sortedSurprisals)

        val ruleSet = validRuleIds.toSet()
        val negEdges = mutableListOf<Pair<Int, Int>>()
        val posEdges = mutableListOf<Pair<Int, Int>>()
        val negIn = HashMap<Int, Int>()
        val negOut = HashMap<Int, Int>()
        val posIn = HashMap<Int, Int>()
        val posOut = HashMap<Int, Int>()
        val posLifts = mutableListOf<Double>()
        val negLifts = mutableListOf<Double>()

        for (rid in ruleSet) {
            negIn[rid] = 0
            negOut[rid] = 0
            posIn[rid] = 0
            posOut[rid] = 0
        }

        for (src in ruleSet) {
            val dstMap = depAdj[src] ?: continue
            for ((dst, metric) in dstMap) {
                if (!ruleSet.contains(dst)) continue
                val lift = metric.lift
                if (lift < 0) {
                    negEdges.add(src to dst)
                    negOut[src] = (negOut[src] ?: 0) + 1
                    negIn[dst] = (negIn[dst] ?: 0) + 1
                    negLifts.add(lift)
                } else {
                    posEdges.add(src to dst)
                    posOut[src] = (posOut[src] ?: 0) + 1
                    posIn[dst] = (posIn[dst] ?: 0) + 1
                    posLifts.add(lift)
                }
            }
        }

        val numNegEdges = negEdges.size
        val numPosEdges = posEdges.size
        val numRulesWithNegIncoming = negIn.values.count { it > 0 }
        val numRulesWithNegOutgoing = negOut.values.count { it > 0 }
        val maxNegIndegree = negIn.values.maxOrNull() ?: 0
        val meanNegIndegree = if (negIn.isNotEmpty()) negIn.values.sum().toDouble() / negIn.size else 0.0
        val maxNegOutdegree = negOut.values.maxOrNull() ?: 0
        val numRulesWithPosEdges = ruleSet.count { (posIn[it] ?: 0) + (posOut[it] ?: 0) > 0 }

        val negNodes = negEdges.flatMap { listOf(it.first, it.second) }.toSet()
        val posNodes = posEdges.flatMap { listOf(it.first, it.second) }.toSet()
        val negComponents = buildComponents(negNodes, negEdges)
        val posComponents = buildComponents(posNodes, posEdges)

        val maxNegOutdegreeMinusIndegree = ruleSet.maxOfOrNull { (negOut[it] ?: 0) - (negIn[it] ?: 0) } ?: 0
        val maxPosOutdegree = posOut.values.maxOrNull() ?: 0

        val topRuleId = surprisalList.first().first
        val outdegreeTopRule = (posOut[topRuleId] ?: 0) + (negOut[topRuleId] ?: 0)
        val indegreeTopRule = (posIn[topRuleId] ?: 0) + (negIn[topRuleId] ?: 0)

        val sumSurprisal = sortedSurprisals.sum()
        val scoreNoisyor = if (sumSurprisal > 0) 1.0 - kotlin.math.exp(-sumSurprisal) else 0.0
        val scoreMaxplus = w1

        fun expDecayScore(tau: Double): Double {
            return sortedSurprisals.mapIndexed { idx, s -> s * kotlin.math.exp(-tau * idx) }.sum()
        }

        val scoreTau025 = expDecayScore(0.25)
        val scoreTau05 = expDecayScore(0.5)
        val scoreTau1 = expDecayScore(1.0)
        val scoreTau2 = expDecayScore(2.0)
        val scoreTau4 = expDecayScore(4.0)

        val numLiftPosGt1 = posLifts.count { kotlin.math.abs(it) >= 1.0 }
        val numLiftNegGt05 = negLifts.count { kotlin.math.abs(it) >= 0.5 }
        val maxLiftPos = posLifts.maxOrNull() ?: 0.0
        val maxLiftNeg = negLifts.maxOfOrNull { kotlin.math.abs(it) } ?: 0.0
        val sumLiftPos = posLifts.sum()
        val sumLiftNeg = negLifts.sum()
        val sumTop3LiftPos = posLifts.sortedDescending().take(3).sum()
        val sumTop3LiftNeg = negLifts.map { kotlin.math.abs(it) }.sortedDescending().take(3).sum()

        return mapOf(
            "num_rules" to validRuleIds.size,
            "w_1" to w1,
            "w_2" to w2,
            "w_3" to w3,
            "w_4" to w4,
            "w_5" to w5,
            "w25" to w25,
            "w50" to w50,
            "w75" to w75,
            "w_std" to wStd,
            "num_neg_edges" to numNegEdges,
            "num_rules_with_neg_incoming" to numRulesWithNegIncoming,
            "num_rules_with_neg_outgoing" to numRulesWithNegOutgoing,
            "max_neg_indegree" to maxNegIndegree,
            "mean_neg_indegree" to meanNegIndegree,
            "max_neg_outdegree" to maxNegOutdegree,
            "num_neg_components" to negComponents.first,
            "num_neg_component" to negComponents.first,
            "largest_neg_component_size" to negComponents.second,
            "num_pos_edges" to numPosEdges,
            "num_rules_with_pos_edges" to numRulesWithPosEdges,
            "num_pos_components" to posComponents.first,
            "largest_pos_component_size" to posComponents.second,
            "max_neg_outdegree_minus_indegree" to maxNegOutdegreeMinusIndegree,
            "max_pos_outdegree" to maxPosOutdegree,
            "outdegree_of_top_rule" to outdegreeTopRule,
            "indegree_of_top_rule" to indegreeTopRule,
            "score_noisyor" to scoreNoisyor,
            "score_maxplus" to scoreMaxplus,
            "score_expdecay_tau_0.25" to scoreTau025,
            "score_expdecay_tau_0.5" to scoreTau05,
            "score_expdecay_tau_1" to scoreTau1,
            "score_expdecay_tau_2" to scoreTau2,
            "score_expdecay_tau_4" to scoreTau4,
            "num_lift_pos_gt_1" to numLiftPosGt1,
            "num_lift_neg_gt_0.5" to numLiftNegGt05,
            "max_lift_neg" to maxLiftNeg,
            "max_lift_pos" to maxLiftPos,
            "sum_lift_pos" to sumLiftPos,
            "sum_lift_neg" to sumLiftNeg,
            "sum_top3_lift_pos" to sumTop3LiftPos,
            "sum_top3_lift_neg" to sumTop3LiftNeg,
        )
    }

    private fun emptyFeatureMap(): Map<String, Any> {
        return mapOf(
            "num_rules" to 0,
            "w_1" to 0.0,
            "w_2" to 0.0,
            "w_3" to 0.0,
            "w_4" to 0.0,
            "w_5" to 0.0,
            "w25" to 0.0,
            "w50" to 0.0,
            "w75" to 0.0,
            "w_std" to 0.0,
            "num_neg_edges" to 0,
            "num_rules_with_neg_incoming" to 0,
            "num_rules_with_neg_outgoing" to 0,
            "max_neg_indegree" to 0,
            "mean_neg_indegree" to 0.0,
            "max_neg_outdegree" to 0,
            "num_neg_components" to 0,
            "num_neg_component" to 0,
            "largest_neg_component_size" to 0,
            "num_pos_edges" to 0,
            "num_rules_with_pos_edges" to 0,
            "num_pos_components" to 0,
            "largest_pos_component_size" to 0,
            "max_neg_outdegree_minus_indegree" to 0,
            "max_pos_outdegree" to 0,
            "outdegree_of_top_rule" to 0,
            "indegree_of_top_rule" to 0,
            "score_noisyor" to 0.0,
            "score_maxplus" to 0.0,
            "score_expdecay_tau_0.25" to 0.0,
            "score_expdecay_tau_0.5" to 0.0,
            "score_expdecay_tau_1" to 0.0,
            "score_expdecay_tau_2" to 0.0,
            "score_expdecay_tau_4" to 0.0,
            "num_lift_pos_gt_1" to 0,
            "num_lift_neg_gt_0.5" to 0,
            "max_lift_neg" to 0.0,
            "max_lift_pos" to 0.0,
            "sum_lift_pos" to 0.0,
            "sum_lift_neg" to 0.0,
            "sum_top3_lift_pos" to 0.0,
            "sum_top3_lift_neg" to 0.0,
        )
    }

    private fun percentile(values: List<Double>, p: Double): Double {
        if (values.isEmpty()) return 0.0
        val idx = ((values.size - 1) * p).toInt()
        return values[idx]
    }

    private fun stdDev(values: List<Double>): Double {
        if (values.size <= 1) return 0.0
        val mean = values.sum() / values.size
        val variance = values.sumOf { (it - mean) * (it - mean) } / values.size
        return kotlin.math.sqrt(variance)
    }

    private fun buildComponents(nodes: Set<Int>, edges: List<Pair<Int, Int>>): Pair<Int, Int> {
        if (edges.isEmpty()) return 0 to 0
        val adj = HashMap<Int, MutableSet<Int>>()
        for (n in nodes) {
            adj[n] = mutableSetOf()
        }
        for ((u, v) in edges) {
            adj.getOrPut(u) { mutableSetOf() }.add(v)
            adj.getOrPut(v) { mutableSetOf() }.add(u)
        }
        val visited = HashSet<Int>()
        var numComponents = 0
        var largestSize = 0
        for (n in adj.keys) {
            if (visited.contains(n)) continue
            val stack = ArrayDeque<Int>()
            stack.add(n)
            visited.add(n)
            var size = 0
            while (stack.isNotEmpty()) {
                val cur = stack.removeLast()
                size++
                for (next in adj[cur].orEmpty()) {
                    if (!visited.contains(next)) {
                        visited.add(next)
                        stack.add(next)
                    }
                }
            }
            numComponents++
            if (size > largestSize) largestSize = size
        }
        return numComponents to largestSize
    }

    private fun csvHeader(): String {
        return listOf(
            "relation",
            "constant",
            "candidate",
            "label",
            "if_head",
        ).plus(FEATURE_KEYS).joinToString(",")
    }

    private fun buildCsvLine(item: AppliedItem, label: Boolean, features: Map<String, Any>): String {
        val values = mutableListOf<String>()
        values.add(escapeCsv(item.relation))
        values.add(escapeCsv(item.constant))
        values.add(escapeCsv(item.candidate))
        values.add(if (label) "1" else "0")
        values.add(item.ifHead.toString())

        for (key in FEATURE_KEYS) {
            val value = features[key]
            values.add(formatCsvValue(value))
        }
        return values.joinToString(",")
    }

    private fun formatCsvValue(value: Any?): String {
        return when (value) {
            null -> "0"
            is Int -> value.toString()
            is Long -> value.toString()
            is Double -> String.format(Locale.US, "%.6f", value)
            is Float -> String.format(Locale.US, "%.6f", value.toDouble())
            is Boolean -> if (value) "1" else "0"
            else -> escapeCsv(value.toString())
        }
    }

    private fun escapeCsv(value: String): String {
        val needsQuotes = value.contains(",") || value.contains("\n") || value.contains("\"")
        if (!needsQuotes) return value
        val escaped = value.replace("\"", "\"\"")
        return "\"$escaped\""
    }
    
    /**
     * Process single binary headAtom, perform pairwise combination of bodyAtoms
     * Uses dynamic sampling strategy to handle large instance sets
     */
    private fun processBinaryHeadAtom(
        headAtom: DepAtom,
        bodyList: List<Triple<DepAtom, Int, Metric>>,
        i: Int
    ) {
        if (bodyList.size < 2) return  // Need at least 2 bodyAtoms to combine
        if (i !in bodyList.indices) return
        
        // 获取当前线程ID，用于控制日志输出（仅线程0输出详细日志）
        val threadId = Thread.currentThread().id % Settings.WORKER_THREADS
        val shouldDebug = (threadId == 0L) && thread0Attempts.incrementAndGet() <= 10
        
        var pairCount = 0
        val headInstances = headAtom.getBinaryInstances()

        // Pairwise combination with dynamic sampling (fixed i)
        val (B1, ruleId1, metric1) = bodyList[i]
        var S_H1_size = B1.instances.count { it in headInstances }
        val initialB1Size = B1.instances.size

        // Sample B1 until S_H1.size >= MIN_SUPP or exhausted
        // 只对非L1原子进行采样
        if (S_H1_size < Settings.MIN_SUPP && !B1.isL1Atom && !B1.samplingExhausted) {
            synchronized(B1) {
                S_H1_size = B1.instances.count { it in headInstances }
                while (S_H1_size < Settings.MIN_SUPP && !B1.isL1Atom && !B1.samplingExhausted) {
                    val newInstances = B1.sampleBinaryInstancesEDIS()
                    // 只检查新采样的实例
                    val newMatchCount = newInstances.count { it in headInstances }
                    S_H1_size += newMatchCount
                    if (shouldDebug)
                    println("\t[Thread-$threadId] ${B1} sampling round ${B1.samplingRound}: " +
                            "new=${newInstances.size}, total=${B1.instances.size}, " +
                            "S_H1=$S_H1_size, exhausted=${B1.samplingExhausted}")
                }
            }
        }
        if (shouldDebug)
        println("[Thread-$threadId] ${B1} total sampling rounds ${B1.samplingRound}: " +
                    "total=${B1.instances.size}, S_H1=$S_H1_size, exhausted=${B1.samplingExhausted}")
        
        if (S_H1_size < Settings.MIN_SUPP) {
            return  // Does not meet minimum support even after sampling
        }
        
        for (j in (i + 1) until bodyList.size) {
                // Check thread interruption
                if (Thread.currentThread().isInterrupted) {
                    println("Thread interrupted, exiting processBinaryHeadAtom for $headAtom (index: $i)")
                    return
                }
                
                val (B2, ruleId2, metric2) = bodyList[j]
                if (metric1.surprisal + metric2.surprisal >= Settings.MAX_SURPRISAL) {
                    continue
                }
                
                pairCount++
                
                var S_12_size = 0
                var S_H12_size = 0
                
                // 先检查 B1 已有的 instances
                for (e in B1.instances) {
                    if (B2.hasBinaryInstance(e)) {
                        S_12_size++
                        if (e in headInstances) {
                            S_H12_size++
                        }
                    }
                }
                
                val initialS12 = S_12_size
                val initialSH12 = S_H12_size
                
                // Dynamic sampling loop for B1
                // 只对非L1原子进行采样
                while (S_H12_size < Settings.MIN_SUPP && !B1.isL1Atom && !B1.samplingExhausted) {
                    val newInstances = B1.sampleBinaryInstancesEDIS()
                    
                    var newS12 = 0
                    var newSH12 = 0
                    
                    // 只检查新采样的实例
                    for (e in newInstances) {
                        if (B2.hasBinaryInstance(e)) {
                            S_12_size++
                            newS12++
                            if (e in headInstances) {
                                S_H12_size++
                                newSH12++
                            }
                        }
                    }
                    // if (shouldDebug)
                    // println("\t[Thread-$threadId] Pair($i,$j) sampling round ${B1.samplingRound}: " +
                    //         "newInstances=${newInstances.size}, newS12=$newS12, newSH12=$newSH12, S_12=$S_12_size, S_H12=$S_H12_size, " + "exhausted=${B1.samplingExhausted}")
                }
                // if (shouldDebug)
                // println("[Thread-$threadId] Pair($i,$j) total sampling rounds ${B1.samplingRound}: " +
                //             "S_12=${S_12_size}, S_H12=${S_H12_size}, exhausted=${B1.samplingExhausted}")
                
                if (S_H12_size < Settings.MIN_SUPP) {
                    continue  // Does not meet minimum support
                }
                
                // Create new metric with bodySize = S_12_size
                val metric = Metric(
                    support = S_H12_size.toDouble(),
                    headSize = headInstances.size,
                    bodySize = S_12_size
                )
                
                storeDependency(ruleId1, ruleId2, metric, metric1, metric2, binaryPositiveLift, binaryNegativeLift)
        }
        // 使用新指标更新B1
        val S_H1 = B1.instances.count { it in headInstances }
        val newMetric = Metric(
            support = S_H1.toDouble(),
            headSize = headInstances.size,
            bodySize = B1.instances.size
        )
        ID2metric[ruleId1] = newMetric
        if (shouldDebug) {
            println("[Thread-$threadId] processBinaryHeadAtom completed: $headAtom, " +
                    "checked $pairCount pairs")
        }
    }
    
    /**
     * Process single unary headAtom, perform pairwise combination of bodyAtoms
     * Uses exact set operations on unary instances
     */
    private fun processUnaryHeadAtom(
        headAtom: DepAtom,
        bodyList: List<Triple<DepAtom, Int, Metric>>,
        binaryBodyList: List<Triple<DepAtom, Int, Metric>>,
        i: Int
    ) {
        if (bodyList.isEmpty()) return
        if (i !in bodyList.indices) return
        
        var pairCount = 0
        // Pairwise combination: only combine (i, j) where i < j to avoid duplicates
        val (B1, ruleId1, metric1) = bodyList[i]
        val B1_instances = B1.getUnaryInstances()
        val headInstances = headAtom.getUnaryInstances()
        val S_H1 = B1_instances.intersect(headInstances)
        if (S_H1.size < Settings.MIN_SUPP) {
            return  // Does not meet minimum support
        }

        // Unary-Binary dependency
        for (k in binaryBodyList.indices) {
            if (Thread.currentThread().isInterrupted) {
                println("Thread interrupted, exiting processUnaryHeadAtom for $headAtom (index: $i)")
                return
            }

            val (B2, ruleId2, metric2) = binaryBodyList[k]
            if (metric1.surprisal + metric2.surprisal >= Settings.MAX_SURPRISAL) {
                continue
            }

            var S_H12_size = 0
            for (h in S_H1) {
                val instance = if (B1.isInverseRelation)  packBinaryInstance(B1.entityId, h)
                else packBinaryInstance(h, B1.entityId)
                if (B2.hasBinaryInstance(instance)) S_H12_size++
            }

            if (S_H12_size < Settings.MIN_SUPP) {
                continue  // Does not meet minimum support
            }

            var S_12_size = 0
            for (h in B1_instances) {
                val instance = if (B1.isInverseRelation) packBinaryInstance(B1.entityId, h)
                else packBinaryInstance(h, B1.entityId)
                if (B2.hasBinaryInstance(instance)) S_12_size++
            }

            val metric = Metric(
                support = S_H12_size.toDouble(),
                headSize = headInstances.size,
                bodySize = S_12_size
            )

            storeDependency(ruleId1, ruleId2, metric, metric1, metric2, mixPositiveLift, mixNegativeLift)
        }

        for (j in (i + 1) until bodyList.size) {
            // === 响应线程中断 ===
            if (Thread.currentThread().isInterrupted) {
                println("Thread interrupted, exiting processUnaryHeadAtom for $headAtom (index: $i)")
                return
            }

            val (B2, ruleId2, metric2) = bodyList[j]
            if (metric1.surprisal + metric2.surprisal >= Settings.MAX_SURPRISAL) {
                println("Skipping pair with high combined surprisal: ${metric1.surprisal} + ${metric2.surprisal}")
                continue
            }

            pairCount++

            val B2_instances = B2.getUnaryInstances()
            var S_H12_size = S_H1.intersect(B2_instances).size
            
            if (S_H12_size < Settings.MIN_SUPP) {
                continue  // Does not meet minimum support
            }
            
            // Calculate common evidence: intersection of two bodyAtom instances
            val S_12_size = B1_instances.intersect(B2_instances).size

            // Create new metric with bodySize = |S_12|
            val metric = Metric(
                support = S_H12_size.toDouble(),
                headSize = headInstances.size,
                bodySize = S_12_size
            )

            // Calculate lift
            storeDependency(ruleId1, ruleId2, metric, metric1, metric2, unaryPositiveLift, unaryNegativeLift)
        }
    }
    
    /**
     * Save metric map to JSON file - streaming output to avoid memory overflow
     * @param metricMap The metric map to save (H2B2metric or H2F2metric)
     * @param outputPath The output JSON file path
     * @param appendMode Whether to append to existing rules file (true for H2F, false for H2B)
     * @param isFormulaMap Whether the body type is DepFormula (true) or DepAtom (false)
     */
    private fun saveMetricToJson(
        metricMap: ConcurrentHashMap<DepAtom, ConcurrentHashMap<DepAtom, Int>>,
        outputPath: String,
        appendMode: Boolean,
        isFormulaMap: Boolean
    ) {
        val outputFile = File(outputPath)
        val outputRule = File(Settings.PATH_RULES_TXT)
        outputFile.parentFile?.mkdirs()
        outputRule.parentFile?.mkdirs()
        
        val metricType = if (isFormulaMap) "H2F2metric" else "H2B2metric"
        println("Saving $metricType to ${outputFile.absolutePath}...")
        
        java.io.BufferedWriter(java.io.FileWriter(outputFile)).use { writer ->
            java.io.BufferedWriter(java.io.FileWriter(outputRule, appendMode)).use { ruleWriter ->
                writer.write("{\n")
                val atomEntries = metricMap.entries.toList()

                atomEntries.forEachIndexed { atomIndex, (atom, bodyMap) ->
                    val headAtomString = atom.toString().replace("\"", "\\\"").replace("\n", "\\n")
                    writer.write("  \"$headAtomString\": {\n")

                    val bodyEntries = bodyMap.entries.toList()
                        .mapNotNull { entry ->
                            val metric = ID2metric[entry.value] ?: return@mapNotNull null
                            Triple(entry.key, entry.value, metric)
                        }
                        .sortedByDescending { it.third.confidence }
                    
                    bodyEntries.forEachIndexed { bodyIndex, (body, ruleId, metric) ->
                        val bodyString = body.toString().replace("\"", "\\\"").replace("\n", "\\n")
                        writer.write("    \"$bodyString\": $metric")
                        if (bodyIndex < bodyEntries.size - 1) writer.write(",")
                        writer.write("\n")
                        
                        // Get rule string based on body type
                        val bodyRuleString = body.getRuleString()
                        
                        // Write rule to text file with lift info for formulas
                        val liftInfo = if (isFormulaMap) metric.lift else metric.confidence
                        val ruleLine = "${metric.bodySize}\t${metric.support.toInt()}\t${format5(liftInfo)}\t${atom.getRuleString()} <= $bodyRuleString"
                        ruleWriter.write(ruleLine)
                        ruleWriter.write("\n")
                    }

                    writer.write("  }")
                    if (atomIndex < atomEntries.size - 1) writer.write(",")
                    writer.write("\n")

                    if ((atomIndex+1) % 1000 == 0) {
                        writer.flush()
                        ruleWriter.flush()
                        println("[save${metricType}ToJson] Processed ${atomIndex + 1}/${atomEntries.size} head atoms...")
                    }
                }
                writer.write("}\n")
            }
        }

        println("Successfully saved $metricType to ${outputFile.absolutePath}")
        println("Successfully saved rules to ${outputRule.absolutePath}")
        println("Total head atoms: ${metricMap.size}")
        println("Total body entries: ${metricMap.values.sumOf { it.size }}")
    }

    /**
     * Save dependency metrics to file
     * Format: bodySize\tsupp\tconfidence\tlift\tconf1\tconf2\tID1\tID2
     */
    private fun saveDependencyToFile(outputPath: String) {
        val outputFile = File(outputPath)
        outputFile.parentFile?.mkdirs()
        println("Saving depAdj2metric to ${outputFile.absolutePath}...")

        val jsonOutputPath = outputPath.replace(".txt", ".json")
        val jsonOutputFile = File(jsonOutputPath)
        jsonOutputFile.parentFile?.mkdirs()

        PrintWriter(outputFile).use { writer ->
            depAdj2metric.entries
                .flatMap { (src, dstMap) -> dstMap.entries.map { Triple(src, it.key, it.value) } }
                .sortedByDescending { it.third.confidence }
                .forEach { (id1, id2, metric) ->
                    val metric1 = ID2metric[id1]
                    val metric2 = ID2metric[id2]
                    val conf1 = metric1?.adjustedConfidence ?: 0.0
                    val conf2 = metric2?.adjustedConfidence ?: 0.0
                    val line = "${metric.bodySize}\t${metric.support.toInt()}\t${format5(metric.lift)}\t$id1\t$id2"
                    writer.println(line)
                }
        }

        val relation2deps = mutableMapOf<String, MutableList<String>>()
        depAdj2metric.entries
            .flatMap { (src, dstMap) -> dstMap.entries.map { Triple(src, it.key, it.value) } }
            .forEach { (id1, id2, metric) ->
                val headRelationId = ruleId2HeadRelationId[id1] ?: -1L
                val relationName = if (headRelationId == -1L) "UNKNOWN" else IdManager.getRelationString(headRelationId)
                val list = relation2deps.getOrPut(relationName) { mutableListOf() }
                list.add("[${id1}, ${id2}, ${format5(metric.lift)}]")
            }

        PrintWriter(jsonOutputFile).use { writer ->
            writer.println("{")
            val entries = relation2deps.entries.toList()
            entries.forEachIndexed { index, entry ->
                val relationName = escapeJson(entry.key)
                val deps = entry.value.joinToString(", ")
                val comma = if (index < entries.size - 1) "," else ""
                writer.println("  \"$relationName\": [$deps]$comma")
            }
            writer.println("}")
        }

        println("Successfully saved depAdj2metric to ${outputFile.absolutePath}")
        println("Successfully saved dependency json to ${jsonOutputFile.absolutePath}")
        println("Total dependency entries: ${depAdj2metric.values.sumOf { it.size }}")
    }

    /**
     * Print statistics about H2B2metric and rules
     */
    fun printStatistics() {
        println("\n=== Statistics ===")
        println("=== Metric Statistics ===")
        
        val totalHeads = H2B2ID.size
        val totalBodyAtoms = H2B2ID.values.sumOf { it.size }
        val totalFormulas = depAdj2metric.values.sumOf { it.size }
        val avgBodyPerHead = if (totalHeads > 0) totalBodyAtoms.toDouble() / totalHeads else 0.0
        
        println("Total head atoms: $totalHeads")
        println("Total H2B rules: $totalBodyAtoms")
        println("Total H2F rules: $totalFormulas")
        println("Average body atoms per head: ${"%.2f".format(avgBodyPerHead)}")
        
        // Breakdown by atom type
        var binaryHeads = 0
        var loopHeads = 0
        var constantHeads = 0
        
        for (head in H2B2ID.keys) {
            when {
                head.entityId == IdManager.getYId() -> binaryHeads++
                head.entityId == IdManager.getXId() -> loopHeads++
                head.entityId == 0 -> throw IllegalStateException("Head atom cannot be existence (*)")
                else -> constantHeads++
            }
        }
        
        println("\nHead atom types:")
        println("  Binary (X,Y): $binaryHeads")
        println("  Loop (X,X): $loopHeads")
        println("  Constant (X,e): $constantHeads")

        // Print lift statistics for composition phase
        println("\nComposition Phase - Lift Statistics:")
        println("-".repeat(60))
        println("Type     Positive Lift    Negative Lift    Total")
        val unaryTotal = unaryPositiveLift.get() + unaryNegativeLift.get()
        val binaryTotal = binaryPositiveLift.get() + binaryNegativeLift.get()
        val mixTotal = mixPositiveLift.get() + mixNegativeLift.get()
        println("Unary    ${unaryPositiveLift.get().toString().padStart(13)}    ${unaryNegativeLift.get().toString().padStart(13)}    ${unaryTotal.toString().padStart(8)}")
        println("Binary   ${binaryPositiveLift.get().toString().padStart(13)}    ${binaryNegativeLift.get().toString().padStart(13)}    ${binaryTotal.toString().padStart(8)}")
        println("Mix      ${mixPositiveLift.get().toString().padStart(13)}    ${mixNegativeLift.get().toString().padStart(13)}    ${mixTotal.toString().padStart(8)}")
        println("Total    ${(unaryPositiveLift.get() + binaryPositiveLift.get() + mixPositiveLift.get()).toString().padStart(13)}    ${(unaryNegativeLift.get() + binaryNegativeLift.get() + mixNegativeLift.get()).toString().padStart(13)}    ${(unaryTotal + binaryTotal + mixTotal).toString().padStart(8)}")
    }
}
