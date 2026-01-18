package tarmorn

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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

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
    val dependency2metric = ConcurrentHashMap<Pair<Int, Int>, Metric>()
    
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
    const val TOP_K_RULE_COMBO = 400

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
        if (lift > 0 || metric.surprisal < maxSurprisal) {
            metric.lift = if (lift > 0) lift else metric.surprisal - maxSurprisal
            dependency2metric[ruleId1 to ruleId2] = metric
            if (metric.lift > 0) positiveCounter.incrementAndGet()
            else negativeCounter.incrementAndGet()
        }
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

        saveMetricToJson(
            metricMap = H2B2ID,
            outputPath = Settings.PATH_H2B2metric,
            appendMode = false,
            isFormulaMap = false
        )

        try {
            compositionPhase()
        } catch (e: Exception) {
            println("Error during composition phase: ${e.message}")
            e.printStackTrace()
        }
        printStatistics()

        saveDependencyToFile(Settings.PATH_DEPENDENCY)

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
                            if (errorCount <= 5) {
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
            setH2B2ID(headAtom, bodyAtom, ruleId, metric)
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
        
        val processedHeads = java.util.concurrent.atomic.AtomicInteger(0)
        val totalHeads = H2B2ID.size
        val threadPool = java.util.concurrent.Executors.newFixedThreadPool(Settings.WORKER_THREADS)
        val compositionActiveThreadCount = java.util.concurrent.atomic.AtomicInteger(0)
        val compositionThreadMonitorLock = Object()
        
        try {
            val futures = H2B2ID.entries.map { (headAtom, bodyMap) ->
                threadPool.submit {
                    compositionActiveThreadCount.incrementAndGet()
                    try {
                        if (headAtom.isBinary) {
                            processBinaryHeadAtom(headAtom, bodyMap)
                        } else {
                            processUnaryHeadAtom(headAtom, bodyMap)
                        }
                        val cnt = processedHeads.incrementAndGet()
                        if (cnt % 1000 == 0) {
                            println("Processed $cnt/$totalHeads head atoms...")
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
                
                if (activeCount < Settings.WORKER_THREADS / 4 && activeCount > 0) {
                    println("FORCING SHUTDOWN: Less than 1/4 threads remaining in composition phase")
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
        
        println("Composition Phase completed. Total dependencies: ${dependency2metric.size}")
    }
    
    /**
     * Process single binary headAtom, perform pairwise combination of bodyAtoms
     * Uses dynamic sampling strategy to handle large instance sets
     */
    private fun processBinaryHeadAtom(headAtom: DepAtom, bodyMap: ConcurrentHashMap<DepAtom, Int>) {
        if (bodyMap.size < 2) return  // Need at least 2 bodyAtoms to combine
        
        // 获取当前线程ID，用于控制日志输出（仅线程0输出详细日志）
        val threadId = Thread.currentThread().id % Settings.WORKER_THREADS
        val shouldDebug = (threadId == 0L) && thread0Attempts.incrementAndGet() <= 10
        
        // Extract rules with surprisal >= MIN_SURPRISAL_LIFT
        val newBodyMap = ConcurrentHashMap<DepAtom, Int>()
        for ((bodyAtom, ruleId) in bodyMap) {
            val metric = ID2metric[ruleId] ?: continue
            if (metric.surprisal >= MIN_SURPRISAL_LIFT && metric.surprisal < Settings.MAX_SURPRISAL) {
                newBodyMap[bodyAtom] = ruleId
            }
        }
        // 获取TOP_K_RULE_COMBO个body atoms，按confidence排序
        val bodyList = newBodyMap.entries.toList()
            .mapNotNull { entry ->
                val metric = ID2metric[entry.value] ?: return@mapNotNull null
                Triple(entry.key, entry.value, metric)
            }
            .sortedByDescending { it.third.confidence }
            .take(TOP_K_RULE_COMBO)
        if (shouldDebug) {
            println("[Thread-$threadId] starting processBinaryHeadAtom for $headAtom with ${bodyList.size} body atoms")
        }
        
        var pairCount = 0
        val headInstances = headAtom.getBinaryInstances()

        for (i in 0 until bodyList.size) {
            val (B1, ruleId1, metric1) = bodyList[i]
            // 先检查 B1 已有的 instances
            // 只对非L1原子进行采样，L1原子的实例已经在r2instanceSet中
            if (!B1.isL1Atom && !B1.hasBeenSampled) {
                B1.sampleBinaryInstancesEDIS()
            }
        }
        
        // Pairwise combination with dynamic sampling
        for (i in 0 until bodyList.size) {
            val (B1, ruleId1, metric1) = bodyList[i]
            var S_H1_size = B1.instances.count { it in headInstances }
            val initialB1Size = B1.instances.size
            
            // Sample B1 until S_H1.size >= MIN_SUPP or exhausted
            // 只对非L1原子进行采样
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
            if (shouldDebug)
            println("[Thread-$threadId] ${B1} total sampling rounds ${B1.samplingRound}: " +
                        "total=${B1.instances.size}, S_H1=$S_H1_size, exhausted=${B1.samplingExhausted}")
            
            
            if (S_H1_size < Settings.MIN_SUPP) {
                continue  // Does not meet minimum support even after sampling
            }
            
            for (j in (i + 1) until bodyList.size) {
                // Check thread interruption
                if (Thread.currentThread().isInterrupted) {
                    println("Thread interrupted, exiting processBinaryHeadAtom for $headAtom")
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
        }
        if (shouldDebug) {
            println("[Thread-$threadId] processBinaryHeadAtom completed: $headAtom, " +
                    "checked $pairCount pairs")
        }
    }
    
    /**
     * Process single unary headAtom, perform pairwise combination of bodyAtoms
     * Uses exact set operations on unary instances
     */
    private fun processUnaryHeadAtom(headAtom: DepAtom, bodyMap: ConcurrentHashMap<DepAtom, Int>) {
        if (bodyMap.size < 2) return  // Need at least 2 bodyAtoms to combine
        
        // Convert to list for pairwise iteration
        // extract rule with surprisal >= MIN_SURPRISAL_LIFT
        val newBodyMap = ConcurrentHashMap<DepAtom, Int>()
        for ((bodyAtom, ruleId) in bodyMap) {
            val metric = ID2metric[ruleId] ?: continue
            if (metric.surprisal >= MIN_SURPRISAL_LIFT && metric.surprisal < Settings.MAX_SURPRISAL) {
                newBodyMap[bodyAtom] = ruleId
            }
        }
        val bodyList = newBodyMap.entries.toList()
            .mapNotNull { entry ->
                val metric = ID2metric[entry.value] ?: return@mapNotNull null
                Triple(entry.key, entry.value, metric)
            }
            .sortedByDescending { it.third.confidence }
            .take(TOP_K_RULE_COMBO)

        val binaryHeadAtom = headAtom.getBinaryAtom()
        val binaryBodyMap = H2B2ID[binaryHeadAtom]
        val binaryBodyList = binaryBodyMap?.entries?.toList()
            ?.mapNotNull { entry ->
                val metric = ID2metric[entry.value] ?: return@mapNotNull null
                Triple(entry.key, entry.value, metric)
            }
            ?.sortedByDescending { it.third.confidence }
            ?.take(TOP_K_RULE_COMBO)
            ?: emptyList()
        
        var pairCount = 0
        // Pairwise combination: only combine (i, j) where i < j to avoid duplicates
        for (i in 0 until bodyList.size) {
            val (B1, ruleId1, metric1) = bodyList[i]
            val B1_instances = B1.getUnaryInstances()
            val headInstances = headAtom.getUnaryInstances()
            val S_H1 = B1_instances.intersect(headInstances)
            if (S_H1.size < Settings.MIN_SUPP) {
                continue  // Does not meet minimum support
            }

            // Unary-Binary dependency
            for (k in binaryBodyList.indices) {
                if (Thread.currentThread().isInterrupted) {
                    println("Thread interrupted, exiting processHeadAtom for $headAtom")
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
                    println("Thread interrupted, exiting processHeadAtom for $headAtom")
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
        println("Saving dependency2metric to ${outputFile.absolutePath}...")

        PrintWriter(outputFile).use { writer ->
            dependency2metric.entries
                .sortedByDescending { it.value.confidence }
                .forEach { (idPair, metric) ->
                    val (id1, id2) = idPair
                    val metric1 = ID2metric[id1]
                    val metric2 = ID2metric[id2]
                    val conf1 = metric1?.confidence ?: 0.0
                    val conf2 = metric2?.confidence ?: 0.0
                    val line = "${metric.bodySize}\t${metric.support.toInt()}\t${format5(metric.confidence)}\t${format5(metric.lift)}\t${format5(conf1)}\t${format5(conf2)}\t$id1\t$id2"
                    writer.println(line)
                }
        }

        println("Successfully saved dependency2metric to ${outputFile.absolutePath}")
        println("Total dependency entries: ${dependency2metric.size}")
    }

    /**
     * Print statistics about H2B2metric and rules
     */
    fun printStatistics() {
        println("\n=== Statistics ===")
        println("=== Metric Statistics ===")
        
        val totalHeads = H2B2ID.size
        val totalBodyAtoms = H2B2ID.values.sumOf { it.size }
        val totalFormulas = dependency2metric.size
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
