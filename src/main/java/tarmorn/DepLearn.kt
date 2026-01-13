package tarmorn

import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import tarmorn.data.TripleSet
import tarmorn.structure.TLearn.DepAtom
import tarmorn.structure.TLearn.DepFormula
import tarmorn.structure.TLearn.Metric
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentHashMap

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
    
    // Rule metric structure: head -> body -> metric
    val H2B2metric = ConcurrentHashMap<DepAtom, ConcurrentHashMap<DepAtom, Metric>>()
    val H2F2metric = ConcurrentHashMap<DepAtom, ConcurrentHashMap<DepFormula, Metric>>()
    
    // Statistics variables
    var totalRules = 0
    val unaryStats = IntArray(4) // M0, M1, M2, M3
    val binaryStats = IntArray(4) // M0, M1, M2, M3
    
    // Lift statistics for composition phase
    val unaryPositiveLift = java.util.concurrent.atomic.AtomicInteger(0)
    val unaryNegativeLift = java.util.concurrent.atomic.AtomicInteger(0)
    val binaryPositiveLift = java.util.concurrent.atomic.AtomicInteger(0)
    val binaryNegativeLift = java.util.concurrent.atomic.AtomicInteger(0)
    
    // Constants from TLearn
    const val MIN_SURPRISAL_LIFT = 0.1
    const val TOP_K_RULE_COMBO = 200
    const val MAX_PATH_LENGTH = 3
    
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
        
        // Step 2: Read rules and convert to H2B2metric
        println("\n=== Step 2: Reading Rules ===")
        println("Reading rules from: ${Settings.PATH_RULES}")
        readRules(Settings.PATH_RULES)
        
        // Step 3: Composition phase - combine atoms into formulas
        println("\n=== Step 3: Composition Phase ===")
        try {
            compositionPhase()
        } catch (e: Exception) {
            println("Error during composition phase: ${e.message}")
            e.printStackTrace()
        }
        
        // Step 4: Save H2B2metric and H2F2metric
        println("\n=== Step 4: Saving Metrics ===")
        saveMetricToJson(
            metricMap = H2B2metric,
            outputPath = Settings.PATH_H2B2metric,
            appendMode = false,
            isFormulaMap = false
        )
        saveMetricToJson(
            metricMap = H2F2metric,
            outputPath = Settings.PATH_H2F2metric,
            appendMode = true,
            isFormulaMap = true
        )
        
        // Step 5: Print statistics
        println("\n=== Step 5: Statistics ===")
        printStatistics()
        
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
     * Simplify a rule by removing Y variable and converting to single-argument format
     * Examples:
     * - rel(X,Y) => rel(X)
     * - rel(/m/entity,Y) => INVERSE_rel(/m/entity)
     * - rel1(X,A), rel2(Y,A) => rel1(X,A), INVERSE_rel2(A)
     */
    private fun simplifyRule(ruleStr: String): String {
        val parts = ruleStr.split(" <= ")
        if (parts.size != 2) return ruleStr
        
        val headStr = parts[0].trim()
        val bodyStr = parts[1].trim()
        
        // Simplify head
        val simplifiedHead = simplifyAtom(headStr, isHead = true)
        
        // Simplify body atoms (comma-separated)
        if (bodyStr.isEmpty()) {
            return "$simplifiedHead <= "
        }
        val bodyAtoms = splitAtomsByComma(bodyStr)
        val simplifiedBodyAtoms = bodyAtoms.map { simplifyAtom(it.trim(), isHead = false) }
        val simplifiedBody = simplifiedBodyAtoms.joinToString(", ")
        
        return "$simplifiedHead <= $simplifiedBody"
    }
    
    /**
     * Split atoms by comma, respecting parentheses
     */
    private fun splitAtomsByComma(str: String): List<String> {
        val result = mutableListOf<String>()
        val current = StringBuilder()
        var depth = 0
        
        for (c in str) {
            when (c) {
                '(' -> {
                    depth++
                    current.append(c)
                }
                ')' -> {
                    depth--
                    current.append(c)
                }
                ',' -> {
                    if (depth == 0) {
                        result.add(current.toString().trim())
                        current.clear()
                    } else {
                        current.append(c)
                    }
                }
                else -> current.append(c)
            }
        }
        
        if (current.isNotEmpty()) {
            result.add(current.toString().trim())
        }
        
        return result
    }
    
    /**
     * Parse a simplified atom string into DepAtom
     * Format: relation(arg) or INVERSE_relation(arg)
     * arg can be: X, *, or /m/entity
     */
    private fun parseAtom(atomStr: String): DepAtom {
        val openParen = atomStr.indexOf('(')
        val closeParen = atomStr.indexOf(')')
        
        if (openParen == -1 || closeParen == -1 || closeParen < openParen) {
            throw IllegalArgumentException("Invalid atom format: $atomStr")
        }
        
        val relationStr = atomStr.substring(0, openParen)
        val arg = atomStr.substring(openParen + 1, closeParen).trim()
        
        val relationId = IdManager.getRelationId(relationStr)
        
        // Determine entity ID based on single argument:
        // Y = binary (X,Y) - for head atoms
        // * = existence (X,_) - for body atoms  
        // X = loop (X,X)
        // /m/entity = constant
        val entityId = when (arg) {
            "Y" -> IdManager.getYId()  // Binary: relation(Y) means relation(X,Y)
            "*" -> 0  // Existence: relation(*) means relation(X,_)
            "X" -> IdManager.getXId()  // Loop: relation(X) means relation(X,X)
            else -> IdManager.getEntityId(arg)  // Constant: relation(/m/entity)
        }
        
        return DepAtom(relationId, entityId)
    }
    
    /**
     * Simplify a single atom by removing Y variable
     * Examples:
     * - rel(X,Y) => rel(X)
     * - rel(Y,X) => INVERSE_rel(X)
     * - rel(/m/entity,Y) => INVERSE_rel(/m/entity)
     * - rel(X,/m/entity) => rel(/m/entity)
     * - rel(A,Y) => rel(A)
     * - rel(Y,A) => INVERSE_rel(A)
     */
    private fun simplifyAtom(atomStr: String, isHead: Boolean): String {
        // Check if it has arguments
        val openParen = atomStr.indexOf('(')
        if (openParen == -1) return atomStr
        
        val closeParen = atomStr.lastIndexOf(')')
        if (closeParen == -1 || closeParen < openParen) return atomStr
        
        val relation = atomStr.substring(0, openParen)
        val argsStr = atomStr.substring(openParen + 1, closeParen)
        val args = argsStr.split(",").map { it.trim() }
        
        // If not 2 arguments, return as-is
        if (args.size != 2) return atomStr
        
        val arg1 = args[0]
        val arg2 = args[1]
        
        // Check if Y is present
        val hasY = arg1 == "Y" || arg2 == "Y"
        if (!hasY) return atomStr
        
        // Determine the kept argument and whether to inverse
        val (keptArg, needsInverse) = when {
            arg1 == "Y" && arg2 != "Y" -> Pair(arg2, true)   // rel(Y,X) => INVERSE_rel(X)
            arg1 != "Y" && arg2 == "Y" -> Pair(arg1, false)  // rel(X,Y) => rel(X) or rel(Y)
            else -> return atomStr  // Both Y or neither Y
        }
        
        // Build simplified atom
        val finalRelation = if (needsInverse) "INVERSE_$relation" else relation
        
        // Determine final argument based on type:
        // - For head atoms with (X,Y): keep Y to indicate binary
        // - For body atoms with (X,Y) or (X,A/B/C...): use * to indicate existence  
        // - For constant entities: keep as-is
        val finalArg = when {
            keptArg == "X" && isHead -> "Y"  // head(X,Y) => head(Y) - binary
            keptArg == "X" && !isHead -> "*"  // body(X,Y) => body(*) - existence
            keptArg.length == 1 && keptArg[0].isUpperCase() && keptArg[0] in 'A'..'Z' -> "*"  // Variables A-Z => *
            else -> keptArg  // Constant entities like /m/entity
        }
        
        return "$finalRelation($finalArg)"
    }
    
    /**
     * Read rules from file and convert to H2B2metric
     * Rule format: bodySize\tsupport\tconfidence\thead <= body1 body2 ...
     * We only process rules without && (no complex rules)
     */
    private fun readRules(filepath: String) {
        val file = File(filepath)
        if (!file.exists()) {
            println("Warning: Rule file not found: $filepath")
            return
        }
        
        var totalLines = 0
        var parsedRules = 0
        var skippedComplex = 0
        var skippedZero = 0
        var errors = 0
        
        BufferedReader(InputStreamReader(FileInputStream(file), StandardCharsets.UTF_8)).use { reader ->
            var line: String? = reader.readLine()
            
            while (line != null) {
                totalLines++
                
                // Skip empty lines and comments
                if (line.isBlank() || line.startsWith("#")) {
                    line = reader.readLine()
                    continue
                }
                
                // Skip complex rules (containing &&)
                if (line.contains("&&")) {
                    skippedComplex++
                    line = reader.readLine()
                    continue
                }
                
                // Simplify rule before parsing
                val tokens = line.split("\t")
                if (tokens.size >= 4) {
                    val ruleString = tokens[3]
                    val simplifiedRuleString = simplifyRule(ruleString)
                    val simplifiedLine = "${tokens[0]}\t${tokens[1]}\t${tokens[2]}\t$simplifiedRuleString"
                    
                    // Parse and add simplified rule
                    try {
                        parseAndAddRule(simplifiedLine)
                        parsedRules++
                        
                        if (parsedRules % 100000 == 0) {
                            println("Parsed $parsedRules rules...")
                        }
                    } catch (e: Exception) {
                        errors++
                        if (errors <= 5) {
                            println("Error parsing rule line $totalLines: ${e.message}")
                            println("  Line: $line")
                        }
                    }
                } else {
                    errors++
                    if (errors <= 5) {
                        println("Error: Invalid line format at line $totalLines")
                        println("  Line: $line")
                    }
                }
                
                line = reader.readLine()
            }
        }
        
        println("Total lines read: $totalLines")
        println("Successfully parsed: $parsedRules rules")
        println("Skipped complex rules (with &&): $skippedComplex")
        println("Skipped Zero Rules (empty body): $skippedZero")
        if (errors > 0) {
            println("Errors encountered: $errors")
        }
    }

    fun setH2F2metric(atom: DepAtom, formula: DepFormula, metric: Metric) {
        val F2metric = H2F2metric.computeIfAbsent(atom) { ConcurrentHashMap() }
        F2metric[formula] = metric
    }

    fun setH2B2metric(headAtom: DepAtom, bodyAtom: DepAtom, metric: Metric) {
        val B2metric = H2B2metric.computeIfAbsent(headAtom) { ConcurrentHashMap() }
        B2metric[bodyAtom] = metric
    }
    
    /**
     * Parse a single rule line and add to H2B2metric
     * Format: bodySize\tsupport\tconfidence\thead <= body
     * 
     * Note: body is a relation path string like "r1*r2(const)" or "r1*INVERSE_r2"
     * It will be parsed as a single DepAtom with encoded relationId
     */
    private fun parseAndAddRule(line: String) {
        val tokens = line.split("\t")
        if (tokens.size < 4) {
            throw IllegalArgumentException("Invalid rule format: expected at least 4 tokens")
        }
        
        val bodySize = tokens[0].toInt()
        val support = tokens[1].toDouble()
        val confidence = tokens[2].toDouble()
        val ruleString = tokens[3]
        
        // Parse rule string: "head <= body"
        val parts = ruleString.split(" <= ")
        if (parts.size != 2) {
            throw IllegalArgumentException("Invalid rule format: expected 'head <= body'")
        }
        
        val headStr = parts[0].trim()
        val bodyStr = parts[1].trim()
        
        // Parse head atom
        val headAtom = parseAtom(headStr)
        val headSize = getAtomSize(headAtom)
        val metric = Metric(support, headSize, bodySize)
        if (bodyStr.isEmpty()) {
            setH2F2metric(headAtom, DepFormula(), metric)
            // Statistics for empty body (M0)
            totalRules++
            if (headAtom.entityId == IdManager.getYId()) {
                binaryStats[0]++
            } else {
                unaryStats[0]++
            }
            return
        }
        // Parse body as a relation path (not split into atoms)
        // bodyStr can be:
        // 1. Simple relation: "/people/person/nationality(X,Y)"
        // 2. Relation path: "r1*r2(X,Y)" or "r1*INVERSE_r2(X,Y)"
        // 3. With constant: "r1*r2(/m/entity)"
        setH2B2metric(headAtom, parseAtom(bodyStr), metric)
        
        // Statistics for single body atom (M1)
        totalRules++
        if (headAtom.entityId == IdManager.getYId()) {
            binaryStats[1]++
        } else {
            unaryStats[1]++
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
        println("Starting Composition Phase with Eclat algorithm...")
        
        val processedHeads = java.util.concurrent.atomic.AtomicInteger(0)
        val totalHeads = H2B2metric.size
        val threadPool = java.util.concurrent.Executors.newFixedThreadPool(Settings.WORKER_THREADS)
        val compositionActiveThreadCount = java.util.concurrent.atomic.AtomicInteger(0)
        val compositionThreadMonitorLock = Object()
        
        try {
            val futures = H2B2metric.entries.map { (headAtom, bodyMap) ->
                threadPool.submit {
                    compositionActiveThreadCount.incrementAndGet()
                    try {
                        processHeadAtom(headAtom, bodyMap)
                        val cnt = processedHeads.incrementAndGet()
                        if (cnt % 100 == 0) {
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
        
        println("Composition Phase completed. Total rules: ${H2F2metric.values.sumOf { it.size }}")
    }
    
    /**
     * Process single headAtom, perform pairwise combination of bodyAtoms
     */
    private fun processHeadAtom(headAtom: DepAtom, bodyMap: ConcurrentHashMap<DepAtom, Metric>) {
        if (bodyMap.size < 2) return  // Need at least 2 bodyAtoms to combine
        
        // Convert to list for pairwise iteration
        // extract rule with surprisal >= MIN_SURPRISAL_LIFT
        val newBodyMap = ConcurrentHashMap<DepAtom, Metric>()
        for ((bodyAtom, metric) in bodyMap) {
            if (metric.surprisal >= MIN_SURPRISAL_LIFT) {
                newBodyMap[bodyAtom] = metric
            }
        }
        val bodyList = newBodyMap.entries.toList().sortedByDescending { it.value.confidence }
        
        var pairCount = 0
        var validPairCount = 0

        // Pairwise combination: only combine (i, j) where i < j to avoid duplicates
        for (i in 0 until minOf(bodyList.size, TOP_K_RULE_COMBO)) {
            val (B1, metric1) = bodyList[i]
            val B1_instances = getL1AtomInstances(B1)
            val headInstances = getL1AtomInstances(headAtom)
            val S_H1 = B1_instances.intersect(headInstances)
            if (S_H1.size < Settings.MIN_SUPP) {
                continue  // Does not meet minimum support
            }

            for (j in (i + 1) until bodyList.size) {
                // === 响应线程中断 ===
                if (Thread.currentThread().isInterrupted) {
                    println("Thread interrupted, exiting processHeadAtom for $headAtom")
                    return
                }

                val (B2, metric2) = bodyList[j]
                pairCount++

                val B2_instances = getL1AtomInstances(B2)
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
                val lift = metric.surprisal - metric1.surprisal - metric2.surprisal
                // Only store if lift is significant
                if (lift > MIN_SURPRISAL_LIFT || lift < -maxOf(metric1.surprisal, metric2.surprisal)) {
                    val formula = DepFormula(B1, B2)
                    metric.lift = lift
                    setH2F2metric(headAtom, formula, metric)
                    validPairCount++

                    // Update lift statistics
                    if (headAtom.entityId == IdManager.getYId()) {
                        if (lift > 0) binaryPositiveLift.incrementAndGet()
                        else binaryNegativeLift.incrementAndGet()
                    } else {
                        if (lift > 0) unaryPositiveLift.incrementAndGet()
                        else unaryNegativeLift.incrementAndGet()
                    }
                }
            }
        }
    }
    
    /**
     * Get instances for a DepAtom from indexes
     */
    private fun getL1AtomInstances(atom: DepAtom): Set<Int> {
        require(atom.isL1Atom) {"Only L1 atoms are supported for instance retrieval"}
        return when {
            // Binary atom: all heads that have this relation
            atom.entityId == IdManager.getYId() -> {
                r2h2tSet[atom.relationId]?.keys ?: emptySet()
            }
            // Loop atom: entities that loop on themselves
            atom.entityId == IdManager.getXId() -> {
                ts.r2loopSet[atom.relationId] ?: emptySet()
            }
            // Existence atom: all heads that have this relation
            atom.entityId == 0 -> {
                r2h2tSet[atom.relationId]?.keys ?: emptySet()
            }
            // Constant atom: heads that connect to this specific entity
            else -> {
                val inverseRelation = RelationPath.getInverseRelation(atom.relationId)
                r2h2tSet[inverseRelation]?.get(atom.entityId) ?: emptySet()
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
    private fun <T> saveMetricToJson(
        metricMap: ConcurrentHashMap<DepAtom, ConcurrentHashMap<T, Metric>>,
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
                        .sortedByDescending { it.value.confidence }
                    
                    bodyEntries.forEachIndexed { bodyIndex, (body, metric) ->
                        val bodyString = body.toString().replace("\"", "\\\"").replace("\n", "\\n")
                        writer.write("    \"$bodyString\": $metric")
                        if (bodyIndex < bodyEntries.size - 1) writer.write(",")
                        writer.write("\n")
                        
                        // Get rule string based on body type
                        val bodyRuleString = when (body) {
                            is DepAtom -> body.getRuleString()
                            is DepFormula -> body.getRuleString()
                            else -> body.toString()
                        }
                        
                        // Write rule to text file with lift info for formulas
                        val liftInfo = if (isFormulaMap) metric.lift else metric.confidence
                        val ruleLine = "${metric.bodySize}\t${metric.support.toInt()}\t$liftInfo\t${atom.getRuleString()} <= $bodyRuleString"
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
     * Print statistics about H2B2metric and rules
     */
    fun printStatistics() {
        println("=== Metric Statistics ===")
        
        val totalHeads = H2B2metric.size
        val totalBodyAtoms = H2B2metric.values.sumOf { it.size }
        val totalFormulas = H2F2metric.values.sumOf { it.size }
        val avgBodyPerHead = if (totalHeads > 0) totalBodyAtoms.toDouble() / totalHeads else 0.0
        
        println("Total head atoms: $totalHeads")
        println("Total H2B rules: $totalBodyAtoms")
        println("Total H2F rules: $totalFormulas")
        println("Average body atoms per head: ${"%.2f".format(avgBodyPerHead)}")
        
        // Breakdown by atom type
        var binaryHeads = 0
        var loopHeads = 0
        var constantHeads = 0
        
        for (head in H2B2metric.keys) {
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
        
        // Print rule statistics
        println("\n=== Rule Statistics ===")
        println("Total rules: $totalRules")
        println("Type     M0       M1       M2       M3")
        println("-".repeat(60))
        println("Unary    ${unaryStats[0].toString().padStart(8)}  ${unaryStats[1].toString().padStart(8)}  ${unaryStats[2].toString().padStart(8)}  ${unaryStats[3].toString().padStart(8)}")
        println("Binary   ${binaryStats[0].toString().padStart(8)}  ${binaryStats[1].toString().padStart(8)}  ${binaryStats[2].toString().padStart(8)}  ${binaryStats[3].toString().padStart(8)}")
        
        // Print lift statistics for composition phase
        println("\nComposition Phase - Lift Statistics:")
        println("-".repeat(60))
        println("Type     Positive Lift    Negative Lift    Total")
        val unaryTotal = unaryPositiveLift.get() + unaryNegativeLift.get()
        val binaryTotal = binaryPositiveLift.get() + binaryNegativeLift.get()
        println("Unary    ${unaryPositiveLift.get().toString().padStart(13)}    ${unaryNegativeLift.get().toString().padStart(13)}    ${unaryTotal.toString().padStart(8)}")
        println("Binary   ${binaryPositiveLift.get().toString().padStart(13)}    ${binaryNegativeLift.get().toString().padStart(13)}    ${binaryTotal.toString().padStart(8)}")
        println("Total    ${(unaryPositiveLift.get() + binaryPositiveLift.get()).toString().padStart(13)}    ${(unaryNegativeLift.get() + binaryNegativeLift.get()).toString().padStart(13)}    ${(unaryTotal + binaryTotal).toString().padStart(8)}")
    }
}
