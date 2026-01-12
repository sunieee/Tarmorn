package tarmorn

import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import tarmorn.data.TripleSet
import tarmorn.structure.TLearn.DepAtom
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
        
        // Step 3: Print statistics
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
                
                // Check if it's a Zero Rule before parsing
                val isZeroRule = if (line.contains(" <= ")) {
                    val parts = line.split(" <= ")
                    parts.size >= 2 && parts[1].trim().isEmpty()
                } else {
                    false
                }
                
                if (isZeroRule) {
                    skippedZero++
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
        
        // Parse body as a relation path (not split into atoms)
        // bodyStr can be:
        // 1. Simple relation: "/people/person/nationality(X,Y)"
        // 2. Relation path: "r1*r2(X,Y)" or "r1*INVERSE_r2(X,Y)"
        // 3. With constant: "r1*r2(/m/entity)"
        val bodyAtom = parseAtom(bodyStr)
        
        // Get headSize from index
        val headSize = getAtomSize(headAtom)
        
        // Create metric
        val metric = Metric(
            support = support,
            headSize = headSize,
            bodySize = bodySize
        )
        
        // Add to H2B2metric
        val bodyMap = H2B2metric.getOrPut(headAtom) { ConcurrentHashMap() }
        bodyMap[bodyAtom] = metric
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
     * Print statistics about H2B2metric
     */
    fun printStatistics() {
        println("\n=== H2B2metric Statistics ===")
        
        val totalHeads = H2B2metric.size
        val totalRules = H2B2metric.values.sumOf { it.size }
        val avgBodyPerHead = if (totalHeads > 0) totalRules.toDouble() / totalHeads else 0.0
        
        println("Total head atoms: $totalHeads")
        println("Total rules: $totalRules")
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
    }
}
