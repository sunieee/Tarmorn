package tarmorn

import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import tarmorn.data.TripleSet
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.util.PriorityQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.iterator

/**
 * TLearn - Top-down relation path learning algorithm
 * Implements the connection algorithm: Binary Atom L=1 -connect-> Binary Atom L<=MAX_PATH_LENGTH
 */
object TLearn {

    const val MIN_SUPP = 100
    const val MAX_PATH_LENGTH = 3

    // MinHash parameters: MH_DIM = BANDS * R
    const val MH_DIM = 128
    const val BANDS = 64
    const val R = 2  // 每band维度

    // Core data structures
    val ts: TripleSet = TripleSet(Settings.PATH_TRAINING, true)
    // lateinit var r2tripleSet: MutableMap<Long, MutableSet<MyTriple>>
    lateinit var r2supp: MutableMap<Long, Int>
    lateinit var r2h2tSet: MutableMap<Long, MutableMap<Int, MutableSet<Int>>>
    lateinit var r2h2supp: MutableMap<Long, MutableMap<Int, Int>>

    // Thread-safe relation queue using ConcurrentLinkedQueue for producer-consumer pattern
    val relationQueue = PriorityQueue<RelationPathItem>(
        compareByDescending { it.supp }
//        compareBy { it.supp }
    )
    val queueLock = Object()

    // Backup of L1 relations for connection attempts
    lateinit var relationL1: List<Long>

    // Synchronized logger for workers
    private val logFile = File("out/workers.log")
    private lateinit var logWriter: BufferedWriter


    // 核心类型定义
    data class MyAtom(val relationId: Long, val entityId: Int)   // (RelationID, EntityID)，EntityID<0 表示 binary

    // Formula作为data class，最多3个Atom，可空位置表示未使用，注意不关心顺序，需重写hash函数
    data class Formula(
        val atom1: MyAtom? = null,
        val atom2: MyAtom? = null,
        val atom3: MyAtom? = null
    ) {
        override fun hashCode(): Int {
            val atoms = listOfNotNull(atom1, atom2, atom3).sortedBy { it.hashCode() }
            return atoms.fold(0) { acc, atom -> acc xor atom.hashCode() }
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (other !is Formula) return false
            return this.hashCode() == other.hashCode()
        }
    }

    // 流式计算结构
    val formulaQueue = PriorityQueue<Pair<Formula, Int>>(compareBy { it.second })  // 按support升序
    lateinit var formulaL1: List<Formula>
    val minHashRegistry = mutableMapOf<Formula, IntArray>()           // 公式→MinHash映射
    val bandToFormulaMap = mutableMapOf<Long, MutableSet<Formula>>()  // Band→公式集合映射

    // R=2时使用Long表示band内容​（高效）：
    fun extractBand(minHash: IntArray, bandIndex: Int): Long {
        val start = bandIndex * R
        return (minHash[start].toLong() shl 32) or (minHash[start + 1].toLong() and 0xFFFFFFFF)
    }


    // Initialize logger and clear the log file
    private fun initializeLogger() {
        synchronized(Any()) {
            try {
                logFile.parentFile?.mkdirs() // Create out directory if it doesn't exist
                logFile.writeText("") // Clear the file content
                logWriter = BufferedWriter(FileWriter(logFile, true)) // Append mode
            } catch (e: Exception) {
                println("Error initializing log file: ${e.message}")
            }
        }
    }

    // Synchronized logging function
    private fun logWorkerResult(connectedPath: Long, supp: Int) {
        synchronized(logWriter) {
            try {
                val pathString = IdManager.getRelationString(connectedPath)
                logWriter.write("$pathString: $supp\n")
                logWriter.flush()
            } catch (e: Exception) {
                println("Error writing to log: ${e.message}")
            }
        }
    }

    data class RelationPathItem(
        val relationPath: Long,
        val supp: Int
    )

    /**
     * Main entry point - can be run directly
     */
    @JvmStatic
    fun main(args: Array<String>) {
        Settings.load()
        println("TLearn - Top-down relation path learning algorithm")
        println("Loading triple set...")

        try {
            learn()

        } catch (e: Exception) {
            println("Error during learning: ${e.message}")
            e.printStackTrace()
        }
    }

    /**
     * Main entry point for the learning algorithm
     */
    fun learn() {
        // Initialize and clear the log file
        initializeLogger()

        // Initialize data structures
        // r2tripleSet = ts.r2tripleSet
        r2h2tSet = ts.r2h2tSet
        r2supp = ts.r2tripleSet.mapValues { it.value.size }.toMutableMap()
        r2h2supp = r2h2tSet.mapValues { entry ->
            entry.value.mapValues { it.value.size }.toMutableMap()
        } as MutableMap<Long, MutableMap<Int, Int>>

        println("Starting TLearn algorithm...")
        println("MIN_SUPP: $MIN_SUPP, MAX_PATH_LENGTH: $MAX_PATH_LENGTH")

        // Step 1: Initialize with L=1 relations
        initializeLevel1Relations()

        // Step 2: Connect relations using multiple threads
        try {
            connectRelations()
        } catch (e: Exception) {
            println("Error during relation connection: ${e.message}")
            e.printStackTrace()
        } finally {
            println("TLearn algorithm completed. Total relation paths: ${r2supp.size}")
            println(r2supp)

            // 打印LSH分桶结果
            printLSHBuckets()

            // Close the log file
            synchronized(logWriter) {
                try {
                    logWriter.close()
                } catch (e: Exception) {
                    println("Error closing log file: ${e.message}")
                }
            }
            // return r2tripleSet.mapValues { it.value.toSet() }
        }
    }

    /**
     * Step 1: Initialize level 1 relations (single relations with sufficient supp)
     */
    fun initializeLevel1Relations() {
        println("Initializing level 1 relations...")

        var addedCount = 0

        synchronized(queueLock) {
            for ((relation, tripleSet) in ts.r2tripleSet) {
                if (tripleSet.size >= MIN_SUPP) {
//                    val pathId = RelationPath.encode(relation)
                    val item = RelationPathItem(relation, tripleSet.size)
                    relationQueue.offer(item)
                    addedCount++

                    // 为L=1关系进行原子化，直接使用r2h2tSet中的反向索引
                    val h2tSet = r2h2tSet[relation]?.toMutableMap() ?: mutableMapOf()
                    val inverseRelation = RelationPath.getInverseRelation(relation)
                    val t2hSet = r2h2tSet[inverseRelation]?.toMutableMap() ?: mutableMapOf()

                    // 复用通用的原子化函数
                    atomizeRelationPath(relation, h2tSet, t2hSet)
                }
            }

            relationL1 = relationQueue.map { it.relationPath }.toList()
        }
        println("Added $addedCount level 1 relations to queue")
        // println("Level 1 relations: ${relationL1.map { IdManager.getRelationString(it) }}")
    }

    /**
     * Step 2: Connect relations using producer-consumer pattern with multiple threads
     */
    fun connectRelations() {
        println("Starting relation connection with ${Settings.WORKER_THREADS} threads...")

        val threadPool = Executors.newFixedThreadPool(Settings.WORKER_THREADS)
        val processedCount = AtomicInteger(0)
        val addedCount = AtomicInteger(0)

        // Create worker threads
        val futures = (1..Settings.WORKER_THREADS).map { threadId ->
            threadPool.submit {
                connectRelationsWorker(threadId, processedCount, addedCount)
            }
        }

        // Wait for all threads to complete
        futures.forEach { it.get() }
        threadPool.shutdown()

        println("Connection completed. Processed: ${processedCount.get()}, Added: ${addedCount.get()}")
    }

    /**
     * Worker thread for connecting relations
     */
    fun connectRelationsWorker(threadId: Int, processedCount: AtomicInteger, addedCount: AtomicInteger) {
        println("Thread $threadId started")

        while (true) {
            // Step 3: Get next relation path from queue
            val currentItem = synchronized(queueLock) {
                relationQueue.poll()
            } ?: break // Queue is empty

            val ri = currentItem.relationPath
            val riLength = RelationPath.getLength(ri)

            // Skip if already at max length
            if (riLength >= MAX_PATH_LENGTH) {
                continue
            }

            processedCount.incrementAndGet()

            // Step 4: Try connecting with all L1 relations
            for (r1 in relationL1) {
                try {
                    val connectedPath = attemptConnection(r1, ri)
                    if (connectedPath != null) {
                        val supp = computeSupp(connectedPath, r1, ri)
                        if (supp >= MIN_SUPP) {
                            synchronized(queueLock) {
                                val newItem = RelationPathItem(connectedPath, supp)
                                relationQueue.offer(newItem)
                            }
                            addedCount.incrementAndGet()

                            // Log the successful path addition
                            logWorkerResult(connectedPath, supp)

                            if (addedCount.get() % 100 == 0) {
                                println("Thread $threadId: Added ${addedCount.get()} new paths")
                                println("Thread $threadId: path $connectedPath added with supp $supp (r1: $r1, ri: $ri) TODO: ${relationQueue.size} remaining in queue")
                            }
                        }
                    }
                } catch (e: Exception) {
                    // Log error but continue processing
                    println("Thread $threadId: Error connecting paths: ${e.message}")
                }
            }
        }

        println("Thread $threadId completed")
    }

    /**
     * Step 3 & 4: Attempt to connect (r1: relation, ri: relation path)，增加前缀
     * Returns the connected path ID if successful, null otherwise
     */
    fun attemptConnection(r1: Long, ri: Long): Long? {
        // Check if ri's last relation conflicts with inverse of r1
        val riFirstRelation = RelationPath.getFirstRelation(ri)
        val inverser1 = IdManager.getInverseRelation(r1)

        // Simple check without accessing RELATION_MASK
        if (riFirstRelation == inverser1) {
            return null // Skip conflicting inverse relations
        }

        // Create connected path rp = r1 · ri (reverse order for better performance)
        val rp = RelationPath.connectHead(r1, ri)

        // Check if rp or its inverse already exists
        // if (r2tripleSet.containsKey(rp) || r2tripleSet.containsKey(IdManager.getInverseRelation(rp))) {
        //     return null // Skip existing paths
        // }
        if (r2supp.containsKey(rp)) {
            return null // Skip existing paths
        }

        return rp
    }

    /**
     * Step 4 & 5: Compute supp for a connected relation path
     * Uses the Cartesian product approach described in the algorithm
     */
    fun computeSupp(rp: Long, r1: Long, ri: Long): Int {
        // Get tail entities of r1 (these become connecting entities)
        val pathLength = RelationPath.getLength(rp)
        val r1TailEntities = r2h2supp[RelationPath.getInverseRelation(r1)]?.keys ?: emptySet()

        // Get head entities for ri
        val riHeadEntities = r2h2supp[ri]?.keys ?: emptySet()

        // Find intersection of possible connecting entities
        // val connectingEntities = r1TailEntities.intersect(riHeadEntities)
        // if (connectingEntities.isEmpty()) return 0

        val connectingEntities = r1TailEntities.asSequence()
            .filter { it in riHeadEntities }
        // if (!connectingEntities.any()) return 0

        // Compute Cartesian product
        // val resultTriples = mutableSetOf<MyTriple>()
        val h2tSet = mutableMapOf<Int, MutableSet<Int>>()
        val t2hSet = mutableMapOf<Int, MutableSet<Int>>()
        // val resultHSet = mutableSetOf<Int>()
        // val resultTSet = mutableSetOf<Int>()
        val h2supp = mutableMapOf<Int, Int>()
        val t2supp = mutableMapOf<Int, Int>()
        var size = 0

        for (connectingEntity in connectingEntities) {
            // Get head entities that can reach this connecting entity via r1
            // This is equivalent to: entities where (entity, r1, connectingEntity) exists
            val r1HeadEntities = r2h2tSet[IdManager.getInverseRelation(r1)]?.get(connectingEntity) ?: emptySet()

            // Get tail entities reachable from this connecting entity via ri
            val riTailEntities = r2h2tSet[ri]?.get(connectingEntity) ?: emptySet()
            val sameEntities = r1HeadEntities.intersect(riTailEntities)

            // Object Entity Constraint: Skip if same entities in both head and tail
            size += r1HeadEntities.size * riTailEntities.size - sameEntities.size
            // resultHSet.addAll(r1HeadEntities)
            // resultTSet.addAll(riTailEntities)

            // Update head and tail supp counts
            for (r1Head in r1HeadEntities)
                h2supp[r1Head] = h2supp.getOrDefault(r1Head, 0) + riTailEntities.size
            for (riTail in riTailEntities)
                t2supp[riTail] = t2supp.getOrDefault(riTail, 0) + r1HeadEntities.size

            // Create Cartesian product
            if (pathLength < 3) {
            // For paths with length < 3, we store the full h2t mapping
                for (r1Head in r1HeadEntities) {
                    for (riTail in riTailEntities) {
                        if (r1Head != riTail) {
                            h2tSet.getOrPut(r1Head) { mutableSetOf() }.add(riTail)
                            // t2hSet.getOrPut(riTail) { mutableSetOf() }.add(r1Head)
                        }
                    }
                }
            }
        }

        if (size >= MIN_SUPP) {
            // Add to main data structures
            // r2tripleSet[rp] = resultTriples
            r2supp[rp] = size
            r2supp[RelationPath.getInverseRelation(rp)] = size // Inverse relation also has same supp
            if (pathLength < 3) {
                // For paths with length < 3, we store the full h2t mapping
                r2h2tSet[rp] = h2tSet
                // r2h2tSet[RelationPath.getInverseRelation(rp)] = h2tSet
            }

            // Update head entity index for the new relation path
            r2h2supp[rp] = h2supp
            r2h2supp[RelationPath.getInverseRelation(rp)] = t2supp

            // 原子化：具象化为6类Atom并进行MinHash计算 + LSH分桶
            atomizeRelationPath(rp, h2tSet, t2hSet)
        }

        return size
    }


    fun computeSuppSet(rp: Long, r1: Long, ri: Long): MutableMap<Int, MutableSet<Int>> {
        // Get tail entities of r1 (these become connecting entities)
        val r1TailEntities = r2h2supp[IdManager.getInverseRelation(r1)]?.keys ?: emptySet()

        // Get head entities for ri
        val riHeadEntities = r2h2supp[ri]?.keys ?: emptySet()

        // Find intersection of possible connecting entities
        val connectingEntities = r1TailEntities.intersect(riHeadEntities)


        if (connectingEntities.isEmpty()) return mutableMapOf()

        val h2tSet = mutableMapOf<Int, MutableSet<Int>>()

        for (connectingEntity in connectingEntities) {
            // Get head entities that can reach this connecting entity via r1
            // This is equivalent to: entities where (entity, r1, connectingEntity) exists
            val r1HeadEntities = r2h2tSet[IdManager.getInverseRelation(r1)]?.get(connectingEntity) ?: emptySet()

            // Get tail entities reachable from this connecting entity via ri
            val riTailEntities = r2h2tSet[ri]?.get(connectingEntity) ?: emptySet()

            // Create Cartesian product
            for (r1Head in r1HeadEntities) {
                for (riTail in riTailEntities) {
                    h2tSet.getOrPut(r1Head) { mutableSetOf() }.add(riTail)
                }
            }
        }
        return h2tSet
    }

    /**
     * 原子化：将relation path具象化为6类Atom
     * 1. r(X,Y): r2supp[r]
     * 2. r'(X,Y): r2supp[r']
     * 3. r(X,c): r2h2supp[r'][c]
     * 4. r(X,·): r2h2supp[r].size
     * 5. r(c,X) / r'(X,c): r2h2supp[r][c]
     * 6. r(·,X) / r'(X,·): r2h2supp[r'].size
     */
    fun atomizeRelationPath(rp: Long, h2tSet: MutableMap<Int, MutableSet<Int>>, t2hSet: MutableMap<Int, MutableSet<Int>>) {
        return
        val inverseRp = RelationPath.getInverseRelation(rp)

        // 1. r(X,Y): Binary Atom with relation path rp
        val binaryAtom = MyAtom(rp, -1) // -1表示二元原子
        val binarySupp = r2supp[rp] ?: 0
        if (binarySupp >= MIN_SUPP) {
            // 生成Binary实例集合：所有(head, tail)对
            val binaryInstanceSet = h2tSet.flatMap { (head, tails) ->
                tails.map { tail -> Pair(head, tail) }
            }.toSet()
            val minHashSignature = computeBinaryMinHash(binaryInstanceSet)
            addAtomToMinHash(binaryAtom, binarySupp, minHashSignature)
        }

        // 2. r'(X,Y): Binary Atom with inverse relation path
        val inverseBinaryAtom = MyAtom(inverseRp, -1)
        val inverseBinarySupp = r2supp[inverseRp] ?: 0
        if (inverseBinarySupp >= MIN_SUPP) {
            // 生成逆Binary实例集合：所有(tail, head)对
            val inverseBinaryInstanceSet = t2hSet.flatMap { (tail, heads) ->
                heads.map { head -> Pair(tail, head) }
            }.toSet()
            val minHashSignature = computeBinaryMinHash(inverseBinaryInstanceSet)
            addAtomToMinHash(inverseBinaryAtom, inverseBinarySupp, minHashSignature)
        }

        // 3. r(X,c): Unary Atom for each constant c where rp(X,c) exists
        r2h2supp[inverseRp]?.forEach { (constant, supp) ->
            if (supp >= MIN_SUPP) {
                val unaryAtom = MyAtom(rp, constant)
                // 生成Unary实例集合：所有能到达constant的head实体
                val unaryInstanceSet = t2hSet[constant] ?: emptySet()
                val minHashSignature = computeUnaryMinHash(unaryInstanceSet)
                addAtomToMinHash(unaryAtom, supp, minHashSignature)
            }
        }

        // 4. r(X,·): Unary Atom for existence - relation rp has head entities
        val headEntityCount = r2h2supp[rp]?.size ?: 0
        if (headEntityCount >= MIN_SUPP) {
            val existenceAtom = MyAtom(rp, 0) // 0表示存在性原子"·"
            // 生成Existence实例集合：所有head实体
            val existenceInstanceSet = h2tSet.keys
            val minHashSignature = computeUnaryMinHash(existenceInstanceSet)
            addAtomToMinHash(existenceAtom, headEntityCount, minHashSignature)
        }

        // 5. r(c,X) / r'(X,c): Unary Atom for each constant c where r(c,X) exists
        r2h2supp[rp]?.forEach { (constant, supp) ->
            if (supp >= MIN_SUPP) {
                val inverseUnaryAtom = MyAtom(inverseRp, constant)
                // 生成逆Unary实例集合：从constant出发能到达的tail实体
                val inverseUnaryInstanceSet = h2tSet[constant] ?: emptySet()
                val minHashSignature = computeUnaryMinHash(inverseUnaryInstanceSet)
                addAtomToMinHash(inverseUnaryAtom, supp, minHashSignature)
            }
        }

        // 6. r(·,X) / r'(X,·): Unary Atom for existence - inverse relation has head entities
        val inverseTailEntityCount = r2h2supp[inverseRp]?.size ?: 0
        if (inverseTailEntityCount >= MIN_SUPP) {
            val inverseExistenceAtom = MyAtom(inverseRp, 0)
            // 生成逆Existence实例集合：所有tail实体
            val inverseExistenceInstanceSet = t2hSet.keys
            val minHashSignature = computeUnaryMinHash(inverseExistenceInstanceSet)
            addAtomToMinHash(inverseExistenceAtom, inverseTailEntityCount, minHashSignature)
        }
    }

    /**
     * 为Atom计算MinHash并加入LSH分桶
     */
    fun addAtomToMinHash(atom: MyAtom, supp: Int, minHashSignature: IntArray) {
        // 创建Formula (单原子公式)
        val formula = Formula(atom1 = atom)

        // 存储MinHash签名
        minHashRegistry[formula] = minHashSignature

        // LSH分桶
        performLSH(formula, minHashSignature, supp)

        // 添加到公式队列
        formulaQueue.offer(Pair(formula, supp))
    }

    /**
     * 计算Binary Atom的MinHash签名
     */
    fun computeBinaryMinHash(instanceSet: Set<Pair<Int, Int>>): IntArray {
        val signature = IntArray(MH_DIM) { Int.MAX_VALUE }

        // 为每个实例生成哈希值
        instanceSet.forEach { (entity1, entity2) ->
            for (i in 0 until MH_DIM) {
                val hashValue = computeBinaryHash(entity1, entity2, i)
                signature[i] = minOf(signature[i], hashValue)
            }
        }

        return signature
    }

    /**
     * 计算Unary Atom的MinHash签名
     */
    fun computeUnaryMinHash(instanceSet: Set<Int>): IntArray {
        val signature = IntArray(MH_DIM) { Int.MAX_VALUE }

        // 为每个实例生成哈希值
        instanceSet.forEach { entity ->
            for (i in 0 until MH_DIM) {
                val hashValue = computeUnaryHash(entity, i)
                signature[i] = minOf(signature[i], hashValue)
            }
        }

        return signature
    }

    /**
     * 计算Binary Atom的哈希值（模拟k个不同的哈希函数）
     */
    fun computeBinaryHash(entity1: Int, entity2: Int, seed: Int): Int {
        return (entity1.hashCode() xor entity2.hashCode() xor seed.hashCode()) and Int.MAX_VALUE
    }

    /**
     * 计算Unary Atom的哈希值（模拟k个不同的哈希函数）
     */
    fun computeUnaryHash(entity: Int, seed: Int): Int {
        return (entity.hashCode() xor seed.hashCode()) and Int.MAX_VALUE
    }

    /**
     * LSH分桶
     */
    fun performLSH(formula: Formula, minHashSignature: IntArray, supp: Int) {
        // 分为BANDS个band，每个band有R行
        for (bandIndex in 0 until BANDS) {
            val bandHash = extractBand(minHashSignature, bandIndex)
            bandToFormulaMap.getOrPut(bandHash) { mutableSetOf() }.add(formula)
        }
    }

    /**
     * 输出LSH分桶结果
     */
    fun printLSHBuckets() {
        println("LSH Buckets Summary:")
        println("Total buckets: ${bandToFormulaMap.size}")
        println("Total formulas in registry: ${minHashRegistry.size}")

        // 统计桶大小分布
        val bucketSizes = bandToFormulaMap.values.map { it.size }
        println("Bucket size distribution:")
        println("  Min: ${bucketSizes.minOrNull() ?: 0}")
        println("  Max: ${bucketSizes.maxOrNull() ?: 0}")
        println("  Average: ${bucketSizes.average()}")

        // 显示前10个最大的桶及其前3个原子
        val sortedBuckets = bandToFormulaMap.entries.sortedByDescending { it.value.size }.take(10)
        println("\nTop 10 largest buckets:")
        sortedBuckets.forEachIndexed { index, (bandHash, formulas) ->
            println("${index + 1}. Bucket $bandHash: ${formulas.size} formulas")
            formulas.take(3).forEach { formula ->
                val atom = formula.atom1
                if (atom != null) {
                    val atomStr = if (atom.entityId == -1) {
                        "Binary(${IdManager.getRelationString(atom.relationId)})"
                    } else if (atom.entityId == 0) {
                        "Existence(${IdManager.getRelationString(atom.relationId)})"
                    } else {
                        "Unary(${IdManager.getRelationString(atom.relationId)}, ${IdManager.getEntityString(atom.entityId)})"
                    }
                    println("    $atomStr")
                }
            }
            if (formulas.size > 3) {
                println("    ... and ${formulas.size - 3} more")
            }
        }
    }
}