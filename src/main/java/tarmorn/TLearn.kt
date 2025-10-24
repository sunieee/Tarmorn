package tarmorn

import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import tarmorn.data.TripleSet
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.iterator
import kotlin.math.abs
import tarmorn.structure.TLearn.MyAtom
import tarmorn.structure.TLearn.Formula
import tarmorn.structure.TLearn.Metric


/**
 * TLearn - Top-down relation path learning algorithm
 * Implements the connection algorithm: Binary Atom L=1 -connect-> Binary Atom L<=MAX_PATH_LENGTH
 */
object TLearn {
    // DEBUG级别，越高输出越详细
    var DEBUG_LEVEL = 1

    // DEBUG输出函数封装
    private fun debug1(message: String) {
        if (DEBUG_LEVEL >= 1) {
            println("[DEBUG1] $message")
        }
    }

    private fun debug2(message: String) {
        if (DEBUG_LEVEL >= 2) {
            println("[DEBUG2] $message")
        }
    }

    const val MIN_SUPP = 50
    const val MAX_PATH_LENGTH = 2

    // MinHash parameters: MH_DIM = BANDS * R
    const val MH_DIM = 200
    const val R = 1  // 每band维度
    const val BANDS = MH_DIM / R
    // const val bucketCountThreshold = 2  // 弃用，过滤太多不应该过滤的
    val bucketCountMap = ConcurrentHashMap<Int, Int>()

    // 全局随机种子数组，在程序启动时初始化
    private lateinit var globalHashSeeds: IntArray

    // Core data structures
    val config = Settings.load()    // 加载配置
    val ts: TripleSet = TripleSet(Settings.PATH_TRAINING, true)
    // lateinit var r2tripleSet: MutableMap<Long, MutableSet<MyTriple>>
    lateinit var R2supp: ConcurrentHashMap<Long, Int>
    // 仅有2跳及以下的relation path才存储完整的头尾实体对
    lateinit var R2h2tSet: MutableMap<Long, MutableMap<Int, MutableSet<Int>>>
    lateinit var R2h2supp: MutableMap<Long, MutableMap<Int, Int>>

    // 小写的r标识relationL1，并且在初始化后不再修改
    lateinit var r2instanceSet: MutableMap<Long, MutableSet<Int>>
    lateinit var r2tSet: Map<Long, IntArray>    // 仅保留relationL1到尾实体，使用快照数组以提升遍历性能
    lateinit var r2loopSet: MutableMap<Long, MutableSet<Int>>

    // Thread-safe relation queue using BlockingQueue (no need to sort by supp)
    val relationQueue = LinkedBlockingQueue<Long>()
    val activeThreadCount = AtomicInteger(0) // 线程安全的活动线程计数
    val threadMonitorLock = Object() // 用于线程监控的锁

    // Backup of L1 relations for connection attempts
    lateinit var relationL1: List<Long>

    // Synchronized logger for workers
    private val logFile = File("out/workers.log")
    private lateinit var logWriter: BufferedWriter
    private var logErrorReported = false // 防止重复报告日志错误
    private var logWriterClosed = false // 跟踪日志写入器状态

    private val POISON = Long.MIN_VALUE
    val processedCount = AtomicInteger(0)
    val addedCount = AtomicInteger(0)

    // 流式计算结构 - 使用线程安全的ConcurrentHashMap
    val formula2supp = ConcurrentHashMap<Formula, Int>()          // 公式→支持度映射
    val minHashRegistry = ConcurrentHashMap<Formula, IntArray>()           // 公式→MinHash映射
    val key2headAtom = ConcurrentHashMap<Int, MutableList<MyAtom>>() // 一级LSH桶：key -> atoms
    val atom2formula2metric = ConcurrentHashMap<MyAtom, ConcurrentHashMap<Formula, Metric>>() // 原子→公式→度量映射

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

    // Close logger safely to avoid keeping file handles open and potential hangs
    private fun closeLogger() {
        try {
            if (this::logWriter.isInitialized && !logWriterClosed) {
                logWriterClosed = true
                synchronized(logWriter) {
                    try {
                        logWriter.flush()
                    } catch (_: Exception) { }
                    try {
                        logWriter.close()
                    } catch (_: Exception) { }
                }
            }
        } catch (e: Exception) {
            println("Error closing log file: ${e.message}")
        }
    }

    // Initialize global hash seeds for MinHash
    private fun initializeGlobalHashSeeds() {
        val random = java.util.Random(42) // 固定主种子以确保可重现性
        val seedSet = mutableSetOf<Int>()
        
        // 生成MH_DIM个不重复的随机种子
        while (seedSet.size < MH_DIM) {
            val seed = random.nextInt(Int.MAX_VALUE)
            seedSet.add(seed)
        }
        
        globalHashSeeds = seedSet.toIntArray()
        println("Initialized ${globalHashSeeds.size} unique global hash seeds")
    }

    // Synchronized logging function
    private fun logWorkerResult(connectedPath: Long, supp: Int) {
        synchronized(logWriter) {
            try {
                // 检查 logWriter 是否仍然可用
                if (logWriterClosed || !this::logWriter.isInitialized) {
                    return
                }
                val pathString = IdManager.getRelationString(connectedPath)
                logWriter.write("$pathString: $supp\n")
                logWriter.flush()
            } catch (e: Exception) {
                // 只在第一次出现错误时打印，避免日志污染
                synchronized(this) {
                    if (!logErrorReported) {
                        println("Error writing to log: ${e.message}")
                        logErrorReported = true
                    }
                }
            }
        }
    }

    // RelationPathItem moved to structure.TLearn

    /**
     * Main entry point - can be run directly
     */
    @JvmStatic
    fun main(args: Array<String>) {
        Settings.load()
        println("TLearn - Top-down relation path learning algorithm")
        println("Loading triple set...")
        // Initialize global hash seeds first
        initializeGlobalHashSeeds()
        
        // Initialize and clear the log file
        initializeLogger()

        // Initialize data structures
        // r2tripleSet = ts.r2tripleSet
        r2loopSet = ts.r2loopSet
        // 复制一份ts.r2h2tSet，避免直接引用
        R2h2tSet = ts.r2h2tSet.mapValues { entry ->
            entry.value.mapValues { it.value.toMutableSet() }.toMutableMap()
        }.toMutableMap()
        R2supp = ConcurrentHashMap(ts.r2tripleSet.mapValues { it.value.size })
        R2h2supp = R2h2tSet.mapValues { entry ->
            entry.value.mapValues { it.value.size }.toMutableMap()
        } as MutableMap<Long, MutableMap<Int, Int>>
        r2instanceSet = R2h2tSet.mapValues { entry ->
            entry.value.flatMap { (head, tails) ->
                tails.map { tail -> pairHash(head, tail) }
            }.toMutableSet()
        }.toMutableMap()

        println("Starting TLearn algorithm...")
        println("MIN_SUPP: $MIN_SUPP, MAX_PATH_LENGTH: $MAX_PATH_LENGTH")

        // Step 1: Initialize with L=1 relations
        initializeL1Relations()

        // Step 2: Connect relations using multiple threads
        try {
            connectRelations()
        } catch (e: Exception) {
            println("Error during relation connection: ${e.message}")
            e.printStackTrace()
        } finally {
            println("TLearn algorithm completed. Total relation paths: ${R2supp.size}")
//            println(R2supp)
            println(bucketCountMap)

            // 打印LSH分桶结果
            printLSHBuckets()

            // 保存atom2formula2metric到JSON文件
            saveAtom2Formula2MetricToJson()
            // 关闭日志
            closeLogger()
            // return r2tripleSet.mapValues { it.value.toSet() }
        }
    }

    /**
     * Step 1: Initialize level 1 relations (single relations with sufficient supp)
     */
    fun initializeL1Relations() {
        println("Initializing level 1 relations...")

        for ((relation, tripleSet) in ts.r2tripleSet) {
            R2supp[relation] = tripleSet.size
            if (tripleSet.size >= MIN_SUPP) {
                relationQueue.offer(relation)
                addedCount.incrementAndGet()
                logWorkerResult(relation, tripleSet.size)

                if (!IdManager.isInverseRelation(relation)) {
                    // 为L=1关系进行原子化，直接使用R2h2tSet中的反向索引
                    val h2tSet = R2h2tSet[relation]
                    val inverseRelation = RelationPath.getInverseRelation(relation)
                    val t2hSet = R2h2tSet[inverseRelation]
                    
                    if (h2tSet != null && t2hSet != null) {
                        // 处理Binary原子
                        atomizeBinaryRelationPath(relation, tripleSet.size,
                            r2instanceSet[relation]!!, r2instanceSet[inverseRelation]!!)
                        // 处理Unary原子
                        atomizeUnaryRelationPath(relation, h2tSet.toMutableMap(), t2hSet.toMutableMap(), r2loopSet[relation] ?: mutableSetOf())
                    } else {
                        println("Warning: Missing h2tSet or t2hSet for relation ${IdManager.getRelationString(relation)} or its inverse ${IdManager.getRelationString(inverseRelation)}")
                    }
                }
            }
        }

        relationL1 = relationQueue.map { it }.toList()
        // 使用不可变快照，避免并发修改影响，并提升遍历效率
        r2tSet = relationL1.associateWith { r ->
            val inv = IdManager.getInverseRelation(r)
            val keys = R2h2supp[inv]?.keys ?: emptySet()
            // 拷贝为数组，遍历更快，且是稳定快照
            keys.toIntArray()
        }
        val cnt = addedCount.get()
        println("Added $cnt level 1 relations to queue")
        // println("Level 1 relations: ${relationL1.map { IdManager.getRelationString(it) }}")
    }

    /**
     * Step 2: Connect relations using producer-consumer pattern with multiple threads
     */
    fun connectRelations() {
        println("Starting relation connection with ${Settings.WORKER_THREADS} threads...")

        val threadPool = Executors.newFixedThreadPool(Settings.WORKER_THREADS)


        // Create worker threads
        activeThreadCount.set(Settings.WORKER_THREADS)
        val futures = (1..Settings.WORKER_THREADS).map { threadId ->
            threadPool.submit {
                connectRelationsWorker(threadId)
            }
        }


        try {
            var lastActiveCount = Settings.WORKER_THREADS
            while (true) {
                val activeCount: Int
                // Guarded wait: only等待当计数未变化，避免丢失通知
                synchronized(threadMonitorLock) {
                    while (activeThreadCount.get() == lastActiveCount) {
                        threadMonitorLock.wait()
                    }
                    activeCount = activeThreadCount.get()
                    lastActiveCount = activeCount
                }
                println("Thread count changed: $activeCount/${Settings.WORKER_THREADS} active")

                if (activeCount < Settings.WORKER_THREADS / 4) {
                    println("FORCING SHUTDOWN: 75% threads have finished")
                    futures.forEach { it.cancel(true) }
                    threadPool.shutdownNow()
                    break
                }
            }
        } catch (e: Exception) {
            println("Error in thread monitoring: ${e.message}")
        } finally {
            threadPool.shutdownNow()
        }
    }

    /**
     * Worker thread for connecting relations
     */
    fun connectRelationsWorker(threadId: Int) {
        println("Thread $threadId started")

        // 如果只有当前一个线程卡主，则直接结束
        while (true) {
            // Step 3: Get next relation path from queue
            val item = relationQueue.poll(1, TimeUnit.SECONDS) ?: run {
                relationQueue.put(POISON)
                POISON
            }
            if (item == POISON) {
                relationQueue.put(POISON)
                break
            }               // 优雅收尾

            val length = RelationPath.getLength(item)
            if (length >= MAX_PATH_LENGTH) continue

            try {
                runTask(threadId, item)
            }
            catch (e: Exception) {
                println("Error in thread $threadId processing relation $item: ${e.message}")
                e.printStackTrace()
            }
        }

        val cnt = activeThreadCount.decrementAndGet()
        // 发送通知需持有锁
        synchronized(threadMonitorLock) {
            println("Thread $threadId completed, $cnt threads remain")
            // 使用notifyAll防止潜在的单通知丢失或未来扩展多个等待者
            threadMonitorLock.notifyAll()
        }
    }

    fun runTask(threadId: Int, ri: Long) {
        processedCount.incrementAndGet()

        // Step 4: Try connecting with all L1 relations (immediate enqueue per item)
        for (r1 in relationL1) {
            val connectedPath = attemptConnection(r1, ri)
            if (connectedPath != null) {
                val supp = computeSupp(connectedPath, r1, ri)

                if (supp >= MIN_SUPP) {
                    // 检查反向关系对的连接结果
//                    if (IdManager.getInverseRelation(r1) == ri) {
//                        println("[runTask] Successfully connected inverse pair: ${IdManager.getRelationString(r1)}, supp: $supp")
//                    }
                    
                    relationQueue.offer(connectedPath)
                    val cnt = addedCount.incrementAndGet()

                    // Per-item log (keeps queue flowing)
                     logWorkerResult(connectedPath, supp)

                    //  || activeThreadCount.get() < Settings.WORKER_THREADS
                    if (cnt % 100 == 0 || activeThreadCount.get() < Settings.WORKER_THREADS / 4) {
                        val remaining = relationQueue.size
                        println("Thread $threadId: Added $cnt new paths; latest supp=$supp; TODO: $remaining remaining in queue")
                    }
                }
            }
        }
    }

    /**
     * Step 3 & 4: Attempt to connect (r1: relation, ri: relation path)，增加前缀
     * Returns the connected path ID if successful, null otherwise
     */
    fun attemptConnection(r1: Long, ri: Long): Long? {
        // Create connected path rp = r1 · ri (reverse order for better performance)
        val rp = RelationPath.connectHead(r1, ri)
        val inverseRp = RelationPath.getInverseRelation(rp)

        // 要求长度为3的路径必须包含 R·INVERSE_R 子串
//       if (ri > RelationPath.MAX_RELATION_ID && !RelationPath.hasInverseRelation(rp)) {
//           println("[attemptConnection] Skipping L3 path without inverse relation: ${IdManager.getRelationString(rp)}")
//           return null
//       }

        // Check if rp or its inverse already exists
        // if (r2tripleSet.containsKey(rp) || r2tripleSet.containsKey(IdManager.getInverseRelation(rp))) {
        //     return null // Skip existing paths
        // }
        // 原子插入，避免全局同步：只有当 rp 和 inverseRp 都是首次出现时才继续
        // 特殊处理：如果 rp == inverseRp（自反路径），只检查一次
        if (rp == inverseRp) {
            val prevRp = R2supp.putIfAbsent(rp, 0)
            if (prevRp != null) {
                return null
            }
        } else {
            val prevRp = R2supp.putIfAbsent(rp, 0)
            val prevInv = R2supp.putIfAbsent(inverseRp, 0)
            if (prevRp != null || prevInv != null) {
                return null
            }
        }

        return rp
    }

    /**
     * Step 4 & 5: Compute supp for a connected relation path
     * Uses the correct connection algorithm matching Python implementation
     */
    fun computeSupp(rp: Long, r1: Long, ri: Long): Int {
        // Get tail entities of r1 (these become connecting entities)
        val pathLength = RelationPath.getLength(rp)
        val r1TailEntities = r2tSet[r1]!!
        // Get head entities for ri
        val riHeadEntities = R2h2supp[ri]?.keys ?: emptySet()

        // Find intersection of possible connecting entities (connection nodes)
        val connectingEntities = r1TailEntities.asSequence()
            .filter { it in riHeadEntities }

        // Initialize data structures
        val h2tSet = mutableMapOf<Int, MutableSet<Int>>()
        val t2hSet = mutableMapOf<Int, MutableSet<Int>>()
        val h2supp = mutableMapOf<Int, Int>()
        val t2supp = mutableMapOf<Int, Int>()
        val loopSet = mutableSetOf<Int>()
        val instanceSet = mutableSetOf<Int>()
        val inverseSet = mutableSetOf<Int>()
        
        for (connectingEntity in connectingEntities) {
            // Get head entities that can reach this connecting entity via r1
            // This is equivalent to: entities where (entity, r1, connectingEntity) exists
            val r1HeadEntities = R2h2tSet[IdManager.getInverseRelation(r1)]?.get(connectingEntity) ?: emptySet()

            // Get tail entities reachable from this connecting entity via ri
            val riTailEntities = R2h2tSet[ri]?.get(connectingEntity) ?: emptySet()

            // Create Cartesian product, avoiding duplicates
            for (r1Head in r1HeadEntities) {
                for (riTail in riTailEntities) {
                    if (r1Head != riTail) { // Object Entity Constraint: avoid self-loops
                        val pairHashValue = pairHash(r1Head, riTail)
                        // instanceSet.add() returns true if element was added (not already present)
                        if (instanceSet.add(pairHashValue)) {
                            inverseSet.add(pairHash(riTail, r1Head))
                            
                            // Store h2t mapping only for paths with length < 3
                            if (pathLength < 3) {
                                h2tSet.getOrPut(r1Head) { mutableSetOf() }.add(riTail)
                                t2hSet.getOrPut(riTail) { mutableSetOf() }.add(r1Head)
                            }
                        }
                    } else {
                        loopSet.add(r1Head)
                    }
                }
            }
        }
        
        // Calculate support counts for heads and tails
        for ((head, tails) in h2tSet) {
            h2supp[head] = tails.size
        }
        for ((tail, heads) in t2hSet) {
            t2supp[tail] = heads.size
        }
        
        val size = instanceSet.size

        // 因为R2supp占用内存较小，且需要记录，频繁访问，直接存储
        val inverseRp = RelationPath.getInverseRelation(rp)
        R2supp[rp] = size
        // 只有当 rp != inverseRp 时才单独存储反向路径的支持度
        if (rp != inverseRp) {
            R2supp[inverseRp] = size
        }

        if (size >= MIN_SUPP) {
            // Add to main data structures
//            r2instanceSet[rp] = instanceSet
//            r2instanceSet[inverseRp] = inverseSet
            val (forwardSuccess, inverseSuccess) = atomizeBinaryRelationPath(rp, size, instanceSet, inverseSet)
//            val (forwardSuccess, inverseSuccess) = Pair(true, true) // 跳过Binary原子化，直接进行Unary原子化

            if (forwardSuccess || pathLength < 3) R2h2supp[rp] = h2supp
            if (inverseSuccess || pathLength < 3) R2h2supp[inverseRp] = t2supp

            // 长度超过1的不进行Unary原子化
            if (pathLength < 3) {
                // For paths with length < 3, we store the full h2t mapping
                R2h2tSet[rp] = h2tSet
                // R2h2tSet[RelationPath.getInverseRelation(rp)] = h2tSet
//                atomizeUnaryRelationPath(rp, h2tSet, t2hSet, loopSet)
            }
        }

        return size
    }

    /**
     * 处理Binary原子化：r(X,Y) 和 r'(X,Y)
     * 直接使用预计算的MinHash签名
     */
    fun atomizeBinaryRelationPath(rp: Long, supp: Int, instanceSet: MutableSet<Int>, inverseSet: MutableSet<Int>): Pair<Boolean, Boolean> {
        debug2("atomizeBinaryRelationPath: rp=$rp, supp=$supp, instanceSet.size=${instanceSet.size}, inverseSet.size=${inverseSet.size}")

        val inverseRp = RelationPath.getInverseRelation(rp)
        // 1. r(X,Y): Binary Atom with relation path rp
        val binaryAtom = MyAtom(rp, IdManager.getYId()) // Y表示二元原子
        // 2. r'(X,Y): Binary Atom with inverse relation path
        val inverseBinaryAtom = MyAtom(inverseRp, IdManager.getYId())
        return Pair(
            performLSH(binaryAtom, instanceSet),
            performLSH(inverseBinaryAtom, inverseSet)
        )
    }
    
    /**
     * 处理Unary原子化：r(X,c), r(X,·), r(c,X), r(·,X), r(X,X)
     * 需要动态计算MinHash签名
     */
    fun atomizeUnaryRelationPath(rp: Long, h2tSet: MutableMap<Int, MutableSet<Int>>, t2hSet: MutableMap<Int, MutableSet<Int>>, loopSet: MutableSet<Int>) {
        debug2("atomizeUnaryRelationPath: rp=$rp, h2tSet.size=${h2tSet.size}, t2hSet.size=${t2hSet.size}, loopSet.size=${loopSet.size}")
        val inverseRp = RelationPath.getInverseRelation(rp)

        // 3. r(X,c): Unary Atom for each constant c where rp(X,c) exists
        R2h2supp[inverseRp]?.forEach { (constant, supp) ->
            if (supp >= MIN_SUPP) {
                val unaryAtom = MyAtom(rp, constant)
                // 生成Unary实例集合：所有能到达constant的head实体
                val unaryInstanceSet = t2hSet[constant]
                if (unaryInstanceSet != null) {
                    performLSH(unaryAtom, unaryInstanceSet)
                }
            }
        }
        
        // 4. r(X,·): Unary Atom for existence - relation rp has head entities
        val headEntityCount = R2h2supp[rp]?.size ?: 0
        if (headEntityCount >= MIN_SUPP) {
            val existenceAtom = MyAtom(rp, 0) // 0表示存在性原子"·"
            // 生成Existence实例集合：所有head实体
            val existenceInstanceSet = h2tSet.keys
            performLSH(existenceAtom, existenceInstanceSet)
        }
        
        // 5. r(c,X) / r'(X,c): Unary Atom for each constant c where r(c,X) exists
        R2h2supp[rp]?.forEach { (constant, supp) ->
            if (supp >= MIN_SUPP) {
                val inverseUnaryAtom = MyAtom(inverseRp, constant)
                // 生成逆Unary实例集合：从constant出发能到达的tail实体
                val inverseUnaryInstanceSet = h2tSet[constant]
                if (inverseUnaryInstanceSet != null) {
                    // TODO: h2tSet 中可能没有该constant的映射
                    performLSH(inverseUnaryAtom, inverseUnaryInstanceSet)
                }
            }
        }
        
        // 6. r(·,X) / r'(X,·): Unary Atom for existence - inverse relation has head entities
        val inverseTailEntityCount = R2h2supp[inverseRp]?.size ?: 0
        if (inverseTailEntityCount >= MIN_SUPP) {
            val inverseExistenceAtom = MyAtom(inverseRp, 0)
            // 生成逆Existence实例集合：所有tail实体
            val inverseExistenceInstanceSet = t2hSet.keys
            performLSH(inverseExistenceAtom, inverseExistenceInstanceSet)
        }

        // 7. r(X,X): Unary Atom for loops - r(X,X) exists
        if (loopSet.size >= MIN_SUPP) {
            val loopAtom = MyAtom(rp, IdManager.getXId()) // X表示循环原子
            performLSH(loopAtom, loopSet)
        }
    }

    /**
     * 计算Unary Atom的MinHash签名
     */
    fun computeMinHash(instanceSet: Set<Int>, isBinary: Boolean=false): IntArray {
        val signature = IntArray(MH_DIM) { Int.MAX_VALUE }

        // 检查空集合，如果为空则抛出异常
        if (instanceSet.isEmpty()) {
            throw IllegalArgumentException("Cannot compute MinHash for empty instance set")
        }

        // 为每个实例生成哈希值 - 先用正数空间计算最小值，再在最后按需取负
        instanceSet.forEach { entity ->
            for (i in 0 until MH_DIM) {
                val hashValue = computeUnaryHash(entity, globalHashSeeds[i])
                if (hashValue < signature[i]) {
                    signature[i] = hashValue
                }
            }
        }

        if (isBinary) {
            for (i in 0 until MH_DIM) {
                signature[i] = -signature[i]
            }
        }

        return signature
    }


    fun pairHash(entity1: Int, entity2: Int): Int {
        // 直接实现Pair的hashCode原理，避免创建对象
        // 33550337 is a prime number, and the 6th perfect number + 1
//        return entity1 * 33550337 + entity2
        return entity1 * 307 + entity2
    }

    fun mix64(z0: Long): Long {
        var z = z0 + 0x9E3779B97F4A7C15UL.toLong()
        z = (z xor (z ushr 30)) * 0xBF58476D1CE4E5B9UL.toLong()
        z = (z xor (z ushr 27)) * 0x94D049BB133111EBUL.toLong()
        return z xor (z ushr 31)
    }

    fun mix32(z0: Int): Int {
        val z = z0.toLong() and 0xFFFF_FFFFL  // 转成无符号 Long
        return (mix64(z) ushr 32).toInt()
    }

    /**
     * 计算Binary Atom的哈希值（模拟k个不同的哈希函数）
     * 使用正数空间 [-Int.MAX_VALUE, 0]
     * 确保(entity1, entity2)和(entity2, entity1)产生不同的哈希值
     */
    fun computeBinaryHash(entity1: Int, entity2: Int, seed: Int): Int {
        // 使用Triple的hashCode，确保顺序敏感且高效
        var hash = pairHash(entity1, entity2)
        hash = mix32(hash xor seed)
//        require(finalHash < Int.MAX_VALUE && finalHash > 0)
        // 确保返回正数
        return - abs(hash)
    }

    /**
     * 计算Unary Atom的哈希值（模拟k个不同的哈希函数）
     * 使用负数空间 [0, Int.MAX_VALUE]
     */
    fun computeUnaryHash(entity: Int, seed: Int): Int {
        // 使用Pair的hashCode，简洁高效
//        val hash = pairHash(entity, seed)
        val hash = mix32(entity xor seed)
        // 与seedIndex进行额外的混合
//        val finalHash = - abs(hash) - 1
        // 确保返回负数
//        require(finalHash < 0)
        return abs(hash)
    }

    /**
     * LSH分桶 - 一级桶优化版本，直接使用单个MinHash值作为key
     * 正确估计Jaccard相似度：bucketCount / BANDS
     * 对于长度超过1的关系路径，只应该将Formula添加到已经存在的LSH桶中，而不应该创建新的桶
     * 为Atom计算MinHash并加入LSH分桶
     */
    fun performLSH(currentAtom: MyAtom, instanceSet: Set<Int>): Boolean {
        debug2("performLSH: Atom=$currentAtom, instanceSet.size=${instanceSet.size}")
        // 动态计算支持度
        val mySupp = instanceSet.size
        
        // 创建Formula (单原子公式)
        val minHashSignature = computeMinHash(instanceSet, currentAtom.isBinary)
        // 使用 isL2Atom：范围关系是 isHeadAtom ⊆ isL1Atom ⊆ isL2Atom
        if (currentAtom.isL2Atom) {
            val formula = Formula(atom1 = currentAtom)
            minHashRegistry[formula] = minHashSignature
            formula2supp[formula] = mySupp
        }

        val relevantAtom2BucketCount = mutableMapOf<MyAtom, Int>()  // 统计每个相关原子的碰撞次数

        // 分为BANDS个band，每个band有R=1行，直接使用MinHash值作为键
        for (bandIndex in 0 until BANDS) {
            val key = minHashSignature[bandIndex]     // 单个MinHash值作为键
            
            // 查询相关的已有的桶 - 线程安全地读取
            val bucket = key2headAtom[key]
            if (bucket != null) {
                // 同步访问bucket以避免并发修改
                synchronized(bucket) {
                    // 为这个桶中的每个原子增加碰撞计数
                    bucket.forEach { relevantAtom ->
                        relevantAtom2BucketCount[relevantAtom] = 
                            relevantAtom2BucketCount.getOrDefault(relevantAtom, 0) + 1
                    }
                }
            }

            // 如果是headAtom，放入桶中 - 线程安全地添加
            if (currentAtom.isHeadAtom) {
                val bucket = key2headAtom.computeIfAbsent(key) { 
                    java.util.Collections.synchronizedList(mutableListOf()) 
                }
                synchronized(bucket) {
                    bucket.add(currentAtom)
                }
            }
        }

        // 进行相似性评估和过滤 - 使用预计算的碰撞次数
        var cnt = 0
        relevantAtom2BucketCount.forEach { (bucketAtom, bucketCount) ->
            // if (bucketCount < bucketCountThreshold) return@forEach // 跳过碰撞次数过少的，避免噪声
            bucketCountMap[bucketCount] = bucketCountMap.getOrDefault(bucketCount, 0) + 1
            if (bucketAtom == currentAtom) return@forEach // 跳过相同原子，避免重复组合
            val formula = Formula(atom1 = bucketAtom)
            val supp = formula2supp[formula]
            val registry = minHashRegistry[formula]
            // 调试信息：如果仍然为null，打印详细信息
            require(registry != null && supp != null) {
                "Error: Missing MinHash registry for formula $formula"
            }
            // 直接使用碰撞次数计算Jaccard相似度：bucketCount / BANDS
            val jaccard = bucketCount.toDouble() / BANDS

            // 估计交集大小
            var intersectionSize = estimateIntersectionSize(jaccard, mySupp, supp)
            var metric = Metric(jaccard, intersectionSize, supp, mySupp)

            if (metric.valid) {
                if (validateAndCreateFormula(currentAtom, bucketAtom, instanceSet, jaccard) != null) cnt++
            }
            if (metric.inverse().valid && currentAtom.isHeadAtom) {
                if (validateAndCreateFormula(bucketAtom, currentAtom, bucketAtom.getInstanceSet(), jaccard) != null) cnt++
            }

        }
//        if (cnt > 0) println("Found $cnt valid combinations for Atom: $myAtom:")
        return currentAtom.isL2Atom || cnt > 0
    }
    
    /**
     * LSH分桶 - 专门用于L2Formula，从key2headAtom中查找相关原子进行组合
     * 使用一级桶正确估计Jaccard相似度
     */
    fun performLSHforL2Formula(myFormula: Formula, myFormulaInstances: IntArray, minHashSignature: IntArray, originalMetric: Metric): Int {
        // 动态计算myFormula的支持度
        val mySupp = myFormulaInstances.size
        
        val relevantAtom2BucketCount = mutableMapOf<MyAtom, Int>()  // 统计每个相关原子的碰撞次数

        // 分为BANDS个band，每个band有R=1行，直接使用MinHash值作为键
        for (bandIndex in 0 until BANDS) {
            val key = minHashSignature[bandIndex]     // 单个MinHash值作为键
            
            // 查询相关的已有的原子桶 - 线程安全地读取
            val bucket = key2headAtom[key]
            if (bucket != null) {
                synchronized(bucket) {
                    // 为这个桶中的每个原子增加碰撞计数
                    bucket.forEach { relevantAtom ->
                        relevantAtom2BucketCount[relevantAtom] = 
                            relevantAtom2BucketCount.getOrDefault(relevantAtom, 0) + 1
                    }
                }
            }
        }

        var cnt = 0
        // 估计与过滤阶段：使用预计算的碰撞次数直接估计Jaccard相似度
        relevantAtom2BucketCount.forEach { (atom, bucketCount) ->
            // if (bucketCount < bucketCountThreshold) return@forEach // 跳过碰撞次数过少的，避免噪声
            bucketCountMap[bucketCount] = bucketCountMap.getOrDefault(bucketCount, 0) + 1
            if (atom == myFormula.atom1 || atom == myFormula.atom2) return@forEach // 跳过相同原子，避免重复组合
            
            // 直接使用碰撞次数计算Jaccard相似度
            val jaccard = bucketCount.toDouble() / BANDS
            
            // 动态计算支持度
            val atomInstances = atom.getInstanceSet()
            val supp = atomInstances.size
            
            // 估计交集大小
            val intersectionSize = estimateIntersectionSize(jaccard, mySupp, supp)
            val metric = Metric(jaccard, intersectionSize, supp, mySupp)

            if (metric.valid && metric.betterThan(originalMetric)) {
                // 创建二元公式组合
                val newFormula = Formula(atom1 = myFormula.atom1, myFormula.atom2, atom)
                val formula2metric = atom2formula2metric.computeIfAbsent(atom) { ConcurrentHashMap() }
                formula2metric[newFormula] = metric
                
                cnt++
            }
        }
//        if (cnt > 0) println("Found $cnt valid combinations for L2Formula $myFormula:")
        return cnt
    }
    
    
    /**
     * 估计交集大小：I_est = J_est * (size_a1 + size_a2) / (1 + J_est)
     */
    private fun estimateIntersectionSize(jaccardSimilarity: Double, size1: Int, size2: Int): Double {
        val ret = jaccardSimilarity * (size1 + size2) / (1 + jaccardSimilarity)
//        return min(ret, min(size1, size2).toDouble()) // 交集大小不应超过较小集合的大小
        return ret
    }

    /**
     * 验证并创建公式 - 处理自证式一元规则问题和度量计算
     * @param currentAtom 当前原子
     * @param bucketAtom 目标原子
     * @param instanceSet 实例集合
     * @param jaccard Jaccard相似度
     * @return 如果创建成功返回公式，否则返回null
     */
    private fun validateAndCreateFormula(
        currentAtom: MyAtom,
        bucketAtom: MyAtom,
        instanceSet: Set<Int>,
        jaccard: Double
    ): Formula? {
        debug2("validateAndCreateFormula: myAtom=$currentAtom, atom=$bucketAtom, instanceSet.size=${instanceSet.size}, jaccard=$jaccard")
        var intersectionSize: Double
        var metric: Metric
        
        // 动态计算支持度
        val atomInstances = bucketAtom.getInstanceSet()
        val supp = atomInstances.size
        val mySupp = instanceSet.size
        debug2("validateAndCreateFormula: atomInstances.size=$supp, mySupp=$mySupp")
        
        // 自证式一元规则（entity-anchored unary rules）问题
//        if (myAtom.isL2Atom && !myAtom.isBinary) {  这种写法有问题，会漏掉L1Atom的情况
        if (!currentAtom.isL1Atom && !currentAtom.isBinary) {
            val constant = bucketAtom.entityId
            val inverseRelation = IdManager.getInverseRelation(currentAtom.firstRelation)
            val t2hSet = ts.r2h2tSet[inverseRelation]
            if (t2hSet == null) {
                val inverseRelationStr = IdManager.getRelationString(inverseRelation)
                println("Warning: Missing t2hSet for relation $inverseRelationStr")
            }
            // 关系稀疏时，可能不存在t2hSet
            val newInstanceSet = if (t2hSet != null && t2hSet[constant] != null) {
                instanceSet.filter { !t2hSet[constant]!!.contains(it) }
            } else {
                instanceSet // 如果逆关系不存在，使用原始实例集合
            }

            intersectionSize = newInstanceSet.intersect(atomInstances).size.toDouble()
            metric = Metric(jaccard, intersectionSize, supp, newInstanceSet.size)
            debug2("validateAndCreateFormula: unary rule, constant=$constant, newInstanceSet.size=${newInstanceSet.size}, intersectionSize=$intersectionSize, metric=$metric")
        } else {
            intersectionSize = instanceSet.intersect(atomInstances).size.toDouble()
            metric = Metric(jaccard, intersectionSize, supp, mySupp)
            debug2("validateAndCreateFormula: binary/general, intersectionSize=$intersectionSize, metric=$metric")
        }

        if (!metric.valid) return null

        val newFormula = Formula(atom1 = currentAtom, atom2 = bucketAtom)
        
        // 添加到结果映射 - 线程安全
        val formula2metric = atom2formula2metric.computeIfAbsent(bucketAtom) { ConcurrentHashMap() }
        formula2metric[newFormula] = metric
        debug2("validateAndCreateFormula: newFormula=$newFormula, metric=$metric")
        return newFormula
    }


    /**
     * 输出LSH分桶结果 - 适配一级桶，防止并发修改异常
     */
    fun printLSHBuckets() {
        println("LSH Buckets Summary:")
        
        // 创建快照以避免并发修改异常
        val key2headAtomSnapshot = synchronized(key2headAtom) {
            key2headAtom.mapValues { (_, atoms) ->
                atoms.toList() // 创建不可变副本
            }.toMap()
        }
        
        val allBuckets = key2headAtomSnapshot.values
        println("Total buckets: ${allBuckets.size}")
        println("Total formulas in registry: ${minHashRegistry.size}")

        // 统计桶大小分布
        val bucketSizes = allBuckets.map { it.size }
        println("Bucket size distribution:")
        println("  Min: ${bucketSizes.minOrNull() ?: 0}")
        println("  Max: ${bucketSizes.maxOrNull() ?: 0}")
        println("  Average: ${bucketSizes.average()}")

        // 收集所有桶并按原子类型分类
        val allBucketsWithInfo = key2headAtomSnapshot.entries.map { (key, atoms) ->
            Pair(key, atoms)
        }
        
        // 定义过滤函数避免重复代码
        fun isBinaryBucket(atoms: List<MyAtom>) = atoms.first().entityId == IdManager.getYId()
        
        // 分离Binary和Unary桶，并统一显示
        val bucketTypes = listOf("Binary", "Unary")
        
        bucketTypes.forEach { bucketType ->
            val allBucketsOfType = allBucketsWithInfo.filter { (_, atoms) ->
                if (bucketType == "Binary") isBinaryBucket(atoms) else !isBinaryBucket(atoms)
            }
            val top20Buckets = allBucketsOfType.sortedByDescending { it.second.size }.take(10)
            
            println("\nTop 20 largest $bucketType buckets:")
            println("Total $bucketType buckets: ${allBucketsOfType.size}")
            top20Buckets.forEachIndexed { index, (key, atoms) ->
                println("${index + 1}. $bucketType Bucket ($key): ${atoms.size} atoms")
                atoms.take(10).forEach { atom ->
                    println("    $atom")
                }
                if (atoms.size > 10) {
                    println("    ... and ${atoms.size - 10} more")
                }
            }
        }
    }

    /**
     * 保存atom2formula2metric为JSON文件 - 流式输出避免内存溢出
     */
    private fun saveAtom2Formula2MetricToJson() {
        val outDir = File("out/" + Settings.DATASET)
        outDir.mkdirs() // 确保out目录存在

        val outputFile = File(outDir, "atom2formula2metric.json")
        val outputRule = File(outDir, "rule.txt")
        BufferedWriter(FileWriter(outputFile)).use { writer ->
            // Write rules in parallel while streaming JSON
            BufferedWriter(FileWriter(outputRule)).use { ruleWriter ->
            writer.write("{\n")
            val atomEntries = atom2formula2metric.entries.toList()

            atomEntries.forEachIndexed { atomIndex, (atom, formula2Metric) ->
                // 转义JSON字符串中的特殊字符
                val atomString = atom.toString().replace("\"", "\\\"").replace("\n", "\\n")
                writer.write("  \"$atomString\": {\n")

                val formulaEntries = formula2Metric.entries.toList()
                    .sortedByDescending { it.value.confidence }  // 按confidence降序排序
                    .let { sorted ->
                        // 保留confidence >= 0.6的formula，或者前20个（取较多者）
                        val highConfidenceFormulas = sorted.filter { it.value.confidence >= 0.6 }
                        if (highConfidenceFormulas.size >= 20) {
                            highConfidenceFormulas
                        } else {
                            sorted.take(20)
                        }
                    }
                formulaEntries.forEachIndexed { formulaIndex, (formula, metric) ->
                    // 输出规则：仅当formula包含恰好两个原子时
                    val atomsInFormula = listOfNotNull(formula.atom1, formula.atom2, formula.atom3)
                    val otherAtoms = atomsInFormula.filter { it != atom }

                    val formulaString = otherAtoms.joinToString(",") { it.toString() }
                        .replace("\"", "\\\"").replace("\n", "\\n")
                    writer.write("    \"$formulaString\": $metric")
                    if (formulaIndex < formulaEntries.size - 1) writer.write(",")
                    writer.write("\n")


                    if (atomsInFormula.size == 2) {
                        val other = if (atomsInFormula[0] == atom) atomsInFormula[1]
                                    else if (atomsInFormula[1] == atom) atomsInFormula[0]
                                    else null
                        if (other != null) {
                            val ruleLine = "${metric.bodySize}\t${metric.support.toInt()}\t${metric.confidence}\t${atom.getRuleString()} <= ${other.getRuleString()}"
                            ruleWriter.write(ruleLine)
                            ruleWriter.write("\n")
                        }
                    }
                }

                writer.write("  }")
                if (atomIndex < atomEntries.size - 1) writer.write(",")
                writer.write("\n")

                // 每处理100个atom就flush一次，避免内存积累
                if (atomIndex % 100 == 0) {
                    writer.flush()
                    ruleWriter.flush()
                    println("Processed ${atomIndex + 1}/${atomEntries.size} atoms...")
                }
            }
            writer.write("}\n")
            }
        }

        println("Successfully saved atom2formula2metric to ${outputFile.absolutePath}")
        println("Successfully saved rules to ${outputRule.absolutePath}")
        println("Total atoms: ${atom2formula2metric.size}")
        println("Total formulas: ${atom2formula2metric.values.sumOf { it.size }}")
    }
}