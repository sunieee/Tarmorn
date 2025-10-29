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
    var DEBUG_LEVEL = 0

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

    const val MIN_CONF = 0
    const val MIN_SUPP = 20
    const val MAX_PATH_LENGTH = 3
    const val ESTIMATE_RATIO = 0.8

    // MinHash parameters: MH_DIM = BANDS * R
    const val MH_DIM = 256
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
                tails.map { tail -> pairHash32(head, tail) }
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
//            checkSpecificRelationBuckets()

            // 保存atom2formula2metric到JSON文件
            saveAtom2Formula2MetricToJson()
            
            // 打印内存诊断信息
            printMemoryDiagnostics()
            
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

                if (activeCount < Settings.WORKER_THREADS / 2) {
                    println("FORCING SHUTDOWN: 1/2 threads have finished")
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
                val supp = computeSupp(connectedPath)

                if (supp >= MIN_SUPP) {
                    // 检查反向关系对的连接结果
                   if (IdManager.getInverseRelation(r1) == ri) {
                       debug1("[runTask] Successfully connected inverse pair: ${IdManager.getRelationString(r1)}, supp: $supp")
                   }
                    
                    relationQueue.offer(connectedPath)
                    val cnt = addedCount.incrementAndGet()

                    // Per-item log (keeps queue flowing)
                     logWorkerResult(connectedPath, supp)

                    //  || activeThreadCount.get() < Settings.WORKER_THREADS
                    val remaining = relationQueue.size
                    if (cnt % 100 == 0) {
                        println("Thread $threadId: Added $cnt new paths; latest supp=$supp; TODO: $remaining remaining in queue")
                    }
                    if (activeThreadCount.get() < Settings.WORKER_THREADS / 4) {
                        debug2("Thread $threadId: Added $cnt new paths; latest supp=$supp; TODO: $remaining remaining in queue")
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
     * 根据路径长度选择不同的计算方法
     */
    fun computeSupp(rp: Long): Int {
        val pathLength = RelationPath.getLength(rp)
        
        return when (pathLength) {
            2 -> computeSuppLength2(rp)
            3 -> computeSuppLength3(rp)
            else -> throw IllegalArgumentException("Unsupported path length: $pathLength")
        }
    }

    /**
     * 计算长度为2的路径支持度
     * 使用原有的连接算法
     */
    private fun computeSuppLength2(rp: Long): Int {
        // 分解路径: rp = r1 · r2
        val relations = RelationPath.decode(rp)
        val r1 = relations[0]
        val r2 = relations[1]
        
        // Get tail entities of r1 (these become connecting entities)
        val r1TailEntities = r2tSet[r1]!!
        // Get head entities for r2
        val r2HeadEntities = R2h2supp[r2]?.keys ?: emptySet()

        // Find intersection of possible connecting entities (connection nodes)
        val connectingEntities = r1TailEntities.asSequence()
            .filter { it in r2HeadEntities }

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
            val r1HeadEntities = R2h2tSet[IdManager.getInverseRelation(r1)]?.get(connectingEntity) ?: emptySet()

            // Get tail entities reachable from this connecting entity via r2
            val r2TailEntities = R2h2tSet[r2]?.get(connectingEntity) ?: emptySet()

            // Create Cartesian product, avoiding duplicates
            for (r1Head in r1HeadEntities) {
                for (r2Tail in r2TailEntities) {
                    if (r1Head != r2Tail) { // Object Entity Constraint: X != Y
                        require(r1Head != connectingEntity && connectingEntity != r2Tail) {
                            "Connection node $connectingEntity should not equal head $r1Head or tail $r2Tail"
                        }
                        val pairHash32Value = pairHash32(r1Head, r2Tail)
                        if (instanceSet.add(pairHash32Value)) {
                            inverseSet.add(pairHash32(r2Tail, r1Head))
                            h2tSet.getOrPut(r1Head) { mutableSetOf() }.add(r2Tail)
                            t2hSet.getOrPut(r2Tail) { mutableSetOf() }.add(r1Head)
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

        // 存储支持度
        val inverseRp = RelationPath.getInverseRelation(rp)
        R2supp[rp] = size
        if (rp != inverseRp) R2supp[inverseRp] = size

        if (size >= MIN_SUPP) {
            val (forwardSuccess, inverseSuccess) = atomizeBinaryRelationPath(rp, size, instanceSet, inverseSet)

            if (forwardSuccess) R2h2supp[rp] = h2supp
            if (inverseSuccess) R2h2supp[inverseRp] = t2supp

            R2h2tSet[rp] = h2tSet
        }

        return size
    }

    /**
     * 计算长度为3的路径支持度
     * 使用两种拆分方式的交集
     */
    private fun computeSuppLength3(rp: Long): Int {
        debug2("computeSuppLength3: rp=${IdManager.getRelationString(rp)}")
        
        // 分解路径: rp = r1 · r2 · r3
        val relations = RelationPath.decode(rp)
        val r1 = relations[0]
        val r2 = relations[1]
        val r3 = relations[2]
        
        // 方式一: r1 · (r2·r3)
        val r2r3 = RelationPath.connectHead(r2, r3)
        // 方式二: INVERSE_r3 · (INVERSE_r2·INVERSE_r1)
        val invR3 = IdManager.getInverseRelation(r3)
        val invR2 = IdManager.getInverseRelation(r2)
        val invR1 = IdManager.getInverseRelation(r1)
        val invR2R1 = RelationPath.connectHead(invR2, invR1)
        
        // 检查两个复合路径是否都存在于R2h2supp中
        if (!R2h2supp.containsKey(r2r3) || !R2h2supp.containsKey(invR2R1)) {
            debug2("  Required paths not found in R2h2supp (r2·r3: ${R2h2supp.containsKey(r2r3)}, INVERSE_r2·INVERSE_r1: ${R2h2supp.containsKey(invR2R1)}), returning 0")
            val inverseRp = RelationPath.getInverseRelation(rp)
            R2supp[rp] = 0
            if (rp != inverseRp) R2supp[inverseRp] = 0
            return 0
        }
        
        // 先计算方式二，得到约束集合
        val (instances2, inverses2) = computeJoinLength1AndPathPairs(invR3, invR2R1)
        debug2("  Method 2 (INVERSE_r3 · INVERSE_r2·INVERSE_r1): ${instances2.size} instances")
        
        // 方式一：在连接过程中直接使用方式二的结果进行过滤
        val (instances1, inverses1) = computeJoinLength1AndPathPairs(r1, r2r3, inverses2.toSet(), instances2.toSet())
        debug2("  Method 1 (r1 · r2·r3) with filtering: ${instances1.size} instances")
        
        val size = instances1.size
        
        // 存储支持度
        val inverseRp = RelationPath.getInverseRelation(rp)
        R2supp[rp] = size
        if (rp != inverseRp) R2supp[inverseRp] = size

        if (size >= MIN_SUPP) {
            atomizeBinaryRelationPath(rp, size, instances1.toMutableSet(), inverses1.toMutableSet())
        }

        return size
    }

    /**
     * 连接单个关系(长度为1)与路径(长度为2)
     * 返回: Pair<IntArray, IntArray> 分别代表instanceSet和inverseSet（已经pairHash32）
     * @param filterInstances 可选的过滤集合，只保留在此集合中的instance
     * @param filterInverses 可选的过滤集合，只保留在此集合中的inverse
     */
    private fun computeJoinLength1AndPathPairs(
        r1: Long, 
        path: Long, 
        filterInstances: Set<Int>? = null,
        filterInverses: Set<Int>? = null
    ): Pair<IntArray, IntArray> {
        require(RelationPath.getLength(r1) == 1) { "r1 must be length 1" }
        require(RelationPath.getLength(path) == 2) { "path must be length 2" }
        
        // 获取r1的tail实体（连接节点）
        val r1TailEntities = r2tSet[r1]!!
        // 获取path的head实体
        val pathHeadEntities = R2h2supp[path]?.keys ?: emptySet()
        
        // 找到连接节点
        val connectingEntities = r1TailEntities.asSequence()
            .filter { it in pathHeadEntities }
        
        val instanceList = mutableListOf<Int>()
        val inverseList = mutableListOf<Int>()
        
        for (connectingEntity in connectingEntities) {
            // 获取能通过r1到达connectingEntity的head实体
            val r1HeadEntities = R2h2tSet[IdManager.getInverseRelation(r1)]?.get(connectingEntity) ?: emptySet()
            
            // 获取从connectingEntity通过path能到达的tail实体
            val pathTailEntities = R2h2tSet[path]?.get(connectingEntity) ?: emptySet()
            
            // 笛卡尔积，排除 x == z，并根据过滤集合进行过滤
            for (x in r1HeadEntities) {
                for (z in pathTailEntities) {
                    if (x != z) {
                        require(x != connectingEntity && connectingEntity != z) {
                            "Connection node $connectingEntity should not equal head $x or tail $z"
                        }

                        val inverseHash = pairHash32(z, x)
                        if (filterInverses != null && inverseHash !in filterInverses) continue
                        
                        val instanceHash = pairHash32(x, z)
                        if (filterInstances != null && instanceHash !in filterInstances) continue
                        
                        instanceList.add(instanceHash)
                        inverseList.add(inverseHash)
                    }
                }
            }
        }
        
        return Pair(instanceList.toIntArray(), inverseList.toIntArray())
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

    // 建议：把 MH_DIM 设为 2 的幂（例如 256/512），使位运算更快
// private const val MH_DIM = 200 // 你已有

    // 这两个种子建议在进程启动时固定或随机化一次（奇数种子更好）
    private const val OPH_SEED_BIN  = 0x9e3779b9.toInt() // 决定“落到哪个桶”
    private const val OPH_SEED_RANK = 0x85ebca6b.toInt() // 决定“该元素在桶内的秩值”
    private const val DOPH_SALT     = 0x165667b1.toInt()

    private inline fun pos32(x: Int) = x and 0x7fffffff

    /**
     * OPH + DOPH（32位、零分配、O(|S| + k)）
     */
    fun computeMinHashDOPH(instanceSet: Set<Int>, isBinary: Boolean = false): IntArray {
        if (instanceSet.isEmpty()) {
            throw IllegalArgumentException("Cannot compute MinHash for empty instance set")
        }

        val k = MH_DIM
        val sig = IntArray(k) { Int.MAX_VALUE }
        require((k and (k - 1)) == 0) { "MH_DIM must be a power of 2" }
        val mask = k - 1

        // 一次遍历：对每个元素只做两次 32 位哈希
        for (e in instanceSet) {
            // hBin: 决定桶 id
            val hBin  = computeUnaryHash(e, OPH_SEED_BIN)
            val binId = pos32(hBin) and mask

            // hRank: 决定该元素在该桶的秩值（越小越好）
            // 这里把 binId 混入，避免不同桶的 rank 值相关
            val hRank = computeUnaryHash(e, OPH_SEED_RANK) xor (binId * DOPH_SALT)
            val rank  = pos32(mix32(hRank))

            if (rank < sig[binId]) sig[binId] = rank
        }

        // DOPH：致密化空桶（把空桶用“下一非空桶的值 ^ 偏移”填上，循环寻找）
        // 这样能消除 OPH 的空桶偏差，并保持估计稳定
        if (sig.any { it == Int.MAX_VALUE }) {
            for (i in 0 until k) {
                if (sig[i] != Int.MAX_VALUE) continue
                var j = 1
                // 找到右侧最近的非空桶（环形）
                while (j < k && sig[(i + j) % k] == Int.MAX_VALUE) j++
                if (j == k) {
                    // 极端情况：所有桶都空（理论上 instanceSet 非空时不该发生）
                    // 给个确定性值
                    sig[i] = pos32(mix32(i * DOPH_SALT + 1))
                } else {
                    val donorIdx = (i + j) % k
                    // 计算与 i/j 相关的偏移，避免多个空桶复制出完全相同的值
                    val offset = pos32(mix32(i * DOPH_SALT + j))
                    sig[i] = sig[donorIdx] xor offset
                }
            }
        }

        if (isBinary) {
            for (i in 0 until k) sig[i] = -sig[i]
        }
        return sig
    }



    fun pairHash(entity1: Int, entity2: Int): Int {
        // 直接实现Pair的hashCode原理，避免创建对象
        // 33550337 is a prime number, and the 6th perfect number + 1
//        return entity1 * 33550337 + entity2
        return entity1 * 307 + entity2
    }

    fun pairHash32(h: Int, t: Int): Int {
        val uH = h * -0x61c88647     // 0x9E3779B9 的补码（黄金比例常数）
        val uT = t * 0x85ebca6b.toInt()
        return uH xor Integer.rotateLeft(uT, 16)
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
        var hash = pairHash32(entity1, entity2)
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
//        val hash = pairHash32(entity, seed)
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
        val minHashSignature = computeMinHashDOPH(instanceSet, currentAtom.isBinary)
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
            // if (currentAtom.isL1Atom) {
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
            // if (!bucketAtom.isHeadAtom) return@forEach // 只考虑headAtom进行组合
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

            if (metric.estimateValid) {
                if (validateAndCreateFormula(currentAtom, bucketAtom, instanceSet, jaccard) != null) cnt++
            }
            if (metric.inverse().estimateValid && currentAtom.isHeadAtom) {
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
    // TODO: 这里 originalMetric 意味着 headAtom必须是formula的第二个，因此bodySize需要重新算
    // TODO: 需要有一个map记录已经计算过的formula
    fun performLSHforL2Formula(formula: Formula, instanceSet: Set<Int>, originalMetric: Metric): Int {
        // 动态计算formula的支持度
        val mySupp = instanceSet.size
        val minHashSignature = computeMinHashDOPH(instanceSet, formula.isBinary)

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
            if (atom == formula.atom1 || atom == formula.atom2) return@forEach // 跳过相同原子，避免重复组合
            
            // 直接使用碰撞次数计算Jaccard相似度
            val jaccard = bucketCount.toDouble() / BANDS
            
            // 动态计算支持度
            val atomInstances = atom.getInstanceSet()
            val supp = atomInstances.size
            
            // 估计交集大小
            var intersectionSize = estimateIntersectionSize(jaccard, mySupp, supp)
            var metric = Metric(jaccard, intersectionSize, supp, mySupp)

            if (metric.estimateValid) {
                // 精确验证，并创建二元公式组合
                intersectionSize = instanceSet.intersect(atomInstances).size.toDouble()
                metric = Metric(jaccard, intersectionSize, supp, mySupp)
                if (metric.valid && metric.betterThan(originalMetric)) {
                    val newFormula = Formula(atom1 = formula.atom1, formula.atom2, atom)
                    val formula2metric = atom2formula2metric.computeIfAbsent(atom) { ConcurrentHashMap() }
                    formula2metric[newFormula] = metric
                    
                    cnt++
                }
            }
        }
//        if (cnt > 0) println("Found $cnt valid combinations for L2Formula $formula:")
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
        lateinit var intersectionSet: Set<Int>
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
            
            intersectionSet = newInstanceSet.intersect(atomInstances)
            intersectionSize = intersectionSet.size.toDouble()
            metric = Metric(jaccard, intersectionSize, supp, newInstanceSet.size)
            debug2("validateAndCreateFormula: unary rule, constant=$constant, newInstanceSet.size=${newInstanceSet.size}, intersectionSize=$intersectionSize, metric=$metric")
        } else {
            intersectionSet = instanceSet.intersect(atomInstances)
            intersectionSize = intersectionSet.size.toDouble()
            metric = Metric(jaccard, intersectionSize, supp, mySupp)
            debug2("validateAndCreateFormula: binary/general, intersectionSize=$intersectionSize, metric=$metric")
        }

        if (!metric.valid) return null

        val newFormula = Formula(atom1 = currentAtom, atom2 = bucketAtom)
        
        // 添加到结果映射 - 线程安全
        val formula2metric = atom2formula2metric.computeIfAbsent(bucketAtom) { ConcurrentHashMap() }
        formula2metric[newFormula] = metric
        debug2("validateAndCreateFormula: newFormula=$newFormula, metric=$metric")

//        performLSHforL2Formula(newFormula, intersectionSet, metric)
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
                    // 不截取，直接输出！
                    // .let { sorted ->
                    //     // 保留confidence >= 0.6的formula，或者前20个（取较多者）
                    //     val highConfidenceFormulas = sorted.filter { it.value.confidence >= 0.6 }
                    //     if (highConfidenceFormulas.size >= 20) {
                    //         highConfidenceFormulas
                    //     } else {
                    //         sorted.take(20)
                    //     }
                    // }
                formulaEntries.forEachIndexed { formulaIndex, (formula, metric) ->
                    // 输出规则：仅当formula包含恰好两个原子时
                    val atomsInFormula = listOfNotNull(formula.atom1, formula.atom2, formula.atom3)
                    val otherAtoms = atomsInFormula.filter { it != atom }
                    require(otherAtoms.isNotEmpty()) { "Error: No other atoms found. Atom: $atom, Formula: $formula" }

                    val formulaString = otherAtoms.joinToString(",") { it.toString() }
                        .replace("\"", "\\\"").replace("\n", "\\n")
                    writer.write("    \"$formulaString\": $metric")
                    if (formulaIndex < formulaEntries.size - 1) writer.write(",")
                    writer.write("\n")


                    val ruleBody = otherAtoms.joinToString(",") { atom -> atom.getRuleString() }
                    val ruleLine = "${metric.bodySize}\t${metric.support.toInt()}\t${metric.confidence}\t${atom.getRuleString()} <= ${ruleBody}"
                    ruleWriter.write(ruleLine)
                    ruleWriter.write("\n")

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

    /**
     * 打印内存诊断信息 - 检查各个数据结构的大小
     */
    private fun printMemoryDiagnostics() {
        println("\n" + "=".repeat(80))
        println("MEMORY DIAGNOSTICS - Data Structure Sizes")
        println("=".repeat(80))
        
        // 1. 基础数据结构
        println("\n[1] Basic Data Structures:")
        println("  R2supp.size = ${R2supp.size}")
        println("  R2h2tSet.size = ${R2h2tSet.size}")
        println("  R2h2supp.size = ${R2h2supp.size}")
        println("  r2instanceSet.size = ${r2instanceSet.size}")
        println("  r2tSet.size = ${r2tSet.size}")
        println("  r2loopSet.size = ${r2loopSet.size}")
        println("  relationL1.size = ${relationL1.size}")
        
        // 2. R2h2tSet 详细信息
        println("\n[2] R2h2tSet Details:")
        val r2h2tSetTotalHeads = R2h2tSet.values.sumOf { it.size }
        val r2h2tSetTotalTails = R2h2tSet.values.sumOf { h2tMap -> h2tMap.values.sumOf { it.size } }
        println("  Total relation paths: ${R2h2tSet.size}")
        println("  Total head entities: $r2h2tSetTotalHeads")
        println("  Total (head, tail) pairs: $r2h2tSetTotalTails")
        println("  Average heads per relation: ${r2h2tSetTotalHeads.toDouble() / R2h2tSet.size}")
        println("  Average tails per head: ${r2h2tSetTotalTails.toDouble() / r2h2tSetTotalHeads}")
        
        // 估算 R2h2tSet 内存占用
        val r2h2tSetMemory = estimateR2h2tSetMemory(R2h2tSet.size, r2h2tSetTotalHeads, r2h2tSetTotalTails)
        println("  Estimated memory usage: ${r2h2tSetMemory} MB")
        println("  Memory breakdown:")
        println("    - Outer HashMap (${R2h2tSet.size} entries): ${(R2h2tSet.size * 48.0 / 1024 / 1024).format(2)} MB")
        println("    - Inner HashMaps (${r2h2tSetTotalHeads} total): ${(r2h2tSetTotalHeads * 48.0 / 1024 / 1024).format(2)} MB")
        println("    - MutableSets (${r2h2tSetTotalHeads} sets): ${(r2h2tSetTotalHeads * 32.0 / 1024 / 1024).format(2)} MB")
        println("    - Integer objects (${r2h2tSetTotalTails} tail integers): ${(r2h2tSetTotalTails * 16.0 / 1024 / 1024).format(2)} MB")
        
        // 3. r2instanceSet 详细信息
        println("\n[3] r2instanceSet Details:")
        val r2instanceSetTotal = r2instanceSet.values.sumOf { it.size }
        println("  Total relations: ${r2instanceSet.size}")
        println("  Total instances: $r2instanceSetTotal")
        println("  Average instances per relation: ${r2instanceSetTotal.toDouble() / r2instanceSet.size}")
        
        // 估算 r2instanceSet 内存占用
        val r2instanceSetMemory = estimateMapOfSetsMemory(r2instanceSet.size, r2instanceSetTotal)
        println("  Estimated memory usage: ${r2instanceSetMemory} MB")
        
        // 4. LSH 相关结构
        println("\n[4] LSH Structures:")
        println("  formula2supp.size = ${formula2supp.size}")
        println("  minHashRegistry.size = ${minHashRegistry.size}")
        println("  key2headAtom.size = ${key2headAtom.size}")
        
        val key2headAtomTotalAtoms = key2headAtom.values.sumOf { it.size }
        println("  Total atoms in LSH buckets: $key2headAtomTotalAtoms")
        println("  Average atoms per bucket: ${key2headAtomTotalAtoms.toDouble() / key2headAtom.size}")
        
        val bucketSizes = key2headAtom.values.map { it.size }
        println("  Bucket size - Min: ${bucketSizes.minOrNull() ?: 0}")
        println("  Bucket size - Max: ${bucketSizes.maxOrNull() ?: 0}")
        println("  Bucket size - Median: ${bucketSizes.sorted().getOrNull(bucketSizes.size / 2) ?: 0}")
        
        // 估算 LSH 结构内存
        val minHashRegistryMemory = (minHashRegistry.size * (48 + MH_DIM * 4).toDouble() / 1024 / 1024).format(2)
        println("  Estimated memory - minHashRegistry: $minHashRegistryMemory MB")
        val key2headAtomMemory = ((key2headAtom.size * 48 + key2headAtomTotalAtoms * 64).toDouble() / 1024 / 1024).format(2)
        println("  Estimated memory - key2headAtom: $key2headAtomMemory MB")
        
        // 5. atom2formula2metric 详细信息
        println("\n[5] atom2formula2metric Details:")
        println("  Total atoms: ${atom2formula2metric.size}")
        val totalFormulas = atom2formula2metric.values.sumOf { it.size }
        println("  Total formulas: $totalFormulas")
        println("  Average formulas per atom: ${totalFormulas.toDouble() / atom2formula2metric.size}")
        
        val formulasPerAtom = atom2formula2metric.values.map { it.size }
        println("  Formulas per atom - Min: ${formulasPerAtom.minOrNull() ?: 0}")
        println("  Formulas per atom - Max: ${formulasPerAtom.maxOrNull() ?: 0}")
        println("  Formulas per atom - Median: ${formulasPerAtom.sorted().getOrNull(formulasPerAtom.size / 2) ?: 0}")
        
        // 估算 atom2formula2metric 内存
        val atom2formula2metricMemory = ((atom2formula2metric.size * 48 + totalFormulas * 128).toDouble() / 1024 / 1024).format(2)
        println("  Estimated memory usage: $atom2formula2metricMemory MB")
        
        // 找出拥有最多公式的前10个原子
        val top10Atoms = atom2formula2metric.entries
            .sortedByDescending { it.value.size }
            .take(10)
        println("\n  Top 10 atoms with most formulas:")
        top10Atoms.forEachIndexed { index, (atom, formulas) ->
            println("    ${index + 1}. $atom -> ${formulas.size} formulas")
        }
        
        // 6. 队列和计数器
        println("\n[6] Processing Statistics:")
        println("  relationQueue.size = ${relationQueue.size}")
        println("  processedCount = ${processedCount.get()}")
        println("  addedCount = ${addedCount.get()}")
        println("  activeThreadCount = ${activeThreadCount.get()}")
        
        // 7. 内存使用情况
        println("\n[7] JVM Memory Usage:")
        val runtime = Runtime.getRuntime()
        val usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024)
        val maxMemory = runtime.maxMemory() / (1024 * 1024)
        val totalMemory = runtime.totalMemory() / (1024 * 1024)
        val freeMemory = runtime.freeMemory() / (1024 * 1024)
        
        println("  Used Memory: ${usedMemory} MB")
        println("  Free Memory: ${freeMemory} MB")
        println("  Total Memory: ${totalMemory} MB")
        println("  Max Memory: ${maxMemory} MB")
        println("  Memory Usage: ${(usedMemory * 100.0 / maxMemory).format(2)}%")
        
        // 估算各数据结构内存总和
        println("\n[7.1] Estimated Memory Breakdown:")
        val r2h2tSetMem = estimateR2h2tSetMemory(R2h2tSet.size, r2h2tSetTotalHeads, r2h2tSetTotalTails).toDouble()
        val r2instanceSetMem = estimateMapOfSetsMemory(r2instanceSet.size, r2instanceSetTotal).toDouble()
        val minHashMem = (minHashRegistry.size * (48 + MH_DIM * 4).toDouble() / 1024 / 1024)
        val key2headAtomMem = ((key2headAtom.size * 48 + key2headAtomTotalAtoms * 64).toDouble() / 1024 / 1024)
        val atom2formula2metricMem = ((atom2formula2metric.size * 48 + totalFormulas * 128).toDouble() / 1024 / 1024)
        
        println("  R2h2tSet: ${r2h2tSetMem.format(2)} MB (${(r2h2tSetMem * 100 / usedMemory).format(1)}%)")
        println("  r2instanceSet: ${r2instanceSetMem.format(2)} MB (${(r2instanceSetMem * 100 / usedMemory).format(1)}%)")
        println("  minHashRegistry: ${minHashMem.format(2)} MB (${(minHashMem * 100 / usedMemory).format(1)}%)")
        println("  key2headAtom: ${key2headAtomMem.format(2)} MB (${(key2headAtomMem * 100 / usedMemory).format(1)}%)")
        println("  atom2formula2metric: ${atom2formula2metricMem.format(2)} MB (${(atom2formula2metricMem * 100 / usedMemory).format(1)}%)")
        val totalEstimated = r2h2tSetMem + r2instanceSetMem + minHashMem + key2headAtomMem + atom2formula2metricMem
        println("  Total Estimated: ${totalEstimated.format(2)} MB (${(totalEstimated * 100 / usedMemory).format(1)}% of used memory)")
        println("  Other/Overhead: ${(usedMemory - totalEstimated).format(2)} MB")
        
        // 8. 可能的内存问题识别
        println("\n[8] Potential Memory Issues:")
        val issues = mutableListOf<String>()
        
        if (r2h2tSetTotalTails > 10_000_000) {
            issues.add("⚠ R2h2tSet has ${r2h2tSetTotalTails} (head,tail) pairs - may cause memory overflow")
        }
        
        if (totalFormulas > 1_000_000) {
            issues.add("⚠ atom2formula2metric has ${totalFormulas} formulas - may cause memory overflow")
        }
        
        if (key2headAtomTotalAtoms > 100_000) {
            issues.add("⚠ LSH buckets contain ${key2headAtomTotalAtoms} atoms - may cause memory overflow")
        }
        
        val maxFormulaCount = formulasPerAtom.maxOrNull() ?: 0
        if (maxFormulaCount > 10_000) {
            issues.add("⚠ Some atoms have up to ${maxFormulaCount} formulas - highly skewed distribution")
        }
        
        val maxBucketSize = bucketSizes.maxOrNull() ?: 0
        if (maxBucketSize > 1_000) {
            issues.add("⚠ Some LSH buckets have up to ${maxBucketSize} atoms - poor hash distribution")
        }
        
        if (usedMemory > maxMemory * 0.9) {
            issues.add("⚠ Memory usage at ${(usedMemory * 100.0 / maxMemory).format(2)}% - critical level")
        }
        
        if (issues.isEmpty()) {
            println("  ✓ No obvious memory issues detected")
        } else {
            issues.forEach { println("  $it") }
        }
        
        println("\n" + "=".repeat(80))
        println("END OF MEMORY DIAGNOSTICS")
        println("=".repeat(80) + "\n")
    }
    
    // 辅助函数：格式化Double为指定小数位数
    private fun Double.format(digits: Int) = "%.${digits}f".format(this)
    
    /**
     * 估算 R2h2tSet 的内存占用
     * 结构: MutableMap<Long, MutableMap<Int, MutableSet<Int>>>
     */
    private fun estimateR2h2tSetMemory(outerMapSize: Int, totalInnerMaps: Int, totalTails: Int): String {
        // JVM对象内存估算（64位系统，压缩指针）：
        // - HashMap entry: ~48 bytes (对象头16 + key 8 + value 8 + hash 4 + next 8 + 对齐4)
        // - HashSet entry: ~32 bytes (对象头16 + 数组引用8 + size等字段8)
        // - Integer对象: ~16 bytes (对象头12 + int值4)
        // - Long对象: ~24 bytes (对象头12 + long值8 + 对齐4)
        
        val outerMapMemory = outerMapSize * 48.0  // 外层HashMap entries
        val outerMapLongKeys = outerMapSize * 24.0  // Long keys
        val innerMapsMemory = totalInnerMaps * 48.0  // 内层HashMap entries (每个head一个)
        val innerMapIntKeys = totalInnerMaps * 16.0  // Int keys (heads)
        val setsMemory = totalInnerMaps * 32.0  // MutableSet对象
        val tailIntegers = totalTails * 16.0  // tail的Integer对象
        
        val totalBytes = outerMapMemory + outerMapLongKeys + innerMapsMemory + innerMapIntKeys + setsMemory + tailIntegers
        val totalMB = totalBytes / 1024 / 1024
        
        return totalMB.format(2)
    }
    
    /**
     * 估算 Map<K, MutableSet<V>> 的内存占用
     */
    private fun estimateMapOfSetsMemory(mapSize: Int, totalElements: Int): String {
        val mapMemory = mapSize * 48.0  // HashMap entries
        val keysMemory = mapSize * 24.0  // Long keys (假设key是Long)
        val setsMemory = mapSize * 32.0  // MutableSet对象
        val elementsMemory = totalElements * 16.0  // 元素Integer对象
        
        val totalBytes = mapMemory + keysMemory + setsMemory + elementsMemory
        val totalMB = totalBytes / 1024 / 1024
        
        return totalMB.format(2)
    }
}