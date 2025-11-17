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

    const val MAX_JOIN_INSTANCES = 500_000
    const val MIN_CONF = 0
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

    private val POISON = Long.MIN_VALUE
    val processedCount = AtomicInteger(0)
    val addedCount = AtomicInteger(0)

    // 流式计算结构 - 使用线程安全的ConcurrentHashMap
    val formula2supp = ConcurrentHashMap<Formula, Int>()          // 公式→支持度映射
    val minHashRegistry = ConcurrentHashMap<Formula, IntArray>()           // 公式→MinHash映射
    val key2headAtom = ConcurrentHashMap<Int, MutableList<MyAtom>>() // 一级LSH桶：key -> atoms
    val atom2formula2metric = ConcurrentHashMap<MyAtom, ConcurrentHashMap<Formula, Metric>>() // 原子→公式→度量映射

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
        println("Settings.MIN_SUPP: $Settings.MIN_SUPP, MAX_PATH_LENGTH: $MAX_PATH_LENGTH")

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
            if (tripleSet.size >= Settings.MIN_SUPP) {
                relationQueue.offer(relation)
                addedCount.incrementAndGet()
                debug2("[path] ${IdManager.getRelationString(relation)}: ${tripleSet.size}")

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

                if (supp >= Settings.MIN_SUPP) {
                    // 检查反向关系对的连接结果
                   if (IdManager.getInverseRelation(r1) == ri) {
                       debug1("[runTask] Successfully connected inverse pair: ${IdManager.getRelationString(r1)}, supp: $supp")
                   }
                    
                    relationQueue.offer(connectedPath)
                    val cnt = addedCount.incrementAndGet()
                    debug2("[path] ${IdManager.getRelationString(connectedPath)}: $supp")

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
            val prevRp = R2supp.putIfAbsent(rp, -1)
            if (prevRp != null) {
                return null
            }
        } else {
            val prevRp = R2supp.putIfAbsent(rp, -1)
            val prevInv = R2supp.putIfAbsent(inverseRp, -1)
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
                        if (instanceSet.size > MAX_JOIN_INSTANCES) {
                            // 认为这条路径极端稠密，直接放弃
                            println("  L2 relation exceeded MAX_JOIN_INSTANCES ($MAX_JOIN_INSTANCES), r1=${IdManager.getRelationString(r1)}, path=${IdManager.getRelationString(r2)}")
                            return 0
                        }

                    } else {
                        loopSet.add(r1Head)
                    }
                }
            }
        }
        
        // Calculate support counts for heads and tails
        for ((head, tails) in h2tSet) h2supp[head] = tails.size
        for ((tail, heads) in t2hSet) t2supp[tail] = heads.size
        val size = instanceSet.size

        // 存储支持度
        val inverseRp = RelationPath.getInverseRelation(rp)
        R2supp[rp] = size
        if (rp != inverseRp) R2supp[inverseRp] = size

        if (size >= Settings.MIN_SUPP) {
            atomizeBinaryRelationPath(rp, size, instanceSet, inverseSet)
            atomizeUnaryRelationPath(rp, h2tSet, t2hSet, loopSet)
            R2h2supp[rp] = h2supp
            R2h2supp[inverseRp] = t2supp
            R2h2tSet[rp] = h2tSet
            // 应该需要保存t2h map！ 
            R2h2tSet[inverseRp] = t2hSet
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
        
        val inverseRp = RelationPath.getInverseRelation(rp)
        R2supp[rp] = 0
        if (rp != inverseRp) R2supp[inverseRp] = 0

        // 检查两个复合路径是否都存在于R2h2supp中
//        if (!R2h2supp.containsKey(r2r3) || !R2h2supp.containsKey(invR2R1)) {
        // if ((R2supp[r2r3]?: -1) == -1) computeSuppLength2(r2r3)
        // if ((R2supp[invR2R1]?: -1) == -1) computeSuppLength2(invR2R1)

        if ((R2supp[r2r3]?: -1) < Settings.MIN_SUPP || (R2supp[invR2R1]?: -1) < Settings.MIN_SUPP) {
            debug1("  Required paths not valid (r2·r3: ${R2supp[r2r3]}, INVERSE_r2·INVERSE_r1: ${R2supp[invR2R1]}), returning 0")
            return 0
        }
        
        // 先计算方式二，得到约束集合
        val (instances2, inverses2) = computeJoinLength1AndPathPairs(invR3, invR2R1)
        debug2("  Method 2 (INVERSE_r3 · INVERSE_r2·INVERSE_r1): ${instances2.size} instances")
        if (instances2.size < Settings.MIN_SUPP) return 0

        // 对instances2和inverses2排序，以便后续使用二分查找
        instances2.sort()
        inverses2.sort()
        
        // 方式一：在连接过程中直接使用方式二的结果进行过滤（使用排序数组+二分查找）
        val (instances1, inverses1) = computeJoinLength1AndPathPairs(r1, r2r3, inverses2, instances2)
        debug2("  Method 1 (r1 · r2·r3) with filtering: ${instances1.size} instances")
        val size = instances1.size
        if (size < Settings.MIN_SUPP) return 0

        R2supp[rp] = size
        if (rp != inverseRp) R2supp[inverseRp] = size
        // Avoid boxing and HashSet creation: consume primitive arrays directly
        atomizeBinaryRelationPath(rp, size, instances1, inverses1)

        return size
    }

    /**
     * 连接单个关系(长度为1)与路径(长度为2)
     * 返回: Pair<IntArray, IntArray> 分别代表instanceSet和inverseSet（已经pairHash32）
     * @param filterInstances 可选的过滤数组（已排序），只保留在此数组中的instance，使用二分查找
     * @param filterInverses 可选的过滤数组（已排序），只保留在此数组中的inverse，使用二分查找
     */
    private fun computeJoinLength1AndPathPairs(
        r1: Long, 
        path: Long, 
        filterInstances: IntArray? = null,
        filterInverses: IntArray? = null
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
        
        val instanceSet = mutableSetOf<Int>()
        val inverseSet = mutableSetOf<Int>()
        
        for (connectingEntity in connectingEntities) {
            // 获取能通过r1到达connectingEntity的head实体
            val r1HeadEntities = R2h2tSet[IdManager.getInverseRelation(r1)]?.get(connectingEntity) ?: emptySet()
            
            // 获取从connectingEntity通过path能到达的tail实体
            val pathTailEntities = R2h2tSet[path]?.get(connectingEntity) ?: emptySet()
            
            // 笛卡尔积，排除 x == z，并根据过滤集合进行过滤（使用二分查找）
            for (x in r1HeadEntities) {
                for (z in pathTailEntities) {
                    if (x != z) {
                        require(x != connectingEntity && connectingEntity != z) {
                            "Connection node $connectingEntity should not equal head $x or tail $z"
                        }

                        val inverseHash = pairHash32(z, x)
                        // 使用二分查找替代Set.contains，避免创建HashSet
                        if (filterInverses != null && filterInverses.binarySearch(inverseHash) < 0) continue
                        
                        val instanceHash = pairHash32(x, z)
                        // 使用二分查找替代Set.contains，避免创建HashSet
                        if (filterInstances != null && filterInstances.binarySearch(instanceHash) < 0) continue
                        
                        instanceSet.add(instanceHash)
                        if (instanceSet.size > MAX_JOIN_INSTANCES) {
                            // 认为这条路径极端稠密，直接放弃
                            println("  L3 relation exceeded MAX_JOIN_INSTANCES ($MAX_JOIN_INSTANCES), r1=${IdManager.getRelationString(r1)}, path=${IdManager.getRelationString(path)}")
                            return Pair(IntArray(0), IntArray(0))
                        }
                        inverseSet.add(inverseHash)
                    }
                }
            }
        }
        
        return Pair(instanceSet.toIntArray(), inverseSet.toIntArray())
    }

    /**
     * 处理Binary原子化：r(X,Y) 和 r'(X,Y)
     * 直接使用预计算的MinHash签名
     */
    fun atomizeBinaryRelationPath(rp: Long, supp: Int, instanceSet: MutableSet<Int>, inverseSet: MutableSet<Int>) {
        debug2("atomizeBinaryRelationPath: rp=$rp, supp=$supp, instanceSet.size=${instanceSet.size}, inverseSet.size=${inverseSet.size}")

        val inverseRp = RelationPath.getInverseRelation(rp)
        // 1. r(X,Y): Binary Atom with relation path rp
        val binaryAtom = MyAtom(rp, IdManager.getYId()) // Y表示二元原子
        // 2. r'(X,Y): Binary Atom with inverse relation path
        val inverseBinaryAtom = MyAtom(inverseRp, IdManager.getYId())
        performLSH(binaryAtom, instanceSet)
        performLSH(inverseBinaryAtom, inverseSet)
    }
    
    /**
     * Overload: atomize Binary relation path using primitive arrays to avoid boxing and HashSet creation.
     */
    fun atomizeBinaryRelationPath(rp: Long, supp: Int, instanceArray: IntArray, inverseArray: IntArray) {
        debug2("atomizeBinaryRelationPath[primitive]: rp=$rp, supp=$supp, instances=${instanceArray.size}, inverses=${inverseArray.size}")
        val inverseRp = RelationPath.getInverseRelation(rp)
        val binaryAtom = MyAtom(rp, IdManager.getYId())
        val inverseBinaryAtom = MyAtom(inverseRp, IdManager.getYId())
        // Reuse Set-based LSH with lightweight IntArraySet wrappers to avoid boxing-heavy HashSet materialization
        val instanceView = IntArraySet(instanceArray)
        val inverseView = IntArraySet(inverseArray)
        performLSH(binaryAtom, instanceView)
        performLSH(inverseBinaryAtom, inverseView)
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
            if (supp >= Settings.MIN_SUPP) {
                val unaryAtom = MyAtom(rp, constant)
                if (RelationPath.isL1Relation(rp)) {
                    val formula = Formula(unaryAtom)
                    val metric = Metric(0.0, supp.toDouble(), supp, R2supp[rp]!!)
                    val formula2metric = atom2formula2metric.getOrPut(unaryAtom) { ConcurrentHashMap() }
                    formula2metric[formula] = metric
                }
                // 生成Unary实例集合：所有能到达constant的head实体
                val unaryInstanceSet = t2hSet[constant]
                if (unaryInstanceSet != null) {
                    performLSH(unaryAtom, unaryInstanceSet)
                }
            }
        }
        
        // 4. r(X,·): Unary Atom for existence - relation rp has head entities
        val headEntityCount = R2h2supp[rp]?.size ?: 0
        if (headEntityCount >= Settings.MIN_SUPP) {
            val existenceAtom = MyAtom(rp, 0) // 0表示存在性原子"·"
            // 生成Existence实例集合：所有head实体
            val existenceInstanceSet = h2tSet.keys
            performLSH(existenceAtom, existenceInstanceSet)
        }
        
        // 5. r(c,X) / r'(X,c): Unary Atom for each constant c where r(c,X) exists
        R2h2supp[rp]?.forEach { (constant, supp) ->
            if (supp >= Settings.MIN_SUPP) {
                val inverseUnaryAtom = MyAtom(inverseRp, constant)
                // 生成逆Unary实例集合：从constant出发能到达的tail实体
                if (RelationPath.isL1Relation(rp)) {
                    val formula = Formula(inverseUnaryAtom)
                    val metric = Metric(0.0, supp.toDouble(), supp, R2supp[inverseRp]!!)
                    val formula2metric = atom2formula2metric.getOrPut(inverseUnaryAtom) { ConcurrentHashMap() }
                    formula2metric[formula] = metric
                }
                val inverseUnaryInstanceSet = h2tSet[constant]
                if (inverseUnaryInstanceSet != null) {
                    // TODO: h2tSet 中可能没有该constant的映射
                    performLSH(inverseUnaryAtom, inverseUnaryInstanceSet)
                }
            }
        }
        
        // 6. r(·,X) / r'(X,·): Unary Atom for existence - inverse relation has head entities
        val inverseTailEntityCount = R2h2supp[inverseRp]?.size ?: 0
        if (inverseTailEntityCount >= Settings.MIN_SUPP) {
            val inverseExistenceAtom = MyAtom(inverseRp, 0)
            // 生成逆Existence实例集合：所有tail实体
            val inverseExistenceInstanceSet = t2hSet.keys
            performLSH(inverseExistenceAtom, inverseExistenceInstanceSet)
        }

        // 7. r(X,X): Unary Atom for loops - r(X,X) exists
        if (loopSet.size >= Settings.MIN_SUPP) {
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
    fun performLSH(currentAtom: MyAtom, instanceSet: Set<Int>) {
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

            // 估计交集大小 (head=currentAtom, body=bucketAtom)
            var intersectionSize = estimateIntersectionSize(jaccard, mySupp, supp)
            // 原实现将 headSize 置为 bucketAtom 的支持度，bodySize 为 currentAtom，导致日志中显示反转。
            // 为与打印格式 "currentAtom <= bucketAtom" 对齐：headSize 应为 currentAtom 的支持度(mySupp)，bodySize 为 bucketAtom 的支持度(supp)。
            var metric = Metric(jaccard, intersectionSize, mySupp, supp)

            if (metric.estimateValid) {
                if (validateAndCreateFormula(currentAtom, bucketAtom, instanceSet, jaccard) != null) cnt++


                if (currentAtom.isHeadAtom) {
                    if (validateAndCreateFormula(bucketAtom, currentAtom, bucketAtom.getInstanceSet(), jaccard) != null) cnt++
                } else if (currentAtom.isL1Atom && currentAtom.isBinary) {
                    val bucketInverse = bucketAtom.inverse()
                    // 注意这里不能只验证 headAtom，因为 currentAtom 可能是 Binary & L1Atom: current'(X,Y) <= bucket(X,Y)
                    if (validateAndCreateFormula(bucketInverse, currentAtom.inverse(), bucketInverse.getInstanceSet(), jaccard) != null) cnt++
                }
            }

        }
    }


    /**
     * Lightweight primitive int array builder to avoid boxing and minimize GC pressure.
     */
    private class IntArrayBuilder(initialCapacity: Int = 1024) {
        private var data = IntArray(initialCapacity)
        private var size = 0
        fun add(value: Int) {
            if (size == data.size) {
                val newCap = if (data.size < 1) 1 else data.size shl 1
                data = data.copyOf(newCap)
            }
            data[size++] = value
        }
        fun toIntArray(): IntArray = data.copyOf(size)
    }

    /**
     * Read-only Set view over an IntArray to avoid HashSet materialization.
     */
    private class IntArraySet(private val data: IntArray) : AbstractSet<Int>() {
        override val size: Int get() = data.size
        override fun contains(element: Int): Boolean {
            // Linear scan; avoids building hash structures. Suitable for one-off validation.
            for (v in data) if (v == element) return true
            return false
        }
        override fun iterator(): Iterator<Int> = object : Iterator<Int> {
            private var idx = 0
            override fun hasNext(): Boolean = idx < data.size
            override fun next(): Int = data[idx++]
        }
    }

    /**
     * LSH分桶 - 专门用于L2Formula，从key2headAtom中查找相关原子进行组合
     * 使用一级桶正确估计Jaccard相似度
     */
    // TODO: 这里 originalMetric 意味着 headAtom必须是formula的第二个，因此bodySize需要重新算
    // TODO: 需要有一个map记录已经计算过的formula
    fun performLSHforL2Formula(formula: Formula, instanceSet: Set<Int>, originalMetric: Metric) {
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
     * @param bodyAtom 当前原子
     * @param headAtom 目标原子
     * @param bodyInstances 实例集合
     * @param jaccard Jaccard相似度
     * @return 如果创建成功返回公式，否则返回null
     */
    private fun validateAndCreateFormula(
        bodyAtom: MyAtom,
        headAtom: MyAtom,
        bodyInstances: Set<Int>,
        jaccard: Double
    ): Formula? {
        debug2("validateAndCreateFormula: myAtom=$bodyAtom, atom=$headAtom, instanceSet.size=${bodyInstances.size}, jaccard=$jaccard")
        // 动态计算支持度
        val headInstances = headAtom.getInstanceSet()
        val headSupp = headInstances.size
        debug2("validateAndCreateFormula: headInstances.size=$headSupp, bodySupp=${bodyInstances.size}")
        
        // 自证式一元规则（entity-anchored unary rules）问题
        // if (myAtom.isL2Atom && !myAtom.isBinary) {  这种写法有问题，会漏掉L1Atom的情况
        var newInstances = bodyInstances
        if (!bodyAtom.isL1Atom && !bodyAtom.isBinary) {
            val constant = headAtom.entityId
            val inverseRelation = IdManager.getInverseRelation(bodyAtom.firstRelation)
            val t2hSet = ts.r2h2tSet[inverseRelation]
            if (t2hSet == null) {
                println("Warning: Missing t2hSet for relation ${IdManager.getRelationString(inverseRelation)}")
            }
            // 关系稀疏时，可能不存在t2hSet
            if (t2hSet != null && t2hSet[constant] != null) {
                newInstances = bodyInstances.filter { !t2hSet[constant]!!.contains(it) }.toSet()
            }
        }
        var intersectionSet = newInstances.intersect(headInstances)
        var intersectionSize = intersectionSet.size.toDouble()
        // 二元/一般情况：currentAtom 左侧 (head)，bucketAtom 右侧 (body)
        var metric = Metric(jaccard, intersectionSize, headSupp, newInstances.size)
        debug2("validateAndCreateFormula: headSize=$headSupp, bodySize=${newInstances.size}, intersectionSize=$intersectionSize, metric=$metric")
        
        if (!metric.valid) return null

        val newFormula = Formula(atom1 = bodyAtom, atom2 = headAtom)
        
        // 添加到结果映射 - 线程安全
        val formula2metric = atom2formula2metric.computeIfAbsent(headAtom) { ConcurrentHashMap() }
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
        // val outDir = File("out/" + Settings.DATASET)
        // outDir.mkdirs() // 确保out目录存在
        val outputFile = File(Settings.PATH_RULES_JSON)
        val outputRule = File(Settings.PATH_RULES_TXT)
        
        // 统计变量
        var totalRules = 0
        val unaryStats = IntArray(MAX_PATH_LENGTH + 1) // L0, L1, L2, L3
        val binaryStats = IntArray(MAX_PATH_LENGTH + 1) // L0, L1, L2, L3
        
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
                    // if (otherAtoms.isEmpty()) println("Found Z rule: ${atom.getRuleString()} <=")
                    val formulaString = otherAtoms.joinToString(",") { it.toString() }
                        .replace("\"", "\\\"").replace("\n", "\\n")
                    writer.write("    \"$formulaString\": $metric")
                    if (formulaIndex < formulaEntries.size - 1) writer.write(",")
                    writer.write("\n")

                    val ruleBody = otherAtoms.joinToString(",") { atom -> atom.getRuleString() }
                    val ruleLine = "${metric.bodySize}\t${metric.support.toInt()}\t${metric.confidence}\t${atom.getRuleString()} <= ${ruleBody}"
                    ruleWriter.write(ruleLine)
                    ruleWriter.write("\n")
                    
                    // 统计规则
                    totalRules++
                    val bodyLength = otherAtoms.size
                    if (bodyLength <= MAX_PATH_LENGTH) {
                        if (atom.isBinary) {
                            binaryStats[bodyLength]++
                        } else {
                            unaryStats[bodyLength]++
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
        
        // 打印规则统计
        println("Total rules: $totalRules")
        println("Type     L0       L1       L2       L3")
        println("-" .repeat(60))
        println("Unary    ${unaryStats[0].toString().padStart(8)}  ${unaryStats[1].toString().padStart(8)}  ${unaryStats[2].toString().padStart(8)}  ${unaryStats[3].toString().padStart(8)}")
        println("Binary   ${binaryStats[0].toString().padStart(8)}  ${binaryStats[1].toString().padStart(8)}  ${binaryStats[2].toString().padStart(8)}  ${binaryStats[3].toString().padStart(8)}")
    }
}