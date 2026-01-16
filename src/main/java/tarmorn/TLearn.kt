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
import tarmorn.structure.TLearn.MyAtom
import tarmorn.structure.TLearn.UnaryAtom
import tarmorn.structure.TLearn.BinaryAtom
import tarmorn.structure.TLearn.Formula
import tarmorn.structure.TLearn.Metric
import kotlin.random.Random


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

    const val MAX_PATH_LENGTH = 3
    const val ESTIMATE_RATIO = 0.6
    const val MIN_LIFT = 1.2
    const val MIN_COMMON_BUCKET = 2
    const val MAX_BUCKET_ATTEMPT = 100
    const val MIN_COMMON_EVIDENCE = 30
    const val MIN_SURPRISAL_LIFT = 0.1
    const val TOP_K_RULE_COMBO = 200
    // const val MIN_SURPRISAL_DEGRADE = 0.2

    // MinHash parameters: MH_DIM = BANDS * R
    const val MH_DIM = 256
    const val R = 1  // 每band维度
    const val BANDS = MH_DIM / R
    // Core data structures
    val config = Settings.load()    // 加载配置
    val ts: TripleSet = TripleSet(Settings.PATH_TRAINING, true)
    // lateinit var r2tripleSet: MutableMap<Long, MutableSet<MyTriple>>
    lateinit var R2supp: ConcurrentHashMap<Long, Int>
    // 仅有2跳及以下的relation path才存储完整的头尾实体对
    lateinit var R2h2tSet: ConcurrentHashMap<Long, MutableMap<Int, MutableSet<Int>>>

    // 小写的r标识relationL1，并且在初始化后不再修改
    lateinit var r2instanceSet: MutableMap<Long, MutableSet<Long>>
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
    val relationL3ThreadCount = AtomicInteger(0)
    val addedCount = AtomicInteger(0)

    // 流式计算结构 - 使用线程安全的ConcurrentHashMap
    val H2B2metric = ConcurrentHashMap<MyAtom<*>, ConcurrentHashMap<MyAtom<*>, Metric>>() // headAtom -> bodyAtom -> metric
    val key2atoms = ConcurrentHashMap<Int, MutableList<MyAtom<*>>>() // 一级LSH桶：key -> atoms
    val H2F2metric = ConcurrentHashMap<MyAtom<*>, ConcurrentHashMap<Formula, Metric>>() // 原子→公式→度量映射

    // Statistics variables
    var totalRules = 0
    val unaryStats = IntArray(4) // M0, M1, M2, M3
    val binaryStats = IntArray(4) // M0, M1, M2, M3
    
    // Lift statistics for composition phase
    val unaryPositiveLift = AtomicInteger(0)
    val unaryNegativeLift = AtomicInteger(0)
    val binaryPositiveLift = AtomicInteger(0)
    val binaryNegativeLift = AtomicInteger(0)

    /**
     * Main entry point - can be run directly
     */
    @JvmStatic
    fun main(args: Array<String>) {
        Settings.load()
        println("TLearn - Top-down relation path learning algorithm")
        println("Loading triple set...")

        // Initialize data structures
        // r2tripleSet = ts.r2tripleSet
        r2loopSet = ts.r2loopSet
        // 复制一份ts.r2h2tSet，避免直接引用
        R2h2tSet = ConcurrentHashMap(ts.r2h2tSet.mapValues { entry ->
            entry.value.mapValues { it.value.toMutableSet() }.toMutableMap()
        })
        R2supp = ConcurrentHashMap(ts.r2tripleSet.mapValues { it.value.size })
        r2instanceSet = R2h2tSet.mapValues { entry ->
            entry.value.flatMap { (head, tails) ->
                tails.map { tail -> (head.toLong() shl 32) or (tail.toLong() and 0xFFFFFFFFL) }
            }.toMutableSet()
        }.toMutableMap()

        println("Starting TLearn algorithm...")
        println("Settings.MIN_SUPP: ${Settings.MIN_SUPP}, MAX_PATH_LENGTH: $MAX_PATH_LENGTH")

        // Step 1: Initialize with L=1 relations
        initializeRelationL1()

        // Step 2: Connect relations using multiple threads
        try {
           connectRelations()
        } catch (e: Exception) {
            println("Error during relation connection: ${e.message}")
            e.printStackTrace()
        }
        
        println("Atomization phase completed.")
        println("Total relation paths: ${R2supp.size}")
        println("Total atoms in H2B2metric: ${H2B2metric.size}")
        printLSHBuckets()

        saveMetricToJson(
            metricMap = H2B2metric,
            outputPath = Settings.PATH_H2B2metric,
            appendMode = false,
            isFormulaMap = false
        )
        
        println("\n=== Phase 2: Composition ===")
        // Step 3: Composition phase - combine atoms into formulas using Eclat
        try {
            compositionPhase()
        } catch (e: Exception) {
            println("Error during composition phase: ${e.message}")
            e.printStackTrace()
        } finally {
            println("\nTLearn algorithm completed.")
            
            saveMetricToJson(
                metricMap = H2F2metric,
                outputPath = Settings.PATH_H2F2metric,
                appendMode = true,
                isFormulaMap = true
            )
        }

        // Print rule statistics
        println("Total rules: $totalRules")
        println("Type     M0       M1       M2       M3")
        println("-" .repeat(60))
        println("Unary    ${unaryStats[0].toString().padStart(8)}  ${unaryStats[1].toString().padStart(8)}  ${unaryStats[2].toString().padStart(8)}  ${unaryStats[3].toString().padStart(8)}")
        println("Binary   ${binaryStats[0].toString().padStart(8)}  ${binaryStats[1].toString().padStart(8)}  ${binaryStats[2].toString().padStart(8)}  ${binaryStats[3].toString().padStart(8)}")
        
        // Print lift statistics for composition phase
        println("\nComposition Phase - Lift Statistics:")
        println("-" .repeat(60))
        println("Type     Positive Lift    Negative Lift    Total")
        val unaryTotal = unaryPositiveLift.get() + unaryNegativeLift.get()
        val binaryTotal = binaryPositiveLift.get() + binaryNegativeLift.get()
        println("Unary    ${unaryPositiveLift.get().toString().padStart(13)}    ${unaryNegativeLift.get().toString().padStart(13)}    ${unaryTotal.toString().padStart(8)}")
        println("Binary   ${binaryPositiveLift.get().toString().padStart(13)}    ${binaryNegativeLift.get().toString().padStart(13)}    ${binaryTotal.toString().padStart(8)}")
        println("Total    ${(unaryPositiveLift.get() + binaryPositiveLift.get()).toString().padStart(13)}    ${(unaryNegativeLift.get() + binaryNegativeLift.get()).toString().padStart(13)}    ${(unaryTotal + binaryTotal).toString().padStart(8)}")
    }

    /**
     * Step 1: Initialize level 1 relations (single relations with sufficient supp)
     */
    fun initializeRelationL1() {
        println("Initializing level 1 relations with ${Settings.WORKER_THREADS} threads...")

        val threadPool = Executors.newFixedThreadPool(Settings.WORKER_THREADS)
        val relations = ts.r2tripleSet.entries.toList()
        
        try {
            val futures = relations.map { (relation, tripleSet) ->
                threadPool.submit {
                    R2supp[relation] = tripleSet.size
                    val relationInv = RelationPath.getInverseRelation(relation)
                    val entitySupp = minOf(
                        R2h2tSet[relation]?.size ?: 0,
                        R2h2tSet[relationInv]?.size ?: 0
                    )
                    if (entitySupp >= Settings.MIN_ENTITY_SUPP) {
                    // if (tripleSet.size >= Settings.MIN_SUPP) {
                    // 注意不要用 supp 过滤，而是用 entity supp 过滤
                        relationQueue.offer(relation)
                        addedCount.incrementAndGet()
                        debug1("[path] ${IdManager.getRelationString(relation)}, supp: ${tripleSet.size}, entitySupp: $entitySupp")

                        if (!IdManager.isInverseRelation(relation)) {
                            // 为L=1关系进行原子化，直接使用R2h2tSet中的反向索引
                            val h2tSet = R2h2tSet[relation]
                            val t2hSet = R2h2tSet[relationInv]
                            
                            if (h2tSet != null && t2hSet != null) {
                                // 处理Binary原子，不再需要构建inverseSet
                                atomizeBinaryRelationL1(relation, r2instanceSet[relation]!!)
                                // 处理Unary原子
                                atomizeUnaryRelationPath(relation, h2tSet.toMutableMap(), t2hSet.toMutableMap(), r2loopSet[relation] ?: mutableSetOf())
                            } else {
                                println("Warning: Missing h2tSet or t2hSet for relation ${IdManager.getRelationString(relation)} or its inverse ${IdManager.getRelationString(relationInv)}")
                            }
                        }
                    }
                }
            }
            
            // 等待所有任务完成
            futures.forEach { it.get() }
            
        } finally {
            threadPool.shutdown()
            threadPool.awaitTermination(1, TimeUnit.HOURS)
        }

        relationL1 = relationQueue.map { it }.toList()
        // 使用不可变快照，避免并发修改影响，并提升遍历效率
        r2tSet = relationL1.associateWith { r ->
            val inv = RelationPath.getInverseRelation(r)
            val keys = R2h2tSet[inv]?.keys ?: emptySet()
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
                    println("FORCING SHUTDOWN: 1/4 threads remaining")
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

            // 这里必须确认 relationL3 candidate数量 > = WORKER_THREADS，才能保证不会死锁
            if (length == 3) {
                val cnt = relationL3ThreadCount.incrementAndGet()
                while (cnt!= Settings.WORKER_THREADS) {
                    // 只有relationL2全部完成，才能继续处理relationL3
                    Thread.sleep(1)
                }
            }

            try {
                if (runTask(threadId, item)) break
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

    fun runTask(threadId: Int, ri: Long): Boolean {
        processedCount.incrementAndGet()

        // Step 4: Try connecting with all L1 relations (immediate enqueue per item)
        for (r1 in relationL1) {
            val rp = RelationPath.connectHead(r1, ri)
            if (createRelationPath(rp) && isValidRelationPath(rp)) {
                relationQueue.offer(rp)
                val cnt = addedCount.incrementAndGet()
                //  || activeThreadCount.get() < Settings.WORKER_THREADS
                val remaining = relationQueue.size
                if (cnt % 100 == 0) {
                    println("Thread $threadId: Added $cnt new paths; latest supp: ${R2supp[rp]}; TODO: $remaining remaining in queue")
                }
                if (activeThreadCount.get() < Settings.WORKER_THREADS / 4) {
                    println("Thread $threadId: Added $cnt new paths; latest supp: ${R2supp[rp]}; TODO: $remaining remaining in queue")
                    return true
                }
            }
        }
        return false
    }

    /**
     * Step 3 & 4: Attempt to connect (r1: relation, ri: relation path)，增加前缀
     * Returns the relation path ID if successful, null otherwise
     */
    fun createRelationPath(rp: Long): Boolean {
        val rpInv = RelationPath.getInverseRelation(rp)
        // 原子插入，避免全局同步：只有当 rp 和 rpInv 都是首次出现时才继续
        // 特殊处理：如果 rp == rpInv（自反路径），只检查一次
        if (rp == rpInv) {
            val prevRp = R2supp.putIfAbsent(rp, -1)
            if (prevRp != null) {
                return false
            }
        } else {
            val prevRp = R2supp.putIfAbsent(rp, -1)
            val prevInv = R2supp.putIfAbsent(rpInv, -1)
            if (prevRp != null && prevInv != null) {
                return false
            }
        }

        return true
    }

    /**
     * Step 4 & 5: Compute supp for a connected relation path
     * 根据路径长度选择不同的计算方法
     */
    fun isValidRelationPath(rp: Long): Boolean {
        val pathLength = RelationPath.getLength(rp)
        
        return when (pathLength) {
            2 -> isValidRelationPathL2(rp)
            3 -> isValidRelationPathL3(rp)
            else -> throw IllegalArgumentException("Unsupported path length: $pathLength")
        }
    }

    /**
     * 计算长度为2的路径支持度
     * 使用分层比例随机采样，确保每个CE按比例贡献instances
     */
    private fun isValidRelationPathL2(rp: Long): Boolean {
        // 分解路径: rp = r1 * r2
        val rpInv = RelationPath.getInverseRelation(rp)
        val relations = RelationPath.decode(rp)
        val r1 = relations[0]
        val r2 = relations[1]
        
        // Get tail entities of r1 (these become connecting entities)
        val r1TailEntities = r2tSet[r1]!!
        // Get head entities for r2
        val r2HeadEntities = R2h2tSet[r2]?.keys ?: emptySet()

        // Find intersection of possible connecting entities (connection nodes)
        val connectingEntities = r1TailEntities.asSequence()
            .filter { it in r2HeadEntities }
            .toList()

        fun setSupp(supp: Int) {
            R2supp[rp] = supp
            if (rp != rpInv) R2supp[rpInv] = supp
        }

        if (connectingEntities.size < Settings.MIN_ENTITY_SUPP) {
            debug2("[isValidRelationPathL2] Not enough connecting entities (${connectingEntities.size}) for rp=${IdManager.getRelationString(rp)}, returning false")
            setSupp(0)
            return false
        }

        // Initialize data structures
        val h2tSet = mutableMapOf<Int, MutableSet<Int>>()
        val t2hSet = mutableMapOf<Int, MutableSet<Int>>()
        val h2supp = mutableMapOf<Int, Int>()
        val t2supp = mutableMapOf<Int, Int>()
        val loopSet = mutableSetOf<Int>()
        val instanceSet = mutableSetOf<Long>()
        val random = Random((r1 xor r2).toLong())
        
        val t2hSet4r1 = R2h2tSet[RelationPath.getInverseRelation(r1)]!!
        val h2tSet4r2 = R2h2tSet[r2]!!
        
        // 辅助函数：添加一对实体
        fun tryAddPair(r1Head: Int, r2Tail: Int, connectingEntity: Int): Boolean {
            if (r1Head == r2Tail) {
                loopSet.add(r1Head)
                return false  // Object Entity Constraint: X != Y
            }
            
            require(r1Head != connectingEntity && connectingEntity != r2Tail) {
                "Connection node $connectingEntity should not equal head $r1Head or tail $r2Tail"
            }
            
            val pairLong = (r1Head.toLong() shl 32) or (r2Tail.toLong() and 0xFFFFFFFFL)
            if (instanceSet.add(pairLong)) {
                h2tSet.getOrPut(r1Head) { mutableSetOf() }.add(r2Tail)
                t2hSet.getOrPut(r2Tail) { mutableSetOf() }.add(r1Head)
                return true
            }
            return false
        }
        
        // === 分层比例随机采样 ===
        
        // 1. 收集每个CE的统计信息
        data class CEInfo(
            val ce: Int,
            val heads: List<Int>,
            val tails: List<Int>,
            val pairCount: Int
        )
        
        val ceInfos = connectingEntities.mapNotNull { ce ->
            val heads = (t2hSet4r1[ce] ?: emptySet()).toList().filter { h -> h != ce }
            val tails = (h2tSet4r2[ce] ?: emptySet()).toList().filter { t -> t != ce }
            if (heads.isEmpty() || tails.isEmpty()) {
                null
            } else {
                CEInfo(ce, heads, tails, heads.size * tails.size)
            }
        }
        
        if (ceInfos.isEmpty()) {
            setSupp(0)
            return false
        }
        
        val totalPairs = ceInfos.sumOf { it.pairCount }
        val targetTotal = minOf(Settings.MAX_JOIN_INSTANCES_L2, totalPairs.toInt())
        
        // 2. 为每个CE按比例分配配额并随机采样（打乱顺序避免前面CE占满）
        for (ceInfo in ceInfos.shuffled(random)) {
            // 计算该CE的理想配额（按比例）
            val ceQuota = (targetTotal * ceInfo.pairCount / totalPairs.toDouble()).toInt()
            var added = 0
            
            // 判断是否需要随机采样
            if (ceInfo.pairCount <= ceQuota * 2) {
                // 总量不大，全部遍历（随机顺序）
                val allPairs = ceInfo.heads.flatMap { h ->
                    ceInfo.tails.map { t -> Pair(h, t) }
                }.shuffled(random)
                
                for ((h, t) in allPairs) {
                    if (tryAddPair(h, t, ceInfo.ce)) {
                        added++
                        if (added >= ceQuota) break  // 达到配额，退出
                    }
                }
            } else {
                // 总量很大，随机采样
                var attempts = 0
                val maxAttempts = ceQuota * 10  // 允许碰撞重试
                
                while (added < ceQuota && attempts < maxAttempts) {
                    val h = ceInfo.heads.random(random)
                    val t = ceInfo.tails.random(random)
                    if (tryAddPair(h, t, ceInfo.ce)) {
                        added++
                    }
                    attempts++
                }
            }
        }
        
        // 3. 验证最小实体支持度
        val entitySupp = minOf(connectingEntities.size, minOf(h2tSet.size, t2hSet.size))
        if (entitySupp < Settings.MIN_ENTITY_SUPP) {
            debug1("[isValidRelationPathL2] entitySupp $entitySupp below threshold for rp=${IdManager.getRelationString(rp)}, returning false")
            setSupp(0)
            return false
        }
        
        val supp = instanceSet.size
        setSupp(supp)
        R2h2tSet[rp] = h2tSet
        if (rp != rpInv) R2h2tSet[rpInv] = t2hSet

        // 即使instance数量不足也有效（更长的连接），但不进行原子化
        if (supp >= Settings.MIN_SUPP) {
            performLSH(BinaryAtom(rp, IdManager.getYId(), instanceSet))
            // atomizeUnaryRelationPath(rp, h2tSet, t2hSet, loopSet)
            // L2 Uc, L2 Ud 没什么用，加上不会提升LP指标，反而拖慢速度
        }

        debug1("[isValidRelationPathL2] ${IdManager.getRelationString(rp)} supp: $supp, entitySupp: $entitySupp, self-inverse: ${rp == rpInv}, estimated: $totalPairs, sampled: $supp")

        return true
    }

    /**
     * 计算长度为3的路径支持度
     * 使用方式二进行连接，同时验证方式一的有效性
     */
    private fun isValidRelationPathL3(rp: Long): Boolean {
        debug2("isValidRelationPathL3: rp=${IdManager.getRelationString(rp)}")
        
        // 分解路径: rp = r1 * r2 * r3
        val relations = RelationPath.decode(rp)
        val r1 = relations[0]
        val r2 = relations[1]
        val r3 = relations[2]
        val r3Inv = RelationPath.getInverseRelation(r3)

        // 方式一: r1 * (r2*r3) = r1 * r23  通过 isForwardValid 用于验证
        // 方式二: r3Inv * (r2Inv*r1Inv) = r3Inv * r12Inv  用于构造
        val r23 = RelationPath.connectHead(r2, r3)
        val r23Inv = RelationPath.getInverseRelation(r23)
        val r12 = RelationPath.connectHead(r1, r2)
        val r12Inv = RelationPath.getInverseRelation(r12)
        val rpInv = RelationPath.getInverseRelation(rp)
        R2supp[rp] = 0
        if (rp != rpInv) R2supp[rpInv] = 0

        // 检查两个复合路径是否都存在且有效
        // if ((R2supp[r2r3]?: -1) < Settings.MIN_SUPP || (R2supp[r12Inv]?: -1) < Settings.MIN_SUPP) {
        // if ((R2EntitySupp[r2r3]?: -1) < Settings.MIN_ENTITY_SUPP || (R2EntitySupp[r12Inv]?: -1) < Settings.MIN_ENTITY_SUPP) {

        fun isValid(rp: Long): Boolean {
            val suppValue = R2supp[rp]
            val rpInv = RelationPath.getInverseRelation(rp)
            require(suppValue != null) {
                "R2supp missing for rp=${IdManager.getRelationString(rp)}"
            }
            require(R2supp[rpInv] == suppValue) {
                "R2supp inconsistent for rp=${IdManager.getRelationString(rp)} and its inverse"
            }
            if (suppValue == -1) {
                println("[isValidRelationPathL3] Warning: Detected incomplete supp for rp=${IdManager.getRelationString(rp)}, skip")
                // isValid(rp)
                return false
            }
            if (suppValue == 0) return false
            if (suppValue > 0) {
                require(R2h2tSet[rp] != null) {
                    "R2h2tSet missing for rp=${IdManager.getRelationString(rp)}"
                }
                require(R2h2tSet[rpInv] != null) {
                    "R2h2tSet missing for rpInv=${IdManager.getRelationString(rpInv)}"
                }
                return true
            }
            throw IllegalStateException("Unexpected supp value for rp=${IdManager.getRelationString(rp)}: $suppValue")
        }

        fun setSupp(supp: Int) {
            R2supp[rp] = supp
            if (rp != rpInv) R2supp[rpInv] = supp
        }

        // Validate the inverse relations we actually use (r23Inv and r12Inv)
        val r23Valid = isValid(r23)
        val r12InvValid = isValid(r12Inv)
        if (!r23Valid || !r12InvValid) {
            debug2("[isValidRelationPathL3] Required paths not valid: r23Valid=$r23Valid, r12InvValid=$r12InvValid for rp=${IdManager.getRelationString(rp)}, return false")
            setSupp(0)
            return false
        }

        // Cache R2h2tSet lookups with safe access to handle race conditions
        val h2tSet4r1 = R2h2tSet[r1]!!
        val t2hSet4r23 = R2h2tSet[r23Inv]!!
        val h2tSet4r12Inv = R2h2tSet[r12Inv]!!
        val t2hSet4r3Inv = R2h2tSet[r3]!!
        
        // 判断实例 (h, t) 是否通过方式一有效：r1 * (r2*r3)
        // 需要存在中间节点 y 使得 r1(h, y) 且 (r2*r3)(y, t)
        fun isForwardValid(h: Int, t: Int): Boolean {
            val r1Tails = h2tSet4r1.get(h) ?: return false  // r1(h, ?) 的所有尾节点
            val r23Heads = t2hSet4r23.get(t) ?: return false  // (r2*r3)(?, t) 的所有头节点
            
            // 检查是否有交集（存在共同的中间节点）
            for (tail in r1Tails) {
                if (tail in r23Heads && tail != h && tail != t) return true
            }
            return false
        }
        
        // 使用方式二进行连接：r3Inv * (r2Inv*r1Inv)
        val instanceSet = mutableSetOf<Long>()
        val random = Random((r1 xor r2 xor r3).toLong())
        
        // 辅助函数：添加一对实体（无返回值）
        fun tryAddPair(h: Int, t: Int, connectingEntity: Int): Boolean {
            if (h == t) return false  // Object Entity Constraint: X != Y
            
            require(h != connectingEntity && connectingEntity != t) {
                "Connection node $connectingEntity should not equal head $h or tail $t"
            }
            
            // 验证方式一是否也有效
            val pairLong = (h.toLong() shl 32) or (t.toLong() and 0xFFFFFFFFL)
            return isForwardValid(h, t) && instanceSet.add(pairLong)
        }
        
        // 获取 r3Inv 的 tail 实体（连接节点）
        val r3InvTailEntities = r2tSet[r3Inv]!!
        // 获取 r12Inv 的 head 实体
        val r12InvHeadEntities = h2tSet4r12Inv.keys ?: emptySet()
        
        // 找到连接节点
        val connectingEntities = r3InvTailEntities.asSequence()
            .filter { it in r12InvHeadEntities }
            .toList()
        
        if (connectingEntities.size < Settings.MIN_ENTITY_SUPP) {
            debug2("[isValidRelationPathL3] Not enough connecting entities (${connectingEntities.size}) for rp=${IdManager.getRelationString(rp)}, returning false")
            setSupp(0)
            return false
        }
        
        // === 分层比例随机采样 ===
        
        // 1. 收集每个CE的统计信息
        data class CEInfo(
            val ce: Int,
            val heads: List<Int>,
            val tails: List<Int>,
            val pairCount: Int
        )
        
        val ceInfos = connectingEntities.mapNotNull { ce ->
            val heads = (h2tSet4r12Inv[ce] ?: emptySet()).toList().filter { h -> h != ce }
            val tails = (t2hSet4r3Inv[ce] ?: emptySet()).toList().filter { t -> t != ce }
            if (heads.isEmpty() || tails.isEmpty()) {
                null
            } else {
                CEInfo(ce, heads, tails, heads.size * tails.size)
            }
        }
        
        if (ceInfos.isEmpty()) {
            setSupp(0)
            return false
        }
        
        val totalPairs = ceInfos.sumOf { it.pairCount }
        val targetTotal = minOf(Settings.MAX_JOIN_INSTANCES_L3, totalPairs.toInt())
        
        // 2. 为每个CE按比例分配配额并随机采样（打乱顺序避免前面CE占满）
        for (ceInfo in ceInfos.shuffled(random)) {
            // 计算该CE的配额（按比例）
            val ceQuota = (targetTotal * ceInfo.pairCount / totalPairs.toDouble()).toInt()
            
            if (ceQuota == 0) continue
            
            var added = 0
            
            // 判断是否需要随机采样
            if (ceInfo.pairCount <= ceQuota * 2) {
                // 总量不大，全部遍历（随机顺序）
                val allPairs = ceInfo.heads.flatMap { h ->
                    ceInfo.tails.map { t -> Pair(h, t) }
                }.shuffled(random)
                
                for ((h, t) in allPairs) {
                    if (tryAddPair(h, t, ceInfo.ce)) {
                        added++
                        if (added >= ceQuota) break  // 达到配额，退出
                    }
                }
            } else {
                // 总量很大，随机采样
                var attempts = 0
                val maxAttempts = ceQuota * 10  // 允许碰撞重试
                
                while (added < ceQuota && attempts < maxAttempts) {
                    val h = ceInfo.heads.random(random)
                    val t = ceInfo.tails.random(random)
                    if (tryAddPair(h, t, ceInfo.ce)) {
                        added++
                    }
                    attempts++
                }
            }
        }
        val supp = instanceSet.size
        setSupp(supp)
        debug2("[isValidRelationPathL3] ${IdManager.getRelationString(rp)} supp: $supp, self-inverse: ${rp == rpInv}, estimated: $totalPairs, sampled: $supp")
        
        // atomize 使用 supp 而不是 entity supp作为阈值
        if (supp >= Settings.MIN_SUPP)
            performLSH(BinaryAtom(rp, IdManager.getYId(), instanceSet))
        return true
    }

    /**
     * 处理Binary原子化：r(X,Y) 和 r'(X,Y)
     * 直接使用预计算的MinHash签名
     */
    fun atomizeBinaryRelationL1(rp: Long, instanceSet: MutableSet<Long>) {
        val rpInv = RelationPath.getInverseRelation(rp)
        // 1. r(X,Y): Binary Atom with relation path rp
        if (instanceSet.size >= Settings.MIN_SUPP)
            performLSH(BinaryAtom(rp, IdManager.getYId(), instanceSet))
        // 2. r'(X,Y): Binary Atom with inverse relation path
        // isInverseInstances 现在通过 isInverseRelation && isBinary && isL1Atom 自动计算
        if (instanceSet.size >= Settings.MIN_SUPP)
            performLSH(BinaryAtom(rpInv, IdManager.getYId(), instanceSet))
    }

    /**
     * 处理Unary原子化：r(X,c), r(X,*), r(c,X), r(*,X), r(X,X)
     * 需要动态计算MinHash签名
     * 注意：先处理 entityId=0 的存在性原子，再处理 entityId>0 的常量原子
     */
    fun atomizeUnaryRelationPath(rp: Long, h2tSet: MutableMap<Int, MutableSet<Int>>, t2hSet: MutableMap<Int, MutableSet<Int>>, loopSet: MutableSet<Int>) {
        val rpInv = RelationPath.getInverseRelation(rp)

        // 先处理 entityId = 0 的存在性原子，确保它们先被添加到 H2B2metric
        
        // 1. r(X,*): Unary Atom for existence - relation rp has head entities
        if (h2tSet.size >= Settings.MIN_SUPP)
            performLSH(UnaryAtom(rp, 0, h2tSet.keys))
        
        // 2. r(*,X) / r'(X,*): Unary Atom for existence - inverse relation has head entities
        if (t2hSet.size >= Settings.MIN_SUPP)
            performLSH(UnaryAtom(rpInv, 0, t2hSet.keys))
        
        // 后处理 entityId > 0 的常量原子，此时可以检查是否存在更好的存在性原子
        
        // 3. r(X,c): Unary Atom for each constant c where rp(X,c) exists
        t2hSet.forEach { (constant, unaryInstanceSet) -> 
            val supp = unaryInstanceSet.size
            if (supp >= Settings.MIN_SUPP) {
                val unaryAtom = UnaryAtom(rp, constant, unaryInstanceSet)
                performLSH(unaryAtom)
                if (RelationPath.isL1Relation(rp)) {
                    // setH2B2metric(unaryAtom, UnaryAtom(0, IdManager.getZId()), Metric(supp.toDouble(), supp, R2supp[rp]!!))
                    setH2F2metric(unaryAtom, Formula(), Metric(supp.toDouble(), supp, R2supp[rp]!!))
                }
            }
        }
        
        // 4. r(c,X) / r'(X,c): Unary Atom for each constant c where r(c,X) exists
        h2tSet.forEach { (constant, inverseUnaryInstanceSet) -> 
            val supp = inverseUnaryInstanceSet.size
            if (supp >= Settings.MIN_SUPP) {
                val inverseUnaryAtom = UnaryAtom(rpInv, constant, inverseUnaryInstanceSet)
                performLSH(inverseUnaryAtom)
                if (RelationPath.isL1Relation(rp)) {
                    // setH2B2metric(inverseUnaryAtom, UnaryAtom(0, IdManager.getZId()), Metric(supp.toDouble(), supp, R2supp[rpInv]!!))
                    setH2F2metric(inverseUnaryAtom, Formula(), Metric(supp.toDouble(), supp, R2supp[rpInv]!!))
                }
            }
        }

        // 5. r(X,X): Unary Atom for loops - r(X,X) exists
        if (loopSet.size >= Settings.MIN_SUPP)
            performLSH(UnaryAtom(rp, IdManager.getXId(), loopSet))
    }

    fun pairHash32(h: Int, t: Int): Int {
        val uH = h * -0x61c88647     // 0x9E3779B9 的补码（黄金比例常数）
        val uT = t * 0x85ebca6b.toInt()
        return uH xor Integer.rotateLeft(uT, 16)
    }

    /**
     * Helper function to set formula metric for an atom
     */
    fun setH2F2metric(atom: MyAtom<*>, formula: Formula, metric: Metric) {
        val F2metric = H2F2metric.computeIfAbsent(atom) { ConcurrentHashMap() }
        F2metric[formula] = metric
    }

    fun setH2B2metric(headAtom: MyAtom<*>, bodyAtom: MyAtom<*>, metric: Metric) {
        val B2metric = H2B2metric.computeIfAbsent(headAtom) { ConcurrentHashMap() }
        
        // 过滤逻辑：Uc vs Ud 规则
        // Uc: r(x,c) <= r1(x,c1)，bodyAtom.entityId > 0
        // Ud: r(x,c) <= r1(x,*)， bodyAtom.entityId = 0
        // 如果 conf(Ud) >= conf(Uc)，则 Uc 无效
        // 
        // 由于 atomizeUnaryRelationPath 保证先处理 entityId=0 再处理 entityId>0，
        // 所以只需要在添加常量原子时检查是否存在更好的存在性原子即可
        
        if (bodyAtom.entityId > 0) {
            // 当前是常量原子，检查是否存在更好的存在性原子
            val existenceAtom = B2metric.keys.find { 
                it.relationId == bodyAtom.relationId && it.entityId == 0 
            }
            if (existenceAtom != null) {
                val existenceMetric = B2metric[existenceAtom]!!
                // if (existenceMetric.confidence >= metric.confidence) {
                if (existenceMetric.confidence >= metric.confidence) {
                    // 存在性原子的 confidence 更好，不添加当前常量原子
                    // 这里不加等号从结果上来说更好：0.318-> 0.320. 原因：Ud权重比Uc更低，相同的conf的rule，Uc实际conf更高
                    // 0105：改成 >=，设置 d_weight=1 避免这个问题，解决规则冗余
                    // 0107: 改成 >，从结果上来看，设置d_weight=1降低了指标 325->322，但是cluster没用，改回来算了 
                    debug2("Filtered out constant atom $bodyAtom (conf=${metric.confidence}) due to better existence atom $existenceAtom (conf=${existenceMetric.confidence})")
                    return
                }
            }
        }
        
        B2metric[bodyAtom] = metric
    }

    /**
     * LSH bucketing - add atom to key2atoms buckets and update H2B2metric
     */
    fun performLSH(currentAtom: MyAtom<*>) {
        debug2("performLSH: Atom=$currentAtom, support=${currentAtom.support}")
        require(!currentAtom.minHashSignature.isEmpty()) {
            "performLSH: Empty MinHash signature for atom $currentAtom"
        }
        
        val relevantAtom2BucketCount = mutableMapOf<MyAtom<*>, Int>()
        
        // Step 1: Update relevantAtom2BucketCount by scanning existing buckets
        for (bandIndex in 0 until BANDS) {
            // 将 bandIndex 编码到 key 中，避免不同 band 的相同 signature 值冲突
            val key = (bandIndex shl 24) or (currentAtom.minHashSignature[bandIndex] and 0xFFFFFF)
            val bucket = key2atoms[key]
            if (bucket != null) {
                synchronized(bucket) {
                    bucket.forEach { existingAtom ->
                        // 避免同实体碰撞
                        if (existingAtom.entityId == currentAtom.entityId && !existingAtom.isBinary) return@forEach 
                        if (existingAtom.isHeadAtom || currentAtom.isHeadAtom) 
                        relevantAtom2BucketCount[existingAtom] = relevantAtom2BucketCount.getOrDefault(existingAtom, 0) + 1
                    }
                }
            }
        }
        
        // Step 2: Filter relevantAtom2BucketCount and only add to key2atoms if valid candidates exist
        var cnt = 0
        relevantAtom2BucketCount.forEach { (bucketAtom, bucketCount) ->
            // if (!bucketAtom.isHeadAtom) return@forEach // 只考虑headAtom进行组合
            // if (bucketCount < MIN_COMMON_BUCKET) return@forEach // 跳过碰撞次数过少的，避免噪声
            require(bucketAtom != currentAtom) {
                "performLSH: Self-collision detected for atom $currentAtom in bucket"
            }

            fun tryValidate() {
                if (bucketAtom.isHeadAtom && validateH2B(bucketAtom, currentAtom)) cnt++
                if (currentAtom.isHeadAtom && validateH2B(currentAtom, bucketAtom)) cnt++
            }
            
            // 如果body比较简单直接验证
            if (currentAtom.isL2Atom) tryValidate()
            else {
                // 直接使用碰撞次数计算Jaccard相似度：bucketCount / BANDS
                val jaccard = bucketCount.toDouble() / BANDS
                // 估计交集大小 (head=currentAtom, body=bucketAtom)
                var intersectionSize = estimateIntersectionSize(jaccard, currentAtom.support, bucketAtom.support)
                if (intersectionSize >= Settings.MIN_SUPP * ESTIMATE_RATIO) tryValidate()
            }
        }

        // IMPORTANT: 所有L1原子必须被添加到桶中以供后续组合
        // 如果只添加headAtom，导致 r(*) 如果先performLSH，会忽略掉与 r(c) 的组合
        if (!currentAtom.isL1Atom && cnt == 0) {
            // No valid candidates and not a L1 atom - skip adding this atom to buckets
            // currentAtom will be garbage collected as it's not referenced anywhere
            return
        }
        
        // Add currentAtom to key2atoms buckets only if there are valid relevant atoms
        for (bandIndex in 0 until BANDS) {
            // 将 bandIndex 编码到 key 中，避免不同 band 的相同 signature 值冲突
            val key = (bandIndex shl 24) or (currentAtom.minHashSignature[bandIndex] and 0xFFFFFF)
            val atomBucket = key2atoms.computeIfAbsent(key) { java.util.Collections.synchronizedList(mutableListOf()) }
            synchronized(atomBucket) { atomBucket.add(currentAtom) }
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

    private fun validateH2B(headAtom: MyAtom<*>, bodyAtom: MyAtom<*>): Boolean {
        // debug2("validateH2B: headAtom=$headAtom, bodyAtom=$bodyAtom")
        var head = headAtom
        var body = bodyAtom
        if (head.isBinary && IdManager.isInverseRelation(head.relationId)) {
            head = head.inverse()
            body = body.inverse()
        }
        if (H2B2metric[head]?.containsKey(body) == true) {
            // Already validated
            return false
        }
        
        // 自证式一元规则（entity-anchored unary rules）问题
        // if (myAtom.isL2Atom && !myAtom.isBinary) {  这种写法有问题，会漏掉L1Atom的情况
        val bodyInstances = if (!body.isL1Atom && !body.isBinary) {
            val constant = head.entityId
            val inverseRelation = RelationPath.getInverseRelation(body.firstRelation)
            val t2hSet = ts.r2h2tSet[inverseRelation]
            
            if (t2hSet != null && t2hSet[constant] != null) {
                body.instances.filter { !t2hSet[constant]!!.contains(it) }.toSet()
            } else {
                body.instances
            }
        } else {
            body.instances
        }

        var intersectionSet = bodyInstances.intersect(head.instances)
        var intersectionSize = intersectionSet.size.toDouble()
        // 二元/一般情况：headAtom 左侧 (head)，bodyAtom 右侧 (body)
        // Note: bodyInstances.size may differ from bodyAtom.support due to filtering
        var metric = Metric(intersectionSize, head.support, bodyInstances.size)
        // debug1("validateH2B: headAtom=$headAtom, bodyAtom=$bodyAtom, metric=$metric")
        
        if (metric.valid) {
            setH2B2metric(head, body, metric)
            return true
        }
        return false
    }

    /**
     * Composition Phase - combine frequent atom sets using Eclat algorithm
     * Called after Atomization completes, builds rules based on H2B2metric
     */
    fun compositionPhase() {
        println("Starting Composition Phase with Eclat algorithm...")
        
        val processedHeads = AtomicInteger(0)
        val totalHeads = H2B2metric.size
        val threadPool = Executors.newFixedThreadPool(Settings.WORKER_THREADS)
        val compositionActiveThreadCount = AtomicInteger(0)
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
            threadPool.awaitTermination(1, TimeUnit.HOURS)
        }
        
        println("Composition Phase completed. Total rules: ${H2F2metric.values.sumOf { it.size }}")
    }
    
    /**
     * Process single headAtom, perform pairwise combination of bodyAtoms
     */
    private fun processHeadAtom(headAtom: MyAtom<*>, bodyMap: ConcurrentHashMap<MyAtom<*>, Metric>) {
        if (bodyMap.size < 2) return  // Need at least 2 bodyAtoms to combine
        
        // Convert to list for pairwise iteration
        // extract rule with surprisal >= MIN_SURPRISAL_LIFT
        val newBodyMap = ConcurrentHashMap<MyAtom<*>, Metric>()
        for ((bodyAtom, metric) in bodyMap) {
            if (metric.surprisal >= MIN_SURPRISAL_LIFT) {
                newBodyMap[bodyAtom] = metric
            }
        }
        val bodyList = newBodyMap.entries.toList().sortedByDescending { it.value.confidence }
        
        debug1("processHeadAtom: $headAtom, bodyMap size=${bodyMap.size}, filtered size=${bodyList.size}")
        
        var pairCount = 0
        var validPairCount = 0

        // Pairwise combination: only combine (i, j) where i < j to avoid duplicates
        for (i in 0 until minOf(bodyList.size, TOP_K_RULE_COMBO)) {
            val (B1, metric1) = bodyList[i]
            val S_H1 = headAtom.instances.intersect(B1.instances)
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

                var S_H12_size = S_H1.intersect(B2.instances).size
                var S_12_size = 0

                if (S_H12_size < Settings.MIN_SUPP && (B1.isSampled || B2.isSampled)) {
                    // 采样情况：需要补充检查
                    val intersection = B1.instances.intersect(B2.instances).toMutableSet()
                    S_12_size = intersection.size

                    // 补充：检查 B1 独有的实例是否在 B2 中存在
                    val originalS_12_size = intersection.size
                    val originS_H12_size = S_H12_size

                    if (B2.isSampled)
                        for (s in B1.instances) {
                            if (s !in intersection && s != null && B2.hasInstance(s)) {
                                S_12_size++
                                if (s in headAtom.instances) S_H12_size++
                            }
                        }
                    // 补充：检查 B2 独有的实例是否在 B1 中存在
                    if (B1.isSampled && S_H12_size < Settings.MIN_SUPP)
                        for (s in B2.instances) {
                            if (s !in intersection && s != null && B1.hasInstance(s)) {
                                S_12_size++
                                if (s in headAtom.instances) S_H12_size++
                            }
                        }

                    if (S_H12_size < Settings.MIN_SUPP) {
                        continue  // Does not meet minimum support
                    }

                    debug1("[DEBUG] Supplemented intersection size for sampled atoms: B1: $B1, B2: $B2, S_12: ${originalS_12_size} -> ${S_12_size}, S_H12: ${originS_H12_size} -> ${S_H12_size}")
                } else {
                    // 非采样情况：直接计算 S_H12
                    if (S_H12_size < Settings.MIN_SUPP) {
                        continue  // Does not meet minimum support
                    }
                    // Calculate common evidence: intersection of two bodyAtom instances
                    S_12_size = B1.instances.intersect(B2.instances).size
                }

                // Create new metric with bodySize = |S_12|
                val metric = Metric(
                    support = S_H12_size.toDouble(),
                    headSize = headAtom.instances.size,
                    bodySize = S_12_size
                )

                // Calculate lift
                val lift = metric.surprisal - metric1.surprisal - metric2.surprisal
                // Only store if lift is significant
                if (lift > MIN_SURPRISAL_LIFT || lift < -maxOf(metric1.surprisal, metric2.surprisal)) {
                    val formula = Formula(B1, B2)
                    metric.lift = lift
                    setH2F2metric(headAtom, formula, metric)
                    validPairCount++

                    // Update lift statistics
                    if (headAtom.isBinary) {
                        if (lift > 0) binaryPositiveLift.incrementAndGet()
                        else binaryNegativeLift.incrementAndGet()
                    } else {
                        if (lift > 0) unaryPositiveLift.incrementAndGet()
                        else unaryNegativeLift.incrementAndGet()
                    }

                    debug2("Valid pair: $headAtom <= ${B1.getRuleString()} & ${B2.getRuleString()}, " +
                           "conf=${metric.confidence}, surprisal=${metric.surprisal}, lift=$lift, supp=${S_H12_size}")
                }
            }
        }

        if (pairCount > 0) {
            debug2("processHeadAtom completed: $headAtom, checked $pairCount pairs, found $validPairCount valid combinations")
        }
    }
    

    
    /**
     * Calculate Jaccard similarity using MinHash signatures
     * Jaccard ≈ (number of matching bands) / (total bands)
     */
    private fun calculateJaccardFromMinHash(atom1: MyAtom<*>, atom2: MyAtom<*>): Double {
        if (atom1.minHashSignature.isEmpty() || atom2.minHashSignature.isEmpty()) {
            return 0.0
        }
        
        var matchingBands = 0
        for (bandIndex in 0 until BANDS) {
            if (atom1.minHashSignature[bandIndex] == atom2.minHashSignature[bandIndex]) {
                matchingBands++
            }
        }
        
        return matchingBands.toDouble() / BANDS
    }


    /**
     * Print LSH bucketing results - adapted for single-level buckets, prevent concurrent modification
     */
    fun printLSHBuckets() {
        println("LSH Buckets Summary:")
        
        // Create snapshot to avoid concurrent modification
        val key2atomsSnapshot = synchronized(key2atoms) {
            key2atoms.mapValues { (_, atoms) ->
                atoms.toList() // 创建不可变副本
            }.toMap()
        }
        
        val allBuckets = key2atomsSnapshot.values
        println("Total buckets: ${allBuckets.size}")
        println("Total head atoms in H2B2metric: ${H2B2metric.size}")

        // Bucket size distribution statistics
        val bucketSizes = allBuckets.map { it.size }
        println("Bucket size distribution:")
        println("  Min: ${bucketSizes.minOrNull() ?: 0}")
        println("  Max: ${bucketSizes.maxOrNull() ?: 0}")
        println("  Average: ${bucketSizes.average()}")

        // Collect all buckets and classify by atom type
        val allBucketsWithInfo = key2atomsSnapshot.entries.map { (key, atoms) ->
            Pair(key, atoms)
        }
        
        // Define filter function to avoid code duplication
        fun isBinaryBucket(atoms: List<MyAtom<*>>) = atoms.first().entityId == IdManager.getYId()
        
        // Separate Binary and Unary buckets and display uniformly
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
     * Save metric map to JSON file - streaming output to avoid memory overflow
     * @param metricMap The metric map to save (H2B2metric or H2F2metric)
     * @param outputPath The output JSON file path
     * @param appendMode Whether to append to existing rules file (true for H2F, false for H2B)
     * @param isFormulaMap Whether the body type is Formula (true) or MyAtom (false)
     */
    private fun <T> saveMetricToJson(
        metricMap: ConcurrentHashMap<MyAtom<*>, ConcurrentHashMap<T, Metric>>,
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
        
        BufferedWriter(FileWriter(outputFile)).use { writer ->
            BufferedWriter(FileWriter(outputRule, appendMode)).use { ruleWriter ->
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
                            is MyAtom<*> -> body.getRuleString()
                            is Formula -> body.getRuleString()
                            else -> body.toString()
                        }
                        
                        // Write rule to text file with lift info for formulas
                        val liftInfo = if (isFormulaMap) metric.lift else metric.confidence
                        val ruleLine = "${metric.bodySize}\t${metric.support.toInt()}\t$liftInfo\t${atom.getRuleString()} <= $bodyRuleString"
                        ruleWriter.write(ruleLine)
                        ruleWriter.write("\n")

                        // Statistics for rules
                        totalRules++
                        if (isFormulaMap) {
                            val bodyLength = (body as Formula).size
                            if (bodyLength <= MAX_PATH_LENGTH) {
                                if (atom.isBinary) binaryStats[bodyLength]++
                                else unaryStats[bodyLength]++
                            }
                        } else {
                            if (atom.isBinary) binaryStats[1]++
                            else unaryStats[1]++
                        }
                    }

                    writer.write("  }")
                    if (atomIndex < atomEntries.size - 1) writer.write(",")
                    writer.write("\n")

                    if (atomIndex % 100 == 0) {
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
}