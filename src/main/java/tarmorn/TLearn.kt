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

    const val MAX_JOIN_INSTANCES_L2 = 6000
    const val MAX_JOIN_INSTANCES_L3 = 3000
    const val MIN_CONF = 0.001
    const val MAX_PATH_LENGTH = 3
    const val ESTIMATE_RATIO = 0.8
    const val IMPROVE_RATIO = 1.2
    const val MIN_COMMON_BUCKET = 2
    const val MAX_BUCKET_ATTEMPT = 100
    const val MAX_STACK_SIZE = 3

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
    val relationL3ThreadCount = AtomicInteger(0)
    val addedCount = AtomicInteger(0)

    // 流式计算结构 - 使用线程安全的ConcurrentHashMap
    val H2B2metric = ConcurrentHashMap<MyAtom, ConcurrentHashMap<MyAtom, Metric>>() // headAtom -> bodyAtom -> metric
    val key2atoms = ConcurrentHashMap<Int, MutableList<MyAtom>>() // 一级LSH桶：key -> atoms
    val H2F2metric = ConcurrentHashMap<MyAtom, ConcurrentHashMap<Formula, Metric>>() // 原子→公式→度量映射

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
                tails.map { tail -> pairHash32(head, tail) }
            }.toMutableSet()
        }.toMutableMap()

        println("Starting TLearn algorithm...")
        println("Settings.MIN_SUPP: ${Settings.MIN_SUPP}, MAX_PATH_LENGTH: $MAX_PATH_LENGTH")

        // Step 1: Initialize with L=1 relations
        initializeL1Relations()

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

        // Save H2B2metric to JSON file
        saveH2B2metricToJson()
        
        println("\n=== Phase 2: Composition ===")
        // Step 3: Composition phase - combine atoms into formulas using Eclat
        try {
            // compositionPhase()
        } catch (e: Exception) {
            println("Error during composition phase: ${e.message}")
            e.printStackTrace()
        } finally {
            println("\nTLearn algorithm completed.")
            
            // 保存H2F2metric到JSON文件
            saveH2F2metricToJson()
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
     * 使用原有的连接算法，当预估超过阈值时进行全局均匀采样
     */
    private fun isValidRelationPathL2(rp: Long): Boolean {
        // 分解路径: rp = r1 · r2
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
        val instanceSet = mutableSetOf<Int>()
        val inverseSet = mutableSetOf<Int>()
        val random: Random = Random((r1 xor r2).toLong())
        
        // 辅助函数：添加一对实体（无返回值）
        fun tryAddPair(r1Head: Int, r2Tail: Int, connectingEntity: Int) {
            if (r1Head == r2Tail) {
                loopSet.add(r1Head)
                return
            }
            
            require(r1Head != connectingEntity && connectingEntity != r2Tail) {
                "Connection node $connectingEntity should not equal head $r1Head or tail $r2Tail"
            }
            
            val pairHash32Value = pairHash32(r1Head, r2Tail)
            if (instanceSet.add(pairHash32Value)) {
                inverseSet.add(pairHash32(r2Tail, r1Head))
                h2tSet.getOrPut(r1Head) { mutableSetOf() }.add(r2Tail)
                t2hSet.getOrPut(r2Tail) { mutableSetOf() }.add(r1Head)
            }
        }
        
        // 采样阶段：估算总的实例数量，并在估算阶段直接采样以保证多样性
        var estimatedTotal = 0L
        val t2hSet4r1 = R2h2tSet[RelationPath.getInverseRelation(r1)]!!
        val h2tSet4r2 = R2h2tSet[r2]!!
        
        for (connectingEntity in connectingEntities) {
            // 在估算阶段直接进行头尾采样，保证多样性
            val r1HeadEntities = t2hSet4r1.get(connectingEntity) ?: emptySet()
            val r2TailEntities = h2tSet4r2.get(connectingEntity) ?: emptySet()
            val estimatedCount = r1HeadEntities.size.toLong() * r2TailEntities.size.toLong()
            estimatedTotal += estimatedCount
            
            if (estimatedCount == 0L) continue
            // 为每个头实体添加一个随机尾实体样本
            for (r1Head in r1HeadEntities) 
                tryAddPair(r1Head, r2TailEntities.random(random), connectingEntity)
            
            // 为每个尾实体添加一个随机头实体样本
            for (r2Tail in r2TailEntities) 
                tryAddPair(r1HeadEntities.random(random), r2Tail, connectingEntity)
        }
        val sampledSize = instanceSet.size
        val entitySupp = Math.min(connectingEntities.size, Math.min(h2tSet.size, t2hSet.size))
        
        if (entitySupp < Settings.MIN_ENTITY_SUPP) {
            debug1("[isValidRelationPathL2] entitySupp $entitySupp below threshold for rp=${IdManager.getRelationString(rp)}, returning false")
            setSupp(0)
            return false
        }

        // 补充阶段：批量添加实体对，直到达到上限
        fun fillToLimit() {
            for (connectingEntity in connectingEntities.shuffled(random)) {
                val r1HeadEntities = t2hSet4r1.get(connectingEntity) ?: emptySet()
                val r2TailEntities = h2tSet4r2.get(connectingEntity) ?: emptySet()
                
                if (r1HeadEntities.isEmpty() || r2TailEntities.isEmpty()) continue
                
                for (r1Head in r1HeadEntities) {
                    for (r2Tail in r2TailEntities) {
                        tryAddPair(r1Head, r2Tail, connectingEntity)
                        if (instanceSet.size >= MAX_JOIN_INSTANCES_L2) return
                    }
                }
            }
        }
        fillToLimit()
        // Calculate support counts for heads and tails
        val supp = instanceSet.size
        setSupp(supp)
        R2h2tSet[rp] = h2tSet
        if (rp != rpInv) R2h2tSet[rpInv] = t2hSet

        // if (size < Settings.MIN_SUPP) return false
        // 即使instance数量不足也有效（更长的连接），但不进行原子化
        if (supp >= Settings.MIN_SUPP) {
            atomizeBinaryRelationPath(rp, supp, instanceSet, inverseSet)
            // atomizeUnaryRelationPath(rp, h2tSet, t2hSet, loopSet)
        }

        debug1("[isValidRelationPathL2] ${IdManager.getRelationString(rp)} supp: $supp, entitySupp: $entitySupp, self-inverse: ${rp == rpInv}, estimated: $estimatedTotal, sampled: $sampledSize")

        return true
    }

    /**
     * 计算长度为3的路径支持度
     * 使用方式二进行连接，同时验证方式一的有效性
     */
    private fun isValidRelationPathL3(rp: Long): Boolean {
        debug2("isValidRelationPathL3: rp=${IdManager.getRelationString(rp)}")
        
        // 分解路径: rp = r1 · r2 · r3
        val relations = RelationPath.decode(rp)
        val r1 = relations[0]
        val r2 = relations[1]
        val r3 = relations[2]
        val r3Inv = RelationPath.getInverseRelation(r3)

        // 方式一: r1 · (r2·r3) = r1 · r23  通过 isForwardValid 用于验证
        // 方式二: r3Inv · (r2Inv·r1Inv) = r3Inv · r12Inv  用于构造
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
        
        // Verify all required maps exist (another thread may have modified between isValid and here)
        // if (h2tSet4r1 == null || t2hSet4r23 == null || h2tSet4r12Inv == null || t2hSet4r3Inv == null) {
        //     debug2("[isValidRelationPathL3] R2h2tSet entries became null (race condition) for rp=${IdManager.getRelationString(rp)}, return false")
        //     setSupp(0)
        //     return false
        // }
        
        // 判断实例 (h, t) 是否通过方式一有效：r1 · (r2·r3)
        // 需要存在中间节点 y 使得 r1(h, y) 且 (r2·r3)(y, t)
        fun isForwardValid(h: Int, t: Int): Boolean {
            val r1Tails = h2tSet4r1.get(h) ?: return false  // r1(h, ?) 的所有尾节点
            val r23Heads = t2hSet4r23.get(t) ?: return false  // (r2·r3)(?, t) 的所有头节点
            
            // 检查是否有交集（存在共同的中间节点）
            for (tail in r1Tails) {
                if (tail in r23Heads) return true
            }
            return false
        }
        
        // 使用方式二进行连接：r3Inv · (r2Inv·r1Inv)
        val instanceSet = mutableSetOf<Int>()
        val inverseSet = mutableSetOf<Int>()
        val random = Random((r1 xor r2 xor r3).toLong())
        
        // 辅助函数：添加一对实体（无返回值）
        fun tryAddPair(h: Int, t: Int, connectingEntity: Int): Boolean {
            if (h == t) return false  // Object Entity Constraint: X != Y
            
            require(h != connectingEntity && connectingEntity != t) {
                "Connection node $connectingEntity should not equal head $h or tail $t"
            }
            
            // 验证方式一是否也有效
            if (isForwardValid(h, t)) {
                val instanceHash = pairHash32(h, t)
                if (instanceSet.add(instanceHash)) {
                    inverseSet.add(pairHash32(t, h))
                }
                // 只要有效就返回true
                return true
            }
            return false
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
        
        // 估算总的实例数量，并在估算阶段直接采样以保证多样性
        var estimatedTotal = 0L
        for (connectingEntity in connectingEntities) {
            // 获取能通过 r3Inv 到达 connectingEntity 的 head 实体（即原路径的 tail）
            val r3InvHeadEntities = t2hSet4r3Inv.get(connectingEntity) ?: emptySet()
            
            // 获取从 connectingEntity 通过 r12Inv 能到达的 tail 实体（即原路径的 head）
            val r12InvTailEntities = h2tSet4r12Inv.get(connectingEntity) ?: emptySet()
            estimatedTotal += r12InvTailEntities.size.toLong() * r3InvHeadEntities.size.toLong()
            
            // 在估算阶段直接进行头尾采样，保证多样性
            if (r12InvTailEntities.isNotEmpty() && r3InvHeadEntities.isNotEmpty()) {
                // 为每个 h 找一个有效的 randomT
                for (h in r12InvTailEntities) {
                    val shuffledTails = r3InvHeadEntities.shuffled(random)
                    for (t in shuffledTails) {
                        if (tryAddPair(h, t, connectingEntity)) break
                    }
                }
                
                // 为每个 t 找一个有效的 randomH
                for (t in r3InvHeadEntities) {
                    val shuffledHeads = r12InvTailEntities.shuffled(random)
                    for (h in shuffledHeads) {
                        if (tryAddPair(h, t, connectingEntity)) break
                    }
                }
            }
        }
        if (instanceSet.size == 0) {
            debug2("[isValidRelationPathL3] No valid instances found for rp=${IdManager.getRelationString(rp)}, returning false")
            setSupp(0)
            return false
        }
        val sampledSize = instanceSet.size

        
        // 补充阶段：批量添加实体对，直到达到上限
        fun fillToLimit() {
            for (connectingEntity in connectingEntities.shuffled(random)) {
                val r3InvHeadEntities = t2hSet4r3Inv.get(connectingEntity) ?: emptySet()
                val r12InvTailEntities = h2tSet4r12Inv.get(connectingEntity) ?: emptySet()
                
                if (r12InvTailEntities.isEmpty() || r3InvHeadEntities.isEmpty()) continue
                
                for (h in r12InvTailEntities) {
                    for (t in r3InvHeadEntities) {
                        tryAddPair(h, t, connectingEntity)
                        if (instanceSet.size >= MAX_JOIN_INSTANCES_L3) return
                    }
                }
            }
        }
        
        if (instanceSet.size < MAX_JOIN_INSTANCES_L3 / 10) fillToLimit()
        val supp = instanceSet.size
        setSupp(supp)
        debug1("[isValidRelationPathL3] ${IdManager.getRelationString(rp)} supp: $supp, self-inverse: ${rp == rpInv}, estimated: $estimatedTotal, sampled: $sampledSize")
        
        // atomize 使用 supp 而不是 entity supp作为阈值
        if (supp >= Settings.MIN_SUPP)
            atomizeBinaryRelationPath(rp, supp, instanceSet, inverseSet)
        return true
    }

    /**
     * 处理Binary原子化：r(X,Y) 和 r'(X,Y)
     * 直接使用预计算的MinHash签名
     */
    fun atomizeBinaryRelationPath(rp: Long, supp: Int, instanceSet: MutableSet<Int>, inverseSet: MutableSet<Int>) {
        debug2("atomizeBinaryRelationPath: rp=$rp, supp=$supp, instanceSet.size=${instanceSet.size}, inverseSet.size=${inverseSet.size}")
        val rpInv = RelationPath.getInverseRelation(rp)
        // 1. r(X,Y): Binary Atom with relation path rp
        performLSH(MyAtom(rp, IdManager.getYId(), instanceSet))
        // 2. r'(X,Y): Binary Atom with inverse relation path
        performLSH(MyAtom(rpInv, IdManager.getYId(), inverseSet))
    }
    
    /**
     * 处理Unary原子化：r(X,c), r(X,·), r(c,X), r(·,X), r(X,X)
     * 需要动态计算MinHash签名
     */
    fun atomizeUnaryRelationPath(rp: Long, h2tSet: MutableMap<Int, MutableSet<Int>>, t2hSet: MutableMap<Int, MutableSet<Int>>, loopSet: MutableSet<Int>) {
        debug2("atomizeUnaryRelationPath: rp=$rp, h2tSet.size=${h2tSet.size}, t2hSet.size=${t2hSet.size}, loopSet.size=${loopSet.size}")
        val rpInv = RelationPath.getInverseRelation(rp)

        // 3. r(X,c): Unary Atom for each constant c where rp(X,c) exists
        t2hSet.forEach { (constant, unaryInstanceSet) -> 
            val supp = unaryInstanceSet.size
            if (supp >= Settings.MIN_SUPP) {
                val unaryAtom = MyAtom(rp, constant, unaryInstanceSet)
                performLSH(unaryAtom)
                if (RelationPath.isL1Relation(rp)) {
                    setH2B2metric(unaryAtom, MyAtom(0, IdManager.getZId()), Metric(supp.toDouble(), supp, R2supp[rp]!!))
                }
            }
        }
        
        // 4. r(X,·): Unary Atom for existence - relation rp has head entities
        if (h2tSet.size >= Settings.MIN_SUPP)
            performLSH(MyAtom(rp, 0, h2tSet.keys))
        
        // 5. r(c,X) / r'(X,c): Unary Atom for each constant c where r(c,X) exists
        h2tSet.forEach { (constant, inverseUnaryInstanceSet) -> 
            val supp = inverseUnaryInstanceSet.size
            if (supp >= Settings.MIN_SUPP) {
                val inverseUnaryAtom = MyAtom(rpInv, constant, inverseUnaryInstanceSet)
                performLSH(inverseUnaryAtom)
                if (RelationPath.isL1Relation(rp)) {
                    setH2B2metric(inverseUnaryAtom, MyAtom(0, IdManager.getZId()), Metric(supp.toDouble(), supp, R2supp[rpInv]!!))
                }
            }
        }
        
        // 6. r(·,X) / r'(X,·): Unary Atom for existence - inverse relation has head entities
        if (t2hSet.size >= Settings.MIN_SUPP)
            performLSH(MyAtom(rpInv, 0, t2hSet.keys))

        // 7. r(X,X): Unary Atom for loops - r(X,X) exists
        if (loopSet.size >= Settings.MIN_SUPP)
            performLSH(MyAtom(rp, IdManager.getXId(), loopSet))
    }

    fun pairHash32(h: Int, t: Int): Int {
        val uH = h * -0x61c88647     // 0x9E3779B9 的补码（黄金比例常数）
        val uT = t * 0x85ebca6b.toInt()
        return uH xor Integer.rotateLeft(uT, 16)
    }

    /**
     * Helper function to set formula metric for an atom
     */
    fun setH2F2metric(atom: MyAtom, formula: Formula, metric: Metric) {
        val F2metric = H2F2metric.computeIfAbsent(atom) { ConcurrentHashMap() }
        F2metric[formula] = metric
    }

    fun setH2B2metric(headAtom: MyAtom, bodyAtom: MyAtom, metric: Metric) {
        val B2metric = H2B2metric.computeIfAbsent(headAtom) { ConcurrentHashMap() }
        B2metric[bodyAtom] = metric
    }

    /**
     * LSH bucketing - add atom to key2atoms buckets and update H2B2metric
     */
    fun performLSH(currentAtom: MyAtom) {
        debug2("performLSH: Atom=$currentAtom, support=${currentAtom.support}")
        require(!currentAtom.minHashSignature.isEmpty()) {
            "performLSH: Empty MinHash signature for atom $currentAtom"
        }
        
        val relevantAtom2BucketCount = mutableMapOf<MyAtom, Int>()
        
        // Step 1: Update relevantAtom2BucketCount by scanning existing buckets
        for (bandIndex in 0 until BANDS) {
            val key = currentAtom.minHashSignature[bandIndex]
            val bucket = key2atoms[key]
            if (bucket != null) {
                synchronized(bucket) {
                    bucket.forEach { existingAtom ->
                        if (existingAtom.isHeadAtom)
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

            // 直接使用碰撞次数计算Jaccard相似度：bucketCount / BANDS
            val jaccard = bucketCount.toDouble() / BANDS
            // 估计交集大小 (head=currentAtom, body=bucketAtom)
            var intersectionSize = estimateIntersectionSize(jaccard, currentAtom.support, bucketAtom.support)
            if (intersectionSize >= Settings.MIN_SUPP * ESTIMATE_RATIO) {
                if (validateH2B(bucketAtom, currentAtom)) cnt++
                
                if (currentAtom.isHeadAtom) {
                    if (validateH2B(currentAtom, bucketAtom)) cnt++
                } else if (currentAtom.isL1Atom && currentAtom.isBinary) {
                    // 注意这里不能只验证 headAtom，因为 currentAtom 可能是 Binary & L1Atom: current'(X,Y) <= bucket(X,Y)
                    if (validateH2B(currentAtom.inverse(), bucketAtom.inverse())) cnt++
                }
            }
        }

        if (!currentAtom.isHeadAtom && cnt == 0) {
            // No valid candidates and not a head atom - skip adding this atom to buckets
            // currentAtom will be garbage collected as it's not referenced anywhere
            return
        }
        
        // Add currentAtom to key2atoms buckets only if there are valid relevant atoms
        for (bandIndex in 0 until BANDS) {
            val key = currentAtom.minHashSignature[bandIndex]
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

    private fun validateH2B(headAtom: MyAtom, bodyAtom: MyAtom): Boolean {
        // debug2("validateH2B: headAtom=$headAtom, bodyAtom=$bodyAtom")
        
        // 自证式一元规则（entity-anchored unary rules）问题
        // if (myAtom.isL2Atom && !myAtom.isBinary) {  这种写法有问题，会漏掉L1Atom的情况
        val bodyInstances = if (!bodyAtom.isL1Atom && !bodyAtom.isBinary) {
            val constant = headAtom.entityId
            val inverseRelation = RelationPath.getInverseRelation(bodyAtom.firstRelation)
            val t2hSet = ts.r2h2tSet[inverseRelation]
            
            if (t2hSet != null && t2hSet[constant] != null) {
                bodyAtom.instances.filter { !t2hSet[constant]!!.contains(it) }.toSet()
            } else {
                bodyAtom.instances
            }
        } else {
            bodyAtom.instances
        }

        var intersectionSet = bodyInstances.intersect(headAtom.instances)
        var intersectionSize = intersectionSet.size.toDouble()
        // 二元/一般情况：headAtom 左侧 (head)，bodyAtom 右侧 (body)
        // Note: bodyInstances.size may differ from bodyAtom.support due to filtering
        var metric = Metric(intersectionSize, headAtom.support, bodyInstances.size)
        // debug1("validateH2B: headAtom=$headAtom, bodyAtom=$bodyAtom, metric=$metric")
        
        if (metric.valid) {
            setH2B2metric(headAtom, bodyAtom, metric)
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
        
        try {
            val futures = H2B2metric.entries.map { (headAtom, bodyMap) ->
                threadPool.submit {
                    processHeadAtom(headAtom, bodyMap)
                    val cnt = processedHeads.incrementAndGet()
                    if (cnt % 100 == 0) {
                        println("Processed $cnt/$totalHeads head atoms...")
                    }
                }
            }
            
            // Wait for all tasks to complete
            futures.forEach { it.get() }
            
        } finally {
            threadPool.shutdown()
            threadPool.awaitTermination(1, TimeUnit.HOURS)
        }
        
        println("Composition Phase completed. Total rules: ${H2F2metric.values.sumOf { it.size }}")
    }
    
    /**
     * Process single headAtom, build BQueue and perform Eclat depth-first search
     */
    private fun processHeadAtom(headAtom: MyAtom, bodyMap: ConcurrentHashMap<MyAtom, Metric>) {
        // Sort by confidence in descending order (Metric implements Comparable)
        val sortedBodies = bodyMap.entries
            .sortedBy { it.value }  // Metric.compareTo sorts by confidence descending
            .toList()
        
        if (sortedBodies.isEmpty()) return
        
        // Take top MAX_BUCKET_ATTEMPT candidates
        val bQueue = if (sortedBodies.size > MAX_BUCKET_ATTEMPT) {
            sortedBodies.take(MAX_BUCKET_ATTEMPT)
        } else {
            sortedBodies
        }
        
        debug2("processHeadAtom: $headAtom, BQueue size=${bQueue.size}")
        
        // bodyMap already contains validated metrics, use it directly as frequent1
        val frequent1 = bQueue.map { (bodyAtom, metric) ->
            // Store L1 Formula
            // val formula = Formula(bodyAtom)
            // setH2F2metric(headAtom, formula, metric)
            
            debug2("Frequent-1: $headAtom <= $bodyAtom, conf=${metric.confidence}, supp=${metric.support}")
            
            // Build FrequentAtomSet for Eclat DFS
            // Note: bodyAtom.instances already filtered in performLSH for entity-anchored unary rules
            FrequentAtomSet(
                atoms = listOf(bodyAtom),
                intersectInstances = headAtom.instances.intersect(bodyAtom.instances),
                bodyInstances = bodyAtom.instances,
                confidence = metric.confidence
            )
        }
        
        // Eclat depth-first search
        for (i in frequent1.indices) {
            val freq1 = frequent1[i]
            // Only combine with subsequent atoms
            val remainingAtoms = frequent1.subList(i + 1, frequent1.size).map { it.atoms[0] }
            
            if (remainingAtoms.isNotEmpty()) {
                tryBodyAtoms(
                    headAtom = headAtom,
                    stack = freq1.atoms.toMutableList(),
                    candidateAtoms = remainingAtoms,
                    intersectInstances = freq1.intersectInstances,
                    bodyInstances = freq1.bodyInstances,
                    currentConf = freq1.confidence
                )
            }
        }
    }
    
    /**
     * Eclat depth-first search - recursively try combining more bodyAtoms
     */
    private fun tryBodyAtoms(
        headAtom: MyAtom,
        stack: MutableList<MyAtom>,
        candidateAtoms: List<MyAtom>,
        intersectInstances: Set<Int>,
        bodyInstances: Set<Int>,
        currentConf: Double
    ) {
        if (stack.size >= MAX_STACK_SIZE) return
        if (candidateAtoms.isEmpty()) return
        
        for (i in candidateAtoms.indices) {
            val nextAtom = candidateAtoms[i]
            
            val newIntersectInstances = intersectInstances.intersect(nextAtom.instances)
            val newIntersectionSize = newIntersectInstances.size
            
            if (newIntersectionSize < Settings.MIN_SUPP) {
                continue // Does not meet minimum support, prune
            }
            
            val newBodyInstances = bodyInstances.intersect(nextAtom.instances)
            val newConf = newIntersectionSize.toDouble() / newBodyInstances.size
            
            if (newConf > currentConf * IMPROVE_RATIO && newConf > H2B2metric[headAtom]!![nextAtom]!!.confidence * IMPROVE_RATIO) {
                // Add nextAtom to stack
                stack.add(nextAtom)
                
                // Create Formula and store
                val formula = when (stack.size) {
                    2 -> Formula(stack[0], stack[1])
                    3 -> Formula(stack[0], stack[1], stack[2])
                    else -> null
                }
                
                if (formula != null) {
                    val metric = Metric(
                        support = newIntersectionSize.toDouble(),
                        headSize = headAtom.instances.size,
                        bodySize = newBodyInstances.size
                    )
                    setH2F2metric(headAtom, formula, metric)
                    
                    debug2("Frequent-${stack.size}: $headAtom <= ${stack.joinToString(" & ")}, conf=$newConf, supp=$newIntersectionSize")
                }
                
                // Recurse: only combine with subsequent atoms
                val remainingCandidates = candidateAtoms.subList(i + 1, candidateAtoms.size)
                if (remainingCandidates.isNotEmpty()) {
                    tryBodyAtoms(
                        headAtom = headAtom,
                        stack = stack,
                        candidateAtoms = remainingCandidates,
                        intersectInstances = newIntersectInstances,
                        bodyInstances = newBodyInstances,
                        currentConf = newConf
                    )
                }
                
                // Backtrack
                stack.removeAt(stack.size - 1)
            }
        }
    }
    
    /**
     * Data class: frequent atom set
     */
    private data class FrequentAtomSet(
        val atoms: List<MyAtom>,
        val intersectInstances: Set<Int>,
        val bodyInstances: Set<Int>,
        val confidence: Double
    )


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
        fun isBinaryBucket(atoms: List<MyAtom>) = atoms.first().entityId == IdManager.getYId()
        
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
     * Save H2B2metric to JSON file - streaming output to avoid memory overflow
     */
    private fun saveH2B2metricToJson() {
        val outputFile = File(Settings.PATH_H2B2metric)
        val outputRule = File(Settings.PATH_RULES_TXT)
        outputFile.parentFile?.mkdirs() // Ensure output directory exists
        outputRule.parentFile?.mkdirs()
        
        println("Saving H2B2metric to ${outputFile.absolutePath}...")
        
        BufferedWriter(FileWriter(outputFile)).use { writer ->
            // FileWriter with false (default) = overwrite mode, clear existing content
            BufferedWriter(FileWriter(outputRule, false)).use { ruleWriter ->
                writer.write("{\n")
                val headAtomEntries = H2B2metric.entries.toList()

                headAtomEntries.forEachIndexed { headIndex, (headAtom, bodyMap) ->
                    // Escape special characters in JSON string
                    val headAtomString = headAtom.toString().replace("\"", "\\\"").replace("\n", "\\n")
                    writer.write("  \"$headAtomString\": {\n")

                    val bodyEntries = bodyMap.entries.toList()
                        .sortedByDescending { it.value.confidence } // Sort by metric descending
                    
                    bodyEntries.forEachIndexed { bodyIndex, (bodyAtom, metric) ->
                        val bodyAtomString = bodyAtom.toString().replace("\"", "\\\"").replace("\n", "\\n")
                        writer.write("    \"$bodyAtomString\": $metric")
                        if (bodyIndex < bodyEntries.size - 1) writer.write(",")
                        writer.write("\n")
                        
                        // Write rule to text file
                        val ruleLine = "${metric.bodySize}\t${metric.support.toInt()}\t${metric.confidence}\t${headAtom.getRuleString()} <= ${bodyAtom.getRuleString()}"
                        ruleWriter.write(ruleLine)
                        ruleWriter.write("\n")
                    }

                    writer.write("  }")
                    if (headIndex < headAtomEntries.size - 1) writer.write(",")
                    writer.write("\n")

                    // Flush every 100 atoms to avoid memory accumulation
                    if (headIndex % 100 == 0) {
                        writer.flush()
                        ruleWriter.flush()
                        println("Processed ${headIndex + 1}/${headAtomEntries.size} head atoms...")
                    }
                }
                writer.write("}\n")
            }
        }

        println("Successfully saved H2B2metric to ${outputFile.absolutePath}")
        println("Successfully saved H2B rules to ${outputRule.absolutePath}")
        println("Total head atoms: ${H2B2metric.size}")
        println("Total bucket connections: ${H2B2metric.values.sumOf { it.size }}")
    }

    /**
     * Save H2F2metric to JSON file - streaming output to avoid memory overflow
     */
    private fun saveH2F2metricToJson() {
        // val outDir = File("out/" + Settings.DATASET)
        // outDir.mkdirs() // 确保out目录存在
        val outputFile = File(Settings.PATH_H2F2metric)
        val outputRule = File(Settings.PATH_RULES_TXT)
        
        // Statistics variables
        var totalRules = 0
        val unaryStats = IntArray(MAX_PATH_LENGTH + 1) // L0, L1, L2, L3
        val binaryStats = IntArray(MAX_PATH_LENGTH + 1) // L0, L1, L2, L3
        
        BufferedWriter(FileWriter(outputFile)).use { writer ->
            // FileWriter with true = append mode, preserve H2B rules written by saveH2B2metricToJson
            BufferedWriter(FileWriter(outputRule, true)).use { ruleWriter ->
            writer.write("{\n")
            val atomEntries = H2F2metric.entries.toList()

            atomEntries.forEachIndexed { atomIndex, (atom, formula2Metric) ->
                // Escape special characters in JSON string
                val atomString = atom.toString().replace("\"", "\\\"").replace("\n", "\\n")
                writer.write("  \"$atomString\": {\n")

                val formulaEntries = formula2Metric.entries.toList()
                    .filter { it.value.bodySize > 0 && it.value.confidence.isFinite() }  // 过滤无效的metric
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
                    val formulaString = formula.toString()
                        .replace("\"", "\\\"").replace("\n", "\\n")
                    writer.write("    \"$formulaString\": $metric")
                    if (formulaIndex < formulaEntries.size - 1) writer.write(",")
                    writer.write("\n")

                    val formulaRuleString = formula.getRuleString()
                    val ruleLine = "${metric.bodySize}\t${metric.support.toInt()}\t${metric.confidence}\t${atom.getRuleString()} <= $formulaRuleString"
                    ruleWriter.write(ruleLine)
                    ruleWriter.write("\n")
                    
                    // Statistics for rules
                    totalRules++
                    val bodyLength = formula.size
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

                // Flush every 100 atoms to avoid memory accumulation
                if (atomIndex % 100 == 0) {
                    writer.flush()
                    ruleWriter.flush()
                    println("Processed ${atomIndex + 1}/${atomEntries.size} atoms...")
                }
            }
            writer.write("}\n")
            }
        }

        println("Successfully saved H2F2metric to ${outputFile.absolutePath}")
        println("Successfully saved rules to ${outputRule.absolutePath}")
        println("Total atoms: ${H2F2metric.size}")
        println("Total formulas: ${H2F2metric.values.sumOf { it.size }}")
        
        // Print rule statistics
        println("Total rules: $totalRules")
        println("Type     M0       M1       M2       M3")
        println("-" .repeat(60))
        println("Unary    ${unaryStats[0].toString().padStart(8)}  ${unaryStats[1].toString().padStart(8)}  ${unaryStats[2].toString().padStart(8)}  ${unaryStats[3].toString().padStart(8)}")
        println("Binary   ${binaryStats[0].toString().padStart(8)}  ${binaryStats[1].toString().padStart(8)}  ${binaryStats[2].toString().padStart(8)}  ${binaryStats[3].toString().padStart(8)}")
    }
}