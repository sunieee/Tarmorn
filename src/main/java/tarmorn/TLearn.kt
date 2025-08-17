package tarmorn

import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import tarmorn.data.TripleSet
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.lang.Thread.sleep
import java.util.PriorityQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.iterator
import kotlin.math.abs
import kotlin.math.min

/**
 * TLearn - Top-down relation path learning algorithm
 * Implements the connection algorithm: Binary Atom L=1 -connect-> Binary Atom L<=MAX_PATH_LENGTH
 */
object TLearn {

    const val MIN_SUPP = 50
    const val MAX_PATH_LENGTH = 3

    // MinHash parameters: MH_DIM = BANDS * R
    const val MH_DIM = 128
    const val R = 2  // 每band维度
    const val BANDS = MH_DIM / R
    
    // 全局随机种子数组，在程序启动时初始化
    private lateinit var globalHashSeeds: IntArray

    // Core data structures
    val config = Settings.load()    // 加载配置
    val ts: TripleSet = TripleSet(Settings.PATH_TRAINING, true)
    // lateinit var r2tripleSet: MutableMap<Long, MutableSet<MyTriple>>
    lateinit var r2supp: MutableMap<Long, Int>
    // 仅有2跳及以下的relation path才存储完整的头尾实体对
    lateinit var r2h2tSet: MutableMap<Long, MutableMap<Int, MutableSet<Int>>>
    lateinit var r2h2supp: MutableMap<Long, MutableMap<Int, Int>>
    lateinit var r2loopSet: MutableMap<Long, MutableSet<Int>>

    // Thread-safe relation queue using ConcurrentLinkedQueue for producer-consumer pattern
    val relationQueue = PriorityQueue<RelationPathItem>(
        compareByDescending { it.supp }
//        compareBy { it.supp }
    )
    val queueLock = Object()
    val activeThreadCount = AtomicInteger(0) // 线程安全的活动线程计数
    val threadMonitorLock = Object() // 用于线程监控的锁

    // Backup of L1 relations for connection attempts
    lateinit var relationL1: List<Long>

    // Synchronized logger for workers
    private val logFile = File("out/workers.log")
    private lateinit var logWriter: BufferedWriter
    private var logErrorReported = false // 防止重复报告日志错误
    private var logWriterClosed = false // 跟踪日志写入器状态


    // 核心类型定义
    data class MyAtom(val relationId: Long, val entityId: Int) {   // (RelationID, EntityID)，EntityID<0 表示 binary
        override fun toString(): String {
            val relationStr = IdManager.getRelationString(relationId)
            return when {
                entityId == IdManager.getYId() -> "$relationStr(X,Y)"
                entityId == IdManager.getXId() -> "$relationStr(X,X)"
                entityId == 0 -> "$relationStr(X,·)"
                else -> {
                    val entityStr = IdManager.getEntityString(entityId)
                    "$relationStr(X,$entityStr)"
                }
            }
        }

        val isL1Atom: Boolean
            get() = relationId < RelationPath.MAX_RELATION_ID

        val isL2Atom: Boolean
            get() = relationId < RelationPath.MAX_L2RELATION_ID

        val isHeadAtom: Boolean
            get() = isL1Atom &&
                    (entityId == IdManager.getYId() && !IdManager.isInverseRelation(relationId) || entityId > 0)
    }

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

        override fun toString(): String {
            return listOfNotNull(atom1, atom2, atom3).joinToString(" & ")
        }
    }

    data class Metric(
        val support: Double,
        val headSize: Int,
        val bodySize: Int,
    ) {
        val coverage: Double = support / headSize
        val confidence: Double = support / bodySize

        val valid: Boolean
            get() = support >= MIN_SUPP && coverage > 0.1 && confidence > 0.1
    }

    // 流式计算结构
    val formula2supp = mutableMapOf<Formula, Int>()          // 公式→支持度映射
    val minHashRegistry = mutableMapOf<Formula, IntArray>()           // 公式→MinHash映射
    val band2headAtom = mutableMapOf<Int, MutableMap<Int, MutableList<MyAtom>>>()  // 双级Map：直接使用MinHash索引作为键
    val band2L2Atom = mutableMapOf<Int, MutableMap<Int, MutableList<MyAtom>>>()

    // 预分配桶空间以减少Map扩容开销
    private fun initializeLSHBuckets() {
        // 预估可能的桶数量，减少Map扩容 - 双级Map只需要更少的预分配
        for (i in 0 until 50) { // 第一级只需要较少的预分配
            band2headAtom[i] = mutableMapOf()
            band2L2Atom[i] = mutableMapOf()
        }
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
        // Initialize global hash seeds first
        initializeGlobalHashSeeds()
        
        // Initialize LSH buckets to reduce Map expansion overhead
        initializeLSHBuckets()
        
        // Initialize and clear the log file
        initializeLogger()

        // Initialize data structures
        // r2tripleSet = ts.r2tripleSet
        r2loopSet = ts.r2loopSet
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

            // Close the log file - 移到这里，确保所有工作线程完成后再关闭
            synchronized(logWriter) {
                try {
                    if (!logWriterClosed && this::logWriter.isInitialized) {
                        logWriterClosed = true
                        logWriter.close()
                    }
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
                r2supp[relation] = tripleSet.size
                if (tripleSet.size >= MIN_SUPP) {
//                    val pathId = RelationPath.encode(relation)
                    val item = RelationPathItem(relation, tripleSet.size)
                    relationQueue.offer(item)
                    addedCount++

                    if (!IdManager.isInverseRelation(relation)) {
                        // 为L=1关系进行原子化，直接使用r2h2tSet中的反向索引
                        val h2tSet = r2h2tSet[relation]!!.toMutableMap()
                        val inverseRelation = RelationPath.getInverseRelation(relation)
                        val t2hSet = r2h2tSet[inverseRelation]!!.toMutableMap()

                        // 计算Binary原子的MinHash签名
                        val binaryInstanceSet = h2tSet.flatMap { (head, tails) ->
                            tails.map { tail -> Pair(head, tail) }
                        }.toSet()
                        val inverseBinaryInstanceSet = t2hSet.flatMap { (tail, heads) ->
                            heads.map { head -> Pair(tail, head) }
                        }.toSet()
                        
                        val binaryMinHash = computeBinaryMinHash(binaryInstanceSet)
                        val inverseBinaryMinHash = computeBinaryMinHash(inverseBinaryInstanceSet)
                    
                        // 处理Binary原子
                        atomizeBinaryRelationPath(relation, tripleSet.size, binaryMinHash, inverseBinaryMinHash)
                        // 处理Unary原子
                        atomizeUnaryRelationPath(relation, h2tSet, t2hSet, r2loopSet[relation] ?: mutableSetOf())
                    }
                    
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
        activeThreadCount.set(Settings.WORKER_THREADS)
        val futures = (1..Settings.WORKER_THREADS).map { threadId ->
            threadPool.submit {
                connectRelationsWorker(threadId, processedCount, addedCount)
            }
        }

        // Monitor and force shutdown if too many threads stuck
        try {
            synchronized(threadMonitorLock) {
                while (!futures.all { it.isDone }) {
                    threadMonitorLock.wait() // 等待子线程通知
                    val activeCount = activeThreadCount.get()
                    println("Thread count changed: $activeCount/${Settings.WORKER_THREADS} active")
                    
                    if (activeCount < Settings.WORKER_THREADS - 5) {
                        println("FORCING SHUTDOWN: Only $activeCount threads active, likely stuck!")
                        futures.forEach { it.cancel(true) }
                        threadPool.shutdownNow()
                        break
                    }
                }
            }
            
            if (!threadPool.isShutdown) {
                futures.forEach { it.get() }
                threadPool.shutdown()
            }
        } catch (e: Exception) {
            threadPool.shutdownNow()
        }

        println("Connection completed. Processed: ${processedCount.get()}, Added: ${addedCount.get()}")
    }

    /**
     * Worker thread for connecting relations
     */
    fun connectRelationsWorker(threadId: Int, processedCount: AtomicInteger, addedCount: AtomicInteger) {
        println("Thread $threadId started")

        // 如果只有当前一个线程卡主，则直接结束
        while (true) {
            // Step 3: Get next relation path from queue
            var currentItem = synchronized(queueLock) {relationQueue.poll()}
            var attempts = 0
            while (currentItem == null && attempts < 3) {
                sleep(1000)
                currentItem = synchronized(queueLock) {relationQueue.poll()}
                attempts++
            }
            if (currentItem == null) {
                println("Thread $threadId: No more items in queue, exiting")
                break
            }

            val ri = currentItem.relationPath
            val riLength = RelationPath.getLength(ri)

            // Skip if already at max length
            if (riLength >= MAX_PATH_LENGTH) {
                continue
            }

            processedCount.incrementAndGet()

            // Step 4: Try connecting with all L1 relations
            for (r1 in relationL1) {
                // try {
                val connectedPath = attemptConnection(r1, ri)
                if (connectedPath != null) {
                    val supp = computeSupp(connectedPath, r1, ri)
                    if (supp >= MIN_SUPP) {
                        synchronized(queueLock) {
                            val newItem = RelationPathItem(connectedPath, supp)
                            relationQueue.offer(newItem)
                        }
                        val cnt = addedCount.incrementAndGet()

                        // Log the successful path addition
                        logWorkerResult(connectedPath, supp)

                        if (cnt % 100 == 0 || activeThreadCount.get() < Settings.WORKER_THREADS) {
                            println("Thread $threadId: Added $cnt new paths")
                            println("Thread $threadId: path $connectedPath added with supp $supp (r1: $r1, ri: $ri) TODO: ${relationQueue.size} remaining in queue")
                        }
                    }
                }
                // } catch (e: Exception) {
                //     // Log error but continue processing
                //     println("Thread $threadId: Error connecting paths: ${e.message}")
                // }
            }
        }

        val cnt = activeThreadCount.decrementAndGet()
        println("Thread $threadId completed, $cnt threads remain")
        
        // 通知主线程检查线程状态
        synchronized(threadMonitorLock) {
            threadMonitorLock.notify()
        }
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
        val inverseRp = RelationPath.getInverseRelation(rp)

        // Check if rp or its inverse already exists
        // if (r2tripleSet.containsKey(rp) || r2tripleSet.containsKey(IdManager.getInverseRelation(rp))) {
        //     return null // Skip existing paths
        // }
        // 为了避免多线程冲突，使用synchronized锁，并且初始化r2supp
        // 否则同一个rp可能被多线程同时计算，导致同一个bucket添加相同的atom
        synchronized(r2supp) {
            if (r2supp.containsKey(rp)) {   //  || r2supp.containsKey(inverseRp)
                return null // Skip existing paths
            }
            r2supp[rp] = 0
            r2supp[inverseRp] = 0
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

        // Initialize data structures
        val h2tSet = mutableMapOf<Int, MutableSet<Int>>()
        val t2hSet = mutableMapOf<Int, MutableSet<Int>>()
        val h2supp = mutableMapOf<Int, Int>()
        val t2supp = mutableMapOf<Int, Int>()
        val loopSet = mutableSetOf<Int>()
        var size = 0

        // Compute Cartesian product and MinHash signatures
        val binaryMinHash = IntArray(MH_DIM) { Int.MAX_VALUE }
        val inverseBinaryMinHash = IntArray(MH_DIM) { Int.MAX_VALUE }
        
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

            // Create Cartesian product and compute MinHash on-the-fly
            for (r1Head in r1HeadEntities) {
                for (riTail in riTailEntities) {
                    if (r1Head == riTail) {
                        loopSet.add(r1Head)
                    } else {
                        // Compute both MinHash signatures in one loop - 优化版本减少数组访问
                        for (i in 0 until MH_DIM) {
                            val seed = globalHashSeeds[i]
                            // For Binary MinHash: (r1Head, riTail) - 保持顺序
                            val binaryHashValue = computeBinaryHash(r1Head, riTail, seed)
                            val currentBinaryMin = binaryMinHash[i]
                            if (binaryHashValue < currentBinaryMin) {
                                binaryMinHash[i] = binaryHashValue
                            }
                            
                            // For Inverse Binary MinHash: (riTail, r1Head) - 保持顺序
                            val inverseBinaryHashValue = computeBinaryHash(riTail, r1Head, seed)
                            val currentInverseMin = inverseBinaryMinHash[i]
                            if (inverseBinaryHashValue < currentInverseMin) {
                                inverseBinaryMinHash[i] = inverseBinaryHashValue
                            }
                        }
                        
                        // Store h2t mapping only for paths with length < 3
                        if (pathLength < 3) {
                            h2tSet.getOrPut(r1Head) { mutableSetOf() }.add(riTail)
                            t2hSet.getOrPut(riTail) { mutableSetOf() }.add(r1Head)
                        }
                    }
                }
            }
        }

        // 因为r2supp占用内存较小，且需要记录，频繁访问，直接存储
        val inverseRp = RelationPath.getInverseRelation(rp)
        r2supp[rp] = size
        r2supp[inverseRp] = size
        
        if (size >= MIN_SUPP) {
            // Add to main data structures
            // 原子化：使用预计算的Binary MinHash + 动态计算Unary，注意supp一定要传递，不能使用r2supp
            binaryMinHash.forEach {
                require(it <= Int.MAX_VALUE && it >= 0)
            }
            inverseBinaryMinHash.forEach {
                require(it <= Int.MAX_VALUE && it >= 0)
            }

            val (forwardSuccess, inverseSuccess) = atomizeBinaryRelationPath(rp, size, binaryMinHash, inverseBinaryMinHash)


            // r2tripleSet[rp] = resultTriples
            if (forwardSuccess || pathLength < 3) r2h2supp[rp] = h2supp
            if (inverseSuccess || pathLength < 3) r2h2supp[inverseRp] = t2supp

            // 长度超过1的不进行Unary原子化
            if (pathLength < 3) {
                // For paths with length < 3, we store the full h2t mapping
                r2h2tSet[rp] = h2tSet
                // r2h2tSet[RelationPath.getInverseRelation(rp)] = h2tSet
                atomizeUnaryRelationPath(rp, h2tSet, t2hSet, loopSet)
            }
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
     * 处理Binary原子化：r(X,Y) 和 r'(X,Y)
     * 直接使用预计算的MinHash签名
     */
    fun atomizeBinaryRelationPath(rp: Long, supp: Int, binaryMinHash: IntArray, inverseBinaryMinHash: IntArray): Pair<Boolean, Boolean> {
        val inverseRp = RelationPath.getInverseRelation(rp)
        // 1. r(X,Y): Binary Atom with relation path rp
        val binaryAtom = MyAtom(rp, IdManager.getYId()) // Y表示二元原子
        // 2. r'(X,Y): Binary Atom with inverse relation path
        val inverseBinaryAtom = MyAtom(inverseRp, IdManager.getYId())
        return Pair(
            addAtomToMinHash(binaryAtom, supp, binaryMinHash),
            addAtomToMinHash(inverseBinaryAtom, supp, inverseBinaryMinHash)
        )
    }
    
    /**
     * 处理Unary原子化：r(X,c), r(X,·), r(c,X), r(·,X), r(X,X)
     * 需要动态计算MinHash签名
     */
    fun atomizeUnaryRelationPath(rp: Long, h2tSet: MutableMap<Int, MutableSet<Int>>, t2hSet: MutableMap<Int, MutableSet<Int>>, loopSet: MutableSet<Int>) {
        val inverseRp = RelationPath.getInverseRelation(rp)

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

        // 7. r(X,X): Unary Atom for loops - r(X,X) exists
        if (loopSet.size >= MIN_SUPP) {
            val loopAtom = MyAtom(rp, IdManager.getXId()) // X表示循环原子
            val minHashSignature = computeUnaryMinHash(loopSet)
            addAtomToMinHash(loopAtom, loopSet.size, minHashSignature)
        }
    }

    /** 对于长度超过1的关系路径，只应该将Formula添加到已经存在的LSH桶中，而不应该创建新的桶
     * 为Atom计算MinHash并加入LSH分桶
     */
    fun addAtomToMinHash(atom: MyAtom, supp: Int, minHashSignature: IntArray): Boolean {
        // 创建Formula (单原子公式)
        if (atom.isL2Atom) {
            val formula = Formula(atom1 = atom)
            minHashRegistry[formula] = minHashSignature
            formula2supp[formula] = supp
        }

        val validCombinationCount = performLSH(atom, minHashSignature, supp)
        return atom.isL2Atom || validCombinationCount > 0
    }

    /**
     * 计算Binary Atom的MinHash签名
     */
    fun computeBinaryMinHash(instanceSet: Set<Pair<Int, Int>>): IntArray {
        val signature = IntArray(MH_DIM) { Int.MAX_VALUE }

        // 检查空集合，如果为空则抛出异常或返回特殊值
        if (instanceSet.isEmpty()) {
            throw IllegalArgumentException("Cannot compute MinHash for empty instance set")
        }

        // 为每个实例生成哈希值 - 优化版本减少数组访问
        instanceSet.forEach { (entity1, entity2) ->
            for (i in 0 until MH_DIM) {
                val hashValue = computeBinaryHash(entity1, entity2, globalHashSeeds[i])
                val currentMin = signature[i]
                if (hashValue < currentMin) {
                    signature[i] = hashValue
                }
            }
        }

        return signature
    }

    /**
     * 计算Unary Atom的MinHash签名
     */
    fun computeUnaryMinHash(instanceSet: Set<Int>): IntArray {
        val signature = IntArray(MH_DIM) { Int.MAX_VALUE }

        // 检查空集合，如果为空则抛出异常
        if (instanceSet.isEmpty()) {
            throw IllegalArgumentException("Cannot compute MinHash for empty instance set")
        }

        // 为每个实例生成哈希值 - 优化版本减少数组访问
        instanceSet.forEach { entity ->
            for (i in 0 until MH_DIM) {
                val hashValue = computeUnaryHash(entity, globalHashSeeds[i])
                val currentMin = signature[i]
                if (hashValue < currentMin) {
                    signature[i] = hashValue
                }
            }
        }

        return signature
    }


    fun pairHash(entity1: Int, entity2: Int): Int {
        // 直接实现Pair的hashCode原理，避免创建对象
        return entity1 * 31 + entity2
    }

    /**
     * 计算Binary Atom的哈希值（模拟k个不同的哈希函数）
     * 使用正数空间 [0, Int.MAX_VALUE]
     * 确保(entity1, entity2)和(entity2, entity1)产生不同的哈希值
     */
    fun computeBinaryHash(entity1: Int, entity2: Int, seed: Int): Int {
        // 使用Triple的hashCode，确保顺序敏感且高效
        var hash = pairHash(entity1, entity2)
        // 与seedIndex进行额外的混合，确保不同seedIndex产生不同结果
//        val finalHash = (hash xor seed) and Int.MAX_VALUE
        hash = pairHash(hash, seed)
//        require(finalHash < Int.MAX_VALUE && finalHash > 0)
        // 确保返回正数
        return abs(hash)
    }

    /**
     * 计算Unary Atom的哈希值（模拟k个不同的哈希函数）
     * 使用负数空间 [Int.MIN_VALUE, -1]
     */
    fun computeUnaryHash(entity: Int, seed: Int): Int {
        // 使用Pair的hashCode，简洁高效
        val hash = pairHash(entity, seed)
        // 与seedIndex进行额外的混合
//        val finalHash = - abs(hash) - 1
        // 确保返回负数
//        require(finalHash < 0)
        return hash or Int.MIN_VALUE
    }

    /**
     * LSH分桶 - 双级Map优化版本，直接使用MinHash索引，线程安全
     * 对于长路径（rpLength > 1）只添加到已存在的桶中，不创建新桶
     */
    fun performLSH(atom: MyAtom, minHashSignature: IntArray, supp: Int): Int {
        var hasRelevantAtom = false
        val relevantAtom = mutableSetOf<MyAtom>()  // 使用Set避免重复

        // 分为BANDS个band，每个band有R行，直接使用MinHash数组索引作为键
        for (bandIndex in 0 until BANDS) {
            val key1 = minHashSignature[bandIndex * R]     // 第一个MinHash值作为第一级键
            val key2 = minHashSignature[bandIndex * R + 1] // 第二个MinHash值作为第二级键
            
            // 查询相关的已有的桶
            val level1Map = band2headAtom[key1]
            if (level1Map != null) {
                val bucket = level1Map[key2]
                if (bucket != null) {
                    // 收集所有rpLength=1的相关公式
                    relevantAtom.addAll(bucket)
                    hasRelevantAtom = true
                }
            }

            // 如果是headAtom，放入桶中
            if (atom.isHeadAtom) {
                val level1Map = band2headAtom.getOrPut(key1) { mutableMapOf() }
                val bucket = level1Map.getOrPut(key2) { mutableListOf() }
                bucket.add(atom)
            }

            // 如果是L2Atom，放入桶中
            if (atom.isL2Atom) {
                val level1Map = band2L2Atom.getOrPut(key1) { mutableMapOf() }
                val bucket = level1Map.getOrPut(key2) { mutableListOf() }
                bucket.add(atom)
            }
        }

        // 进行相似性评估和过滤
        if (hasRelevantAtom)
            return findValidCombinations(atom, minHashSignature, supp, relevantAtom)

        return 0
    }
    
    /**
     * 评估和过滤公式组合 - 封装相似性评估和过滤逻辑
     */
    private fun findValidCombinations(
        myAtom: MyAtom,
        minHashSignature: IntArray, 
        mySupp: Int,
        relevantAtom: Set<MyAtom>
    ): Int {
        var cnt = 0

        // 估计与过滤阶段：评估候选对并过滤
        relevantAtom.forEach { atom ->
            val signature = minHashRegistry[Formula(atom)]
            if (signature != null) {
                // 估计Jaccard相似度
                val jaccard = estimateJaccardSimilarity(minHashSignature, signature)
                
                // 估计交集大小
                val supp = formula2supp[Formula(atom)]!!
                val intersectionSize = estimateIntersectionSize(jaccard, mySupp, supp)
                val metric = Metric(intersectionSize, supp, mySupp)

                if (metric.valid) {
                    println("\t$metric $atom")
                    cnt ++
                }
            }
        }
        if (cnt > 0) println("Found $cnt valid combinations for $myAtom:")
        return cnt
    }
    
    /**
     * 估计Jaccard相似度：J_est = (两个签名中值相等的哈希函数数量) / k
     */
    private fun estimateJaccardSimilarity(signature1: IntArray, signature2: IntArray): Double 
        = signature1.zip(signature2).count { (h1, h2) -> h1 == h2 }.toDouble() / signature1.size
    
    /**
     * 估计交集大小：I_est = J_est * (size_a1 + size_a2) / (1 + J_est)
     */
    private fun estimateIntersectionSize(jaccardSimilarity: Double, size1: Int, size2: Int): Double {
        val ret = jaccardSimilarity * (size1 + size2) / (1 + jaccardSimilarity)
        return min(ret, min(size1, size2).toDouble()) // 交集大小不应超过较小集合的大小
//        return ret
    }


    /**
     * 输出LSH分桶结果 - 适配双级Map，防止并发修改异常
     */
    fun printLSHBuckets() {
        println("LSH Buckets Summary:")
        
        // 创建快照以避免并发修改异常
        val band2headAtomSnapshot = synchronized(band2headAtom) {
            band2headAtom.mapValues { (_, level2Map) ->
                level2Map.mapValues { (_, formulas) ->
                    formulas.toList() // 创建不可变副本
                }.toMap()
            }.toMap()
        }
        
        val allBuckets = band2headAtomSnapshot.values.flatMap { it.values }
        println("Total buckets: ${allBuckets.size}")
        println("Total formulas in registry: ${minHashRegistry.size}")

        // 统计桶大小分布
        val bucketSizes = allBuckets.map { it.size }
        println("Bucket size distribution:")
        println("  Min: ${bucketSizes.minOrNull() ?: 0}")
        println("  Max: ${bucketSizes.maxOrNull() ?: 0}")
        println("  Average: ${bucketSizes.average()}")

        // 收集所有桶并按原子类型分类
        val allBucketsWithInfo = band2headAtomSnapshot.entries.flatMap { (key1, level2Map) ->
            level2Map.entries.map { (key2, atoms) ->
                Triple(key1, key2, atoms)
            }
        }
        
        // 定义过滤函数避免重复代码
        fun isBinaryBucket(atoms: List<MyAtom>) = atoms.first().entityId == IdManager.getYId()
        
        // 分离Binary和Unary桶，并统一显示
        val bucketTypes = listOf("Binary", "Unary")
        
        bucketTypes.forEach { bucketType ->
            val allBucketsOfType = allBucketsWithInfo.filter { (_, _, atoms) ->
                if (bucketType == "Binary") isBinaryBucket(atoms) else !isBinaryBucket(atoms)
            }
            val level1Keys = allBucketsOfType.map { it.first }.toSet().size
            val level2Keys = allBucketsOfType.size
            val top20Buckets = allBucketsOfType.sortedByDescending { it.third.size }.take(20)
            
            println("\nTop 20 largest $bucketType buckets:")
            println("Total $bucketType buckets: $level1Keys level-1 buckets, $level2Keys level-2 buckets")
            top20Buckets.forEachIndexed { index, (key1, key2, atoms) ->
                println("${index + 1}. $bucketType Bucket ($key1, $key2): ${atoms.size} atoms")
                atoms.take(10).forEach { atom ->
                    println("    $atom")
                }
                if (atoms.size > 10) {
                    println("    ... and ${atoms.size - 10} more")
                }
            }
        }
    }
}