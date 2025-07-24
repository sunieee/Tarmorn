package tarmorn

import tarmorn.Settings.load
import tarmorn.data.TripleSet
//import tarmorn.io.IOHelper
import tarmorn.structure.Dice
import tarmorn.structure.Rule
import tarmorn.threads.RuleWriterAsThread
import tarmorn.threads.Scorer
import java.io.FileNotFoundException
import java.io.PrintWriter
import java.text.DecimalFormat
import java.util.Collections
import kotlin.math.abs

object Learn {
    private var timeStamp: Long = 0

    // used at the begging to check if all threads are really available
    private val availableThreads = mutableSetOf<Int>()

    private var rwt: RuleWriterAsThread? = null

    /*
	 * Lets hope that people will not run AnyBURl with more than 100 cores ... up to these 307 buckets should be sufficient
	 * I somehow like the number 307
	 */
    private val rules307: Array<MutableSet<Rule>> = Array(307) {
        Collections.synchronizedSet(mutableSetOf<Rule>())
    }

    var stats: Array<IntArray> = arrayOf()

    var dice: Dice? = null


    @JvmField
    var active: Boolean = true
    var report: Boolean = false

    var activeThread: BooleanArray = BooleanArray(0)

    var finished: Boolean = false


    @Throws(FileNotFoundException::class, InterruptedException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        load()

        val df = DecimalFormat("000000.00")
        val log = PrintWriter(Settings.PATH_OUTPUT + "_log")
        log.println("Logfile")
        log.println("~~~~~~~\n")

        val indexStartTime = System.currentTimeMillis()

        val ts = TripleSet(Settings.PATH_TRAINING, true)


        //DecimalFormat df = new DecimalFormat("0.0000");
        //println("MEMORY REQUIRED (before precomputeNRandomEntitiesPerRelatio): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte at " + System.currentTimeMillis());
        ts.setupListStructure()
        ts.precomputeNRandomEntitiesPerRelation(Settings.BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS)
        println(" done.")


        //println("MEMORY REQUIRED (after precomputeNRandomEntitiesPerRelatio): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte at " + System.currentTimeMillis());
        val indexEndTime = System.currentTimeMillis()
        log.println("indexing dataset: " + Settings.PATH_TRAINING)
        log.println("time elapsed: " + (indexEndTime - indexStartTime) + "ms")
        log.println()
//        log.println(IOHelper.params)
        log.flush()


        var now = System.currentTimeMillis()


        // Thread[] scorer = new Thread[Learn.WORKER_THREADS];
        dice = Dice(Settings.PATH_DICE)
        dice!!.computeRelevenatScores()
        dice!!.saveScores()

        // new
        activeThread = BooleanArray(Settings.WORKER_THREADS)
        stats = Array<IntArray>(Settings.WORKER_THREADS) { IntArray(3) }

        val scorer = arrayOfNulls<Scorer>(Settings.WORKER_THREADS)
        for (threadCounter in 0..<Settings.WORKER_THREADS) {
            Thread.sleep(50)
            println("* creating worker thread #" + threadCounter)
            val s = Scorer(ts, threadCounter)

            val type = dice!!.ask(0)
            val zero = Dice.decodedDiceZero(type)
            val cyclic = Dice.decodedDiceCyclic(type)
            val acyclic = Dice.decodedDiceAcyclic(type)
            val len = Dice.decodedDiceLength(type)
            s.setSearchParameters(zero, cyclic, acyclic, len)

            scorer[threadCounter] = s
            scorer[threadCounter]!!.start()
            activeThread[threadCounter] = true
        }


        dice!!.resetScores()

        val done = false

        var batchCounter = 0


        // =================
        // === MAIN LOOP ===
        // =================
        val startTime = System.currentTimeMillis()


        var snapshotIndex = 0
        var batchStart = System.currentTimeMillis()
        while (done == false) {
            // println("main thread sleeps for 10 ms");
            Thread.sleep(10)

            now = System.currentTimeMillis()


            //elapsed seconds
            // snapshotIndex
            val elapsedSeconds = (now - startTime).toInt() / 1000
            val currentIndex = snapshotIndex
            snapshotIndex = Learn.checkTimeMaybeStoreRules(log, done, snapshotIndex, elapsedSeconds, dice!!)
            if (snapshotIndex > currentIndex) {
                // this needs t be done to avoid that a zeror time batch conducted because of long rule storage times
                batchStart = System.currentTimeMillis()
                now = System.currentTimeMillis()
                // println("currentIndex=" +  currentIndex +  " snapshotIndex=" + snapshotIndex);
                // println("now: " + now);
            }


            if (now - batchStart > Settings.BATCH_TIME) {
                report = true
                active = false

                /** println(">>> set status to inactive"); */
                do {
                    // println(">>> waiting for threads to report");
                    Thread.sleep(10)
                } while (!allThreadsReported())
                batchCounter++

                // saturation = getSaturationOfBatch();
                dice!!.computeRelevenatScores()
                dice!!.saveScores()


                var numOfRules = 0
                for (i in 0..306) numOfRules += rules307[i]!!.size


                // System.out.print(">>> Batch #" + batchCounter + " [" + numOfRules + " mined in " + (now - startTime) + "ms] ");
                // System.out.print(">>> Batch #" + batchCounter + " ");
                // println("MEMORY REQUIRED: " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte");
                for (t in scorer.indices) {
                    val type = dice!!.ask(batchCounter)
                    // System.out.print(type + "|");
                    val zero = Dice.decodedDiceZero(type)
                    val cyclic = Dice.decodedDiceCyclic(type)
                    val acyclic = Dice.decodedDiceAcyclic(type)
                    val len = Dice.decodedDiceLength(type)
                    scorer[t]!!.setSearchParameters(zero, cyclic, acyclic, len)
                }

                // System.out.print("   ");
                println(dice)

                dice!!.resetScores()

                Learn.activateAllThreads(scorer)
                batchStart = System.currentTimeMillis()
                active = true
                report = false
            }
        }


        // =================
        log.flush()
        log.close()
    }

    private fun checkTimeMaybeStoreRules(
        log: PrintWriter,
        done: Boolean,
        snapshotIndex: Int,
        elapsedSeconds: Int,
        dice: Dice
    ): Int {
        var snapshotIndex = snapshotIndex
        if (elapsedSeconds > Settings.SNAPSHOTS_AT!![snapshotIndex] || done) {
            active = false
            // this time might be required for letting the other threads go on one line in the code
            try {
                Thread.sleep(50)
            } catch (e1: InterruptedException) {
                e1.printStackTrace()
            }


            // ArrayList<Set<? extends Rule>> allUsefulRules = new ArrayList<Set<? extends Rule>>();


            // ...
            if (!done) println("\n>>> CREATING SNAPSHOT " + snapshotIndex + " after " + elapsedSeconds + " seconds")
            else println("\n>>> CREATING FINAL SNAPSHOT 0 after " + elapsedSeconds + " seconds")
            val suffix = "" + (if (done) 0 else Settings.SNAPSHOTS_AT!![snapshotIndex])
            rwt = RuleWriterAsThread(
                Settings.PATH_OUTPUT,
                if (done) 0 else Settings.SNAPSHOTS_AT!![snapshotIndex],
                rules307,
                log,
                elapsedSeconds
            )
            rwt!!.start()
            // storeRules(Settings.PATH_OUTPUT, done ? 0 : Settings.SNAPSHOTS_AT[snapshotIndex], rulesSyn, log, elapsedSeconds);
            println()
            dice.write(suffix)
            snapshotIndex++
            if (snapshotIndex == Settings.SNAPSHOTS_AT!!.size || done) {
                log.close()
                println(">>> Bye, bye.")
                finished = true


                while (rwt != null && rwt!!.isAlive()) {
                    try {
                        Thread.sleep(1000)
                    } catch (e: InterruptedException) {
                        e.printStackTrace()
                    }
                    println(">>> waiting for rule writer thread to finish")
                }


                System.exit(1)
            }
            active = true
        }
        return snapshotIndex
    }


    fun printStatsOfBatch() {
        for (i in stats.indices) {
            print("Worker #" + i + ": ")
            for (j in 0..<stats[i]!!.size - 1) {
                print(stats[i]!![j].toString() + " / ")
            }
            println(stats[i]!![stats[i]!!.size - 1])
        }
    }

    val saturationOfBatch: Double
        get() {
            var storedTotal = 0
            var createdTotal = 0
            for (i in stats.indices) {
                storedTotal += stats[i]!![0]
                createdTotal += stats[i]!![1]
            }
            return 1.0 - (storedTotal.toDouble() / createdTotal.toDouble())
        }


    /**
     * Called by a worker thread to see if batch time is over. If this is the case the thread is deactivated
     * and the thread specific statistics of its performance in the current batch are stored.
     *
     * @param threadId Id of the thread reporting.
     * @param storedRules the number of good rule that have been stored.
     * @param createdRules The number of created rules that have been created and checked for novelty and quality.
     * @return
     */
    @JvmStatic
    fun active(
        threadId: Int,
        storedRules: Int,
        createdRules: Int,
        producedScore: Double,
        zero: Boolean,
        cyclic: Boolean,
        acyclic: Boolean,
        len: Int
    ): Boolean {
        if (active) return true
        if (!report) return true
        else if (activeThread[threadId]) {
            // println("retrieved message from thread " + threadId + " created=" + createdRules + " stored=" + storedRules + " produced=" +producedScore);

            val type = Dice.encode(zero, cyclic, acyclic, len)
            // println("type of thread: " + type + " cyclic=" + cyclic + " len=" + len);
            stats[threadId]!![0] = storedRules
            stats[threadId]!![1] = createdRules
            // connect to the dice 
            // TODO
            //int type = Dice.encode(cyclic, len);
            dice!!.addScore(type, producedScore)
            activeThread[threadId] = false
            return false
        }
        return false
    }

    /**
     * Checks whether all worker threads have reported and are deactivated.
     *
     * @return True, if all threads have reported and are thus inactive.
     */
    fun allThreadsReported(): Boolean {
        for (i in activeThread.indices) {
            if (activeThread[i] == true) return false
        }
        return true
    }

    /**
     * Activates all threads .
     *
     * @param scorer The set of threads to be activated.
     */
    fun activateAllThreads(scorer: Array<Scorer?>) {
        for (i in activeThread.indices) {
            activeThread[i] = true
        }
        active = true
    }


    fun showElapsedMoreThan(duration: Long, message: String) {
        val now = System.currentTimeMillis()
        val elapsed = now - timeStamp
        if (elapsed > duration) {
            System.err.println(message + " required " + elapsed + " millis!")
        }
    }

    fun takeTime() {
        timeStamp = System.currentTimeMillis()
    }


    /**
     * Stores a given rule in a set. If the rule is a cyclic rule it also stores it in a way that is can be checked in
     * constants time for a AC1 rule if the AC1 follows.
     *
     * @param learnedRule
     */
    @JvmStatic
    fun storeRule(rule: Rule) {
        val code307 = abs(rule.hashCode()) % 307
        rules307[code307]!!.add(rule)
        // rulesSyn.add(rule);
        /*
		if (rule instanceof RuleCyclic) {
			indexXYRule((RuleCyclic)rule);
		}
		*/
    }

    /*
	private static synchronized void indexXYRule(RuleCyclic rule) {
		StringBuilder sb = new StringBuilder();
		sb.append(rule.getHead().toString());
		for (int i = 0; i < rule.bodysize(); i++) { sb.append(rule.getBodyAtom(i).toString()); }
		String rs = sb.toString();
		if (indexedXYRules.containsKey(rs)) {
			// should not happen
		}
		else {
			indexedXYRules.put(rs, rule);
		}
	}
	*/
    /**
     * Checks if the given rule is already stored.
     *
     */
    @JvmStatic
    fun isStored(rule: Rule): Boolean {
        val code307 = abs(rule.hashCode()) % 307
        if (!rules307[code307]!!.contains(rule)) {
            return true
        }
        return false
    }

    @JvmStatic
    fun areAllThere(): Boolean {
        // println("there are " + availableThreads.size() + " threads here" );
        if (availableThreads.size == Settings.WORKER_THREADS) return true
        return false
    }

    @JvmStatic
    fun heyYouImHere(id: Int) {
        availableThreads.add(id)
    }
}
