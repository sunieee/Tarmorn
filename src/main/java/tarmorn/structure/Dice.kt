package tarmorn.structure

import tarmorn.Settings
import java.io.FileNotFoundException
import java.io.PrintWriter
import java.text.DecimalFormat
import java.util.*

class Dice @JvmOverloads constructor(filePath: String? = null) {
    private var OUTPUT_PATH: String? = null

    // [batch][type]
    private val timestamps = ArrayList<Long?>()

    private val scores = ArrayList<Array<Double?>?>()
    private val freqs = ArrayList<Array<Int?>?>()

    private val currentScores = DoubleArray(SUPPORTED_TYPES)
    private val currentFreqs = IntArray(SUPPORTED_TYPES)

    private val relevantScores = DoubleArray(SUPPORTED_TYPES)
    private var relevantScoresComputed = false


    init {
        this.OUTPUT_PATH = filePath
        for (i in 0..<SUPPORTED_TYPES) {
            this.currentScores[i] = INITIAL_SCORE
            this.currentFreqs[i] = 1
        }

        if (!Settings.ZERO_RULES_ACTIVE) this.currentScores[0] = 0.0
        // resets all unused slots to 0.0
        for (j in Settings.MAX_LENGTH_CYCLIC + 1..<SUPPORTED_TYPES_CYCLIC + 1) this.currentScores[j] = 0.0
        for (j in SUPPORTED_TYPES_CYCLIC + 1 + Settings.MAX_LENGTH_ACYCLIC..<SUPPORTED_TYPES) this.currentScores[j] =
            0.0
    }

    /**
     * Throws a dice for the rule types which is weighted according to the scores collected the last time this type
     * was mined.
     *
     * @return The type that was chosen by the dice.
     */
    fun ask(batchCounter: Int): Int {
        var r =
            (Settings.RANDOMIZED_DECISIONS_ANNEALING - batchCounter.toDouble()) / Settings.RANDOMIZED_DECISIONS_ANNEALING
        if (r < Settings.EPSILON) r = Settings.EPSILON


        if (rand.nextDouble() < r) {
            var i: Int
            do {
                i = rand.nextInt(SUPPORTED_TYPES)
            } while (this.scores.get(0)!![i] == 0.0)
            return i
        }

        if (Settings.POLICY == 1) {
            var score: Double
            var max = -100.0
            var maxIndex = -1
            for (i in 0..<SUPPORTED_TYPES) {
                score = relevantScores[i]
                if (score > max) {
                    maxIndex = i
                    max = score
                }
            }
            return maxIndex
        } else if (Settings.POLICY == 2) {
            if (this.relevantScoresComputed == false) throw RuntimeException("before asking the dice you have to compute the relevant scores")
            var total = 0.0
            for (i in 0..<SUPPORTED_TYPES) {
                total += relevantScores[i]
            }
            var d: Double = rand.nextDouble() * total

            for (i in 0..<SUPPORTED_TYPES) {
                if (d < relevantScores[i]) return i
                d -= relevantScores[i]
            }
        }
        return -1 // should never happen
    }


    fun computeRelevenatScores() {
        for (i in 0..<SUPPORTED_TYPES) {
            if (this.currentScores[i] > 0) {
                this.relevantScores[i] = this.currentScores[i] / this.currentFreqs[i].toDouble()
                // System.out.print(i + "<=" + this.relevantScores[i] + ", ");
            } else {
                // System.out.print(i + "==" + this.relevantScores[i] + ", ");
                // do nothing, keep relevant score from previous run
                // this might be the place to compute the last time this type has been tried
                // the more time is gone, the more probable it should be that this is used again
            }
        }
        this.relevantScoresComputed = true

        // System.out.println();
    }

    fun resetScores() {
        for (i in 0..<SUPPORTED_TYPES) {
            this.currentScores[i] = 0.0
            this.currentFreqs[i] = 0
        }
        this.relevantScoresComputed = false
    }

    @Synchronized
    fun addScore(index: Int, score: Double) {
        // System.out.println("type=" + index + " scored:" + score);
        this.currentScores[index] += score
        this.currentFreqs[index] += 1
        if (score == 0.0) this.currentScores[index] += GAMMA
    }

    fun saveScores() {
        this.scores.add(arrayOfNulls<Double>(SUPPORTED_TYPES))
        this.freqs.add(arrayOfNulls<Int>(SUPPORTED_TYPES))
        this.timestamps.add(System.currentTimeMillis())
        val lastScores = this.scores.get(this.scores.size - 1)!!
        val lastFreqs = this.freqs.get(this.scores.size - 1)!!
        for (i in this.currentScores.indices) {
            lastFreqs[i] = this.currentFreqs[i]
            lastScores[i] = this.currentScores[i] / (if (lastFreqs[i]!! > 0) lastFreqs[i] else 1)!!.toDouble()
        }
        // this.resetScores();
    }


    fun write(suffix: String?) {
        // System.out.println("writing to " + this.OUTPUT_PATH + suffix);
        if (this.OUTPUT_PATH == null) return
        try {
            val pw = PrintWriter(this.OUTPUT_PATH + "_" + suffix)

            for (n in this.scores.indices) {
                pw.print(this.timestamps.get(n))
                for (i in 0..<SUPPORTED_TYPES) {
                    pw.print("\t" + scores.get(n)!![i])
                    // sb.append("\t" + scores.get(n)[i]);
                }
                for (i in 0..<SUPPORTED_TYPES) {
                    pw.print("\t" + freqs.get(n)!![i])
                }
                pw.print("\n")
            }

            pw.flush()
            pw.close()
        } catch (e: FileNotFoundException) {
            // TODO Auto-generated catch block
            e.printStackTrace()
        }
    }

    override fun toString(): String {
        val df = DecimalFormat("000000")
        val sb = StringBuilder("")
        for (i in 0..<SUPPORTED_TYPES) {
            if (i == 1) sb.append(" |")
            if (i >= Settings.MAX_LENGTH_CYCLIC + 1 && i < SUPPORTED_TYPES_CYCLIC + 1) continue
            if (i - (SUPPORTED_TYPES_CYCLIC + 1) >= Settings.MAX_LENGTH_ACYCLIC) continue
            if (i == SUPPORTED_TYPES_CYCLIC + 1) sb.append(" |")
            val s = this.relevantScores[i]
            sb.append(" " + (if (s > 999999) " > 99k" else df.format(s)))
        }
        // sb.append("\n");	
        return sb.toString()
    }


    companion object {
        // already zero-adapted
        private const val SUPPORTED_TYPES = 14

        // already zero-adapted
        private const val SUPPORTED_TYPES_CYCLIC = 10

        // a small number larger than 0, which is saved as score, if there has been an attempt to mine that type
        // however, it has achieved a score of 0
        private const val GAMMA = 0.0001


        private val INITIAL_SCORE: Double = Double.Companion.MAX_VALUE / (SUPPORTED_TYPES + 1.0)

        private val rand = Random()


        // types listing, well, pretty awful way of doing this :-)
        // 0 = zero (the hero)
        // 1 = cyclic 1
        // 2 = cyclic 2
        // ... 
        // 10 = cyclic 10
        // 11 = acyclic 1
        // 12 = acyclic 2
        // 13 = acyclic 3
        @JvmStatic
        fun main(args: Array<String>) {
            // a simulated run;

            val numOfThreads = 10
            val dice = Dice()

            for (round in 0..39) {
                println("ROUND " + round)


                dice.computeRelevenatScores()
                dice.saveScores()
                println("DICE: " + dice)


                val thread: IntArray? = IntArray(numOfThreads)
                for (i in 0..<numOfThreads) {
                    val type = dice.ask(0)
                    thread!![i] = type
                    // System.out.print("dice=" + i + "=>" + type + "   ");
                }



                dice.resetScores()


                for (t in thread!!.indices) {
                    run {
                        val s: Double = Companion.simulateScore(thread[t])
                        dice.addScore(thread[t], s)
                    }
                }
            }
        }

        fun simulateScore(type: Int): Double {
            if (type == 3) return 100 + rand.nextDouble() * 10
            if (type == 5) return 500 + rand.nextDouble() * 10
            return 10 + rand.nextDouble() * 10
        }


        // decode and encode
        fun decodedDiceCyclic(dice: Int): Boolean {
            if (dice >= 0 && dice < SUPPORTED_TYPES_CYCLIC + 1) return true
            else return false
        }

        fun decodedDiceAcyclic(dice: Int): Boolean {
            if (dice >= SUPPORTED_TYPES_CYCLIC + 1) return true
            else return false
        }

        fun decodedDiceZero(dice: Int): Boolean {
            if (dice == 0) return true
            else return false
        }


        fun decodedDiceLength(dice: Int): Int {
            if (dice == 0) return 0
            if (decodedDiceCyclic(dice)) return dice
            else return dice - SUPPORTED_TYPES_CYCLIC
        }

        fun encode(zero: Boolean, cyclic: Boolean, acyclic: Boolean, len: Int): Int {
            if (len == 0) return 0
            if (cyclic) return len
            else return (SUPPORTED_TYPES_CYCLIC) + len
        }
    }
}
