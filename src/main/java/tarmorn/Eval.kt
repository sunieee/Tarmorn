package tarmorn

import tarmorn.Settings.load
import tarmorn.data.TripleSet
import tarmorn.eval.HitsAtK
import tarmorn.eval.ResultSet
import tarmorn.structure.Rule
import java.io.IOException

object Eval {
    private var CONFIG_FILE: String? = "config-eval.properties"


    /**
     * Read the top k from the ranking file for MRR computation. If less candidates are available, no error is thrown.
     */
    var TOP_K: Int = 100


    /**
     * Path to the file that contains the triple set used for learning the rules.
     */
    var PATH_TRAINING: String = ""


    /**
     * Path to the file that contains the triple set used for to test the rules.
     */
    var PATH_TEST: String = ""

    /**
     * Path to the file that contains the triple set used for validation purpose.
     */
    var PATH_VALID: String = ""

    /**
     * Path to the output file where the predictions are stored.
     */
    var PATH_PREDICTIONS: String = ""


    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size == 1) {
            CONFIG_FILE = args[0]
            println("reading params from file " + CONFIG_FILE)
        }

        Rule.applicationMode()
        load()
        Settings.REWRITE_REFLEXIV = false

        val trainingSet = TripleSet(PATH_TRAINING)
        val validationSet = TripleSet(PATH_VALID)
        val testSet = TripleSet(PATH_TEST)


        val values = Apply.getMultiProcessing(PATH_PREDICTIONS)

        val hitsAtK = HitsAtK()
        hitsAtK.addFilterTripleSet(trainingSet)
        hitsAtK.addFilterTripleSet(validationSet)
        hitsAtK.addFilterTripleSet(testSet)

        val sb = StringBuilder()
        if (values.size == 1) {
            val rs = ResultSet(PATH_PREDICTIONS, true, TOP_K)
            computeScores(rs, testSet, hitsAtK)
            println(hitsAtK.getHitsAtK(0) + "   " + hitsAtK.getHitsAtK(2) + "   " + hitsAtK.getHitsAtK(9) + "   " + hitsAtK.getApproxMRR())
        } else {
            for (value in values) {
                val rsPath = PATH_PREDICTIONS.replaceFirst("\\|.*\\|".toRegex(), "" + value)
                val rs = ResultSet(rsPath, true, TOP_K)
                computeScores(rs, testSet, hitsAtK)
                println(hitsAtK.getHitsAtK(0) + "   " + hitsAtK.getHitsAtK(2) + "   " + hitsAtK.getHitsAtK(9) + "   " + hitsAtK.getApproxMRR())
                sb.append(value + "   " + hitsAtK.getHitsAtK(0) + "   " + hitsAtK.getHitsAtK(9) + "   " + hitsAtK.getApproxMRR() + "\n")
            }
            println("-----")
            println(sb)
        }
    }


    private fun computeScores(rs: ResultSet, gold: TripleSet, hitsAtK: HitsAtK) {
        for (t in gold.getTriples()) {
            val cand1 = rs.getHeadCandidates(t.toString())
            // String c1 = cand1.size() > 0 ? cand1.get(0) : "-";
            hitsAtK.evaluateHead(cand1, t)
            val cand2 = rs.getTailCandidates(t.toString())
            // String c2 = cand2.size() > 0 ? cand2.get(0) : "-";
            hitsAtK.evaluateTail(cand2, t)
        }
    }
}
