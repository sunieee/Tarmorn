package tarmorn

import tarmorn.Settings.load
import tarmorn.threads.RuleEngine
import tarmorn.data.TripleSet
import tarmorn.threads.RuleReader
import tarmorn.structure.Rule
import tarmorn.structure.RuleAcyclic
import tarmorn.structure.RuleCyclic
import java.io.File
import java.io.IOException
import java.io.PrintWriter

object Apply {
    private var PW_JOINT_FILE: String = ""


    /**
     * Filter the rule set prior to applying it. Removes redundant rules which do not have any impact (or no desired impact).
     */
    // public static boolean FILTER = true;	
    /**
     * Always should be set to false. The TILDE results are based on a setting where this is set to true.
     * This parameter is sued to check in how far this setting increases the quality of the results.
     */
    var USE_VALIDATION_AS_BK: Boolean = false


    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size == 1) {
            PW_JOINT_FILE = args[0]
            println("* using pairwise joint file file " + PW_JOINT_FILE)
        }

        Rule.applicationMode()
        load()
        assert(Settings.PREDICTION_TYPE == "aRx")

        val values = Apply.getMultiProcessing(Settings.PATH_RULES)

        var log: PrintWriter? = null

        if (values.size == 0) log = PrintWriter(Settings.PATH_RULES + "_plog")
        else log = PrintWriter(Settings.PATH_OUTPUT!!.replace("|", "") + "_plog")

        log.println("Logfile")
        log.println("~~~~~~~\n")
        log.println()
//        log.println(IOHelper.params)
        log.flush()

        val rr = RuleReader()
        var base = mutableListOf<Rule>()


        if (Settings.PATH_RULES_BASE != "") {
            println("* reading additional rule file as base")
            base = rr.read(Settings.PATH_RULES_BASE)
        }



        for (value in values) {
            val startTime = System.currentTimeMillis()


            // long indexStartTime = System.currentTimeMillis();
            var path_output_used: String
            var path_rules_used: String
            if (value == null) {
                path_output_used = Settings.PATH_OUTPUT
                path_rules_used = Settings.PATH_RULES
            } else {
                path_output_used = Settings.PATH_OUTPUT!!.replaceFirst("\\|.*\\|".toRegex(), "" + value)
                path_rules_used = Settings.PATH_RULES!!.replaceFirst("\\|.*\\|".toRegex(), "" + value)
            }
            log.println("rules:   " + path_rules_used)
            log.println("output: " + path_output_used)
            log.flush()

            val pw = PrintWriter(File(path_output_used))


             if (Settings.PATH_EXPLANATION != "") Settings.EXPLANATION_WRITER =
                PrintWriter(File(Settings.PATH_EXPLANATION))
            println("* writing prediction to " + path_output_used)

            val trainingSet = TripleSet(Settings.PATH_TRAINING)

            val testSet = TripleSet(Settings.PATH_TEST)
            var validSet = TripleSet(Settings.PATH_VALID)


            // check if you should predict only unconnected
            // ACHTUNG: Never remove that comment here
            // checkIfPredictOnlyUnconnected(validSet, trainingSet);
            if (USE_VALIDATION_AS_BK) {
                trainingSet.addTripleSet(validSet)
                validSet = TripleSet()
            }


            //DecimalFormat df = new DecimalFormat("0.0000");
            //println("MEMORY REQUIRED (before reading rules): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte");
            var rules = rr.read(path_rules_used)

            rules.addAll(base)


            //println("MEMORY REQUIRED (after reading rules): " + df.format(Runtime.getRuntime().totalMemory() / 1000000.0) + " MByte");
            val rulesSize = rules.size
            val rulesThresholded = mutableListOf<Rule>()
            if (Settings.THRESHOLD_CONFIDENCE > 0.0) {
                for (r in rules) {
                    // if (r instanceof RuleAcyclic1 && (r.bodysize() == 3 || r.bodysize() == 2) && r.getHead().getConstant().equals(r.getBodyAtom(r.bodysize()-1).getConstant())) continue;

                    if (r.confidence > Settings.THRESHOLD_CONFIDENCE) {
                        rulesThresholded.add(r)
                    }
                }
                println("* applied confidence threshold of " + Settings.THRESHOLD_CONFIDENCE + " and reduced from " + rules.size + " to " + rulesThresholded.size + " rules")
            }
            rules = rulesThresholded

            val startApplicationTime = System.currentTimeMillis()
            RuleEngine.applyRulesARX(rules, testSet, trainingSet, validSet, Settings.TOP_K_OUTPUT, pw)


            val endTime = System.currentTimeMillis()
            println("* evaluated " + rulesSize + " rules to propose candiates for " + testSet.triples.size + "*2 completion tasks")
            println("* finished in " + (endTime - startTime) + "ms.")


            println()


            val indexEndTime = System.currentTimeMillis()

            log.println("finished in " + (endTime - startApplicationTime) / 1000 + "s (rule indexing and application, creation and storage of ranking).")
            log.println("finished in " + (endTime - startTime) / 1000 + "s including all operations (+ loading triplesets,  + loading rules).")
            log.println()
            log.flush()
        }
        log.close()
    }


    private fun filterTSA(TSA: Array<String>, TSAindex: Int, rulesThresholded: MutableList<Rule>, r: Rule) {
        when (TSA[TSAindex]) {
            "ALL" -> rulesThresholded.add(r)
            "C-1" -> if (r is RuleCyclic && r.bodysize() == 1) rulesThresholded.add(r)
            "C-2" -> if (r is RuleCyclic && r.bodysize() == 2) rulesThresholded.add(r)
            "C-3" -> if (r is RuleCyclic && r.bodysize() == 3) rulesThresholded.add(r)
            "AC1-1" -> if (r is RuleAcyclic1 && r.bodysize() == 1) rulesThresholded.add(r)
            "AC1-2" -> if (r is RuleAcyclic1 && r.bodysize() == 2) rulesThresholded.add(r)
            "AC2-1" -> if (r is RuleAcyclic2 && r.bodysize() == 1) rulesThresholded.add(r)
            "AC2-2" -> if (r is RuleAcyclic2 && r.bodysize() == 2) rulesThresholded.add(r)
            "N-C-1" -> if (!(r is RuleCyclic && r.bodysize() == 1)) rulesThresholded.add(r)
            "N-C-2" -> if (!(r is RuleCyclic && r.bodysize() == 2)) rulesThresholded.add(r)
            "N-C-3" -> if (!(r is RuleCyclic && r.bodysize() == 3)) rulesThresholded.add(r)
            "N-AC1-1" -> if (!(r is RuleAcyclic1 && r.bodysize() == 1)) rulesThresholded.add(r)
            "N-AC1-2" -> if (!(r is RuleAcyclic1 && r.bodysize() == 2)) rulesThresholded.add(r)
            "N-AC2-1" -> if (!(r is RuleAcyclic2 && r.bodysize() == 1)) rulesThresholded.add(r)
            "N-AC2-2" -> if (!(r is RuleAcyclic2 && r.bodysize() == 2)) rulesThresholded.add(r)
        }
    }


    /*
	private static void showRulesStats(List<Rule> rules) { 
		int xyCounter = 0;
		int xCounter = 0;
		int yCounter = 0;
		for (Rule rule : rules) {
			if (rule.isXYRule()) xyCounter++;
			if (rule.isXRule()) xCounter++;
			if (rule.isYRule()) yCounter++;
		}
		println("XY=" + xyCounter + " X="+ xCounter + " Y=" + yCounter);
		
	}
	*/
    fun getMultiProcessing(path1: String): Array<String> {
        val token: Array<String>? = path1.split("\\|".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        if (token!!.size < 2) {
            return arrayOf<String>()
        } else {
            val values: Array<String> =
                token[1].split(",".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
            return values
        }
    }
}