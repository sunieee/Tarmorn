package tarmorn.threads

import tarmorn.structure.Rule
import tarmorn.structure.RuleAcyclic
import tarmorn.structure.RuleCyclic
import tarmorn.structure.RuleZero
import java.io.File
import java.io.IOException
import java.io.PrintWriter

class RuleWriterAsThread : Thread {
    private val rules: MutableList<Rule>
    private val filepath: String
    private val elapsedSeconds: Int
    private val log: PrintWriter?
    private val snapshotCounter: Int

    constructor(
        filepath: String,
        snapshotCounter: Int,
        rules307: Array<MutableSet<Rule>>,
        log: PrintWriter?,
        elapsedSeconds: Int
    ) {
        this.rules = mutableListOf<Rule>()
        for (i in 0..306) {
            val ruleSet = rules307[i]
            for (r in ruleSet) {
                this.rules.add(r)
            }
        }
        this.filepath = filepath
        this.elapsedSeconds = elapsedSeconds
        this.log = log
        this.snapshotCounter = snapshotCounter
    }

    constructor(
        filepath: String,
        snapshotCounter: Int,
        ruless: MutableList<MutableSet<Rule>>,
        log: PrintWriter?,
        elapsedSeconds: Int
    ) {
        this.rules = mutableListOf<Rule>()
        for (ruleSet in ruless) {
            for (r in ruleSet) {
                this.rules.add(r)
            }
        }
        this.filepath = filepath
        this.elapsedSeconds = elapsedSeconds
        this.log = log
        this.snapshotCounter = snapshotCounter
    }

    override fun run() {
        this.storeRules()
    }

    /*
	private void write() throws FileNotFoundException {
		println("* starting to write rules to " + filepath);
		long start = System.currentTimeMillis();
		
		int i = 0;
		PrintWriter pw = new PrintWriter(filepath);
		for (Rule rule : rules) {
			pw.println(rule);
			i++;
		}
		pw.flush();
		pw.close();
		long stop = System.currentTimeMillis();
		long elapsed = stop - start;
		println("* wrote " + i + " rules to " + filepath + " within " + elapsed +  " ms");
	}
	*/
    private fun storeRules() {
        val startWriting = System.currentTimeMillis()
        val ruleFile = File(this.filepath + "-" + this.snapshotCounter)
        val maxBodySize = 10
        var zeroCounter = 0
        val acyclicCounter = IntArray(maxBodySize)
        val cyclicCounter = IntArray(maxBodySize)

        try {
            if (log != null) log.println()
            if (log != null) log.println("rule file: " + ruleFile.getPath())
            println(">>> storing rules in file " + ruleFile.getPath())


            val pw = PrintWriter(ruleFile)
            var numOfRules: Long = 0
            for (r in rules) {
                if (r.bodysize() < maxBodySize) if (r is RuleCyclic) cyclicCounter[r.bodysize() - 1]++
                if (r is RuleAcyclic) acyclicCounter[r.bodysize() - 1]++
                if (r is RuleZero) zeroCounter++
                pw.println(r)
                numOfRules++
            }
            // }
            pw.flush()
            pw.close()

            if (log != null) log.println("zero: " + zeroCounter)
            if (log != null) log.print("cyclic: ")
            for (i in 0..<maxBodySize) {
                if (cyclicCounter[i] == 0) break
                if (log != null) log.print(cyclicCounter[i].toString() + " | ")
            }
            if (log != null) log.print("\nacyclic: ")
            for (i in 0..<maxBodySize) {
                if (acyclicCounter[i] == 0) break
                if (log != null) log.print(acyclicCounter[i].toString() + " | ")
            }
            // log.println("\nfinally reached coverage: " + df.format(lastC * 100) + "%");
            val stopWriting = System.currentTimeMillis()
            if (log != null) log.println("time planned: " + snapshotCounter + "s")
            if (log != null) log.println("time elapsed: " + elapsedSeconds + "s")
            println(">>> stored " + numOfRules + " rules in " + (stopWriting - startWriting) + "ms")
            if (log != null) log.println("")
            if (log != null) log.flush()
        } catch (e: IOException) {
            e.printStackTrace()
        }
    }
}
