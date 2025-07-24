package tarmorn.io

import tarmorn.Settings
import tarmorn.structure.*
import java.io.*
import java.nio.charset.StandardCharsets

class RuleReader {
    /**
     * @param filepath The file to read the rules from.
     * @returnA list of rules.
     * @throws IOException
     */
    @Throws(IOException::class)
    fun read(filepath: String): MutableList<Rule> {
        print("* reading rules from " + filepath + "")
        // int i = 0;
        val rules = mutableListOf<Rule>()
        // HashMap<Long, Rule> ids2Rules = new HashMap<Long,Rule>();
        val file = File(filepath)
        val br = BufferedReader((InputStreamReader(FileInputStream(file), StandardCharsets.UTF_8)))
        var counter: Long = 0
        try {
            var line = br.readLine()
            while (line != null) {
                if (line == null || line == "") break
                val r = this.getRule(line)
                if (r != null && r.confidence >= Settings.READ_THRESHOLD_CONFIDENCE && r.correctlyPredicted >= Settings.READ_THRESHOLD_CORRECT_PREDICTIONS && r.bodysize() <= Settings.READ_THRESHOLD_MAX_RULE_LENGTH) {
                    rules.add(r)
                    counter++
                    if (counter % 1000000 == 0L) print(" ~")
                }
                line = br.readLine()
            }
        } finally {
            br.close()
        }
        println(", read " + rules.size + " rules")
        return rules
    }


    /**
     * @param filepath The file to read the rules from.
     * @returnA list of rules.
     * @throws IOException
     */
    @Throws(IOException::class)
    fun readRefinable(filepath: String): MutableList<Rule> {
        // int i = 0;
        val rules = mutableListOf<Rule>()
        // HashMap<Long, Rule> ids2Rules = new HashMap<Long,Rule>();
        val file = File(filepath)
        val br = BufferedReader((InputStreamReader(FileInputStream(file), StandardCharsets.UTF_8)))
        try {
            var line = br.readLine()
            while (line != null) {
                if (line == null || line == "") break
                // println(line);
                val r = this.getRule(line)
                if (r != null) {
                    if (r.isRefinable()) rules.add(r)
                }
                line = br.readLine()
            }
        } finally {
            br.close()
        }
        return rules
    }

    fun getRule(line: String): Rule? {
        if (line.startsWith("#")) return null
        val token: Array<String>? = line.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        // rule with constant in head
        var r: RuleUntyped? = null
        if (token!!.size == 4) {
            r = RuleUntyped(
                token[0]!!.toInt(),
                token[1]!!.toInt(),
                token[2]!!.toDouble()
            )
        }
        if (token.size == 7) {
            System.err.println("you are trying to read am old rule set which is based on head/tail distiction not yet supported anymore")
            System.exit(0)
        }
        r = r
        val atomsS: Array<String>? =
            token[token.size - 1]!!.split(" ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        r!!.head = Atom(atomsS!![0]!!)
        for (i in 2..<atomsS.size) {
            val lit = Atom(atomsS[i]!!)
            r.addBodyAtom(lit)
        }
        if (r.isCyclic) {
            if (Settings.READ_CYCLIC_RULES == 1) return RuleCyclic(r, 0.0)
        }
        if (r.isAcyclic1) {
            if (Settings.READ_ACYCLIC1_RULES == 1) return RuleAcyclic1(r)
        }
        if (r.isAcyclic2) {
            if (Settings.READ_ACYCLIC2_RULES == 1) return RuleAcyclic2(r)
        }
        if (r.isZero) {
            if (Settings.READ_ZERO_RULES == 1) return RuleZero(r)
        }
        return null
    }

    companion object {
        const val TYPE_UNDEFINED: Int = 0

        const val TYPE_CYCLIC: Int = 1
        const val TYPE_ACYCLIC: Int = 2
        const val TYPE_REFINED: Int = 3

        @Throws(IOException::class)
        @JvmStatic
        fun main(args: Array<String>) {
            val rr = RuleReader()
            val r1 = rr.read("exp/zero/fb237-rules-1000")

            for (r in r1) {
                if (r.head.toString().startsWith("/film/film/story_by") && r.head.toString()
                        .contains("X,Y") && r.appliedConfidence > 0.001
                ) {
                    println(r)
                }
            }


            // /music/genre/artists(/m/0dl5d,Y)
        }
    }
}
