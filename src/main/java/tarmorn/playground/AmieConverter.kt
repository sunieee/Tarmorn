package tarmorn.playground

import tarmorn.io.RuleReader
import tarmorn.structure.Rule
import java.io.*
import java.nio.charset.StandardCharsets
import java.util.Scanner

/**
 * Coverts the output of AMIE into the rule format of AnyBURL.
 * @author Christian
 */
object AmieConverter {
    var vars: Array<String> = arrayOf<String>("X", "A", "B", "C", "D", "E")


    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val inputPath = "exp/large-final/amie/amie-amf-yago-maxad4-rules.txt"
        val outputPath = "exp/large-final/amie/amie-abf-yago-maxad4-rules-new-xxx.txt"


        val pw = PrintWriter(outputPath)

        val rr = RuleReader()


        // ?a  hasChild  ?p  ?b  isMarriedTo  ?a  ?a  isMarriedTo  ?p   => ?a  hasChild  ?b	0.011650869	0.776315789	0.867647059	59	76	68	?b
        // ?p  hasChild  ?b  ?b  isMarriedTo  ?a  ?b  isMarriedTo  ?p   => ?a  hasChild  ?b	0.011650869	0.776315789	0.776315789	59	76	76	?b

        // Rule r = convertLine(rr, "?a  hasChild  ?p  ?b  isMarriedTo  ?a  ?a  isMarriedTo  ?p   => ?a  hasChild  ?b	0.011650869	0.776315789	0.867647059	59	76	68	?b");
        // println(r);
        //System.exit(0);

        // Rule r = convertLine(rr, "?g  dealsWith  ?a  ?g  dealsWith  ?b   => ?a  dealsWith  ?b	0.475422427	0.193196005	0.229769859	619	3204	2694	?a");
        //Rule r = convertLine(rr, "?b  dealsWith  ?a  ?a  hasNeighbor  ?b   => ?a  dealsWith  ?b	0.041474654	0.327272727	0.350649351	54	165	154	?a");


        // println(r);
        convertFile(rr, pw, inputPath)


        //Rule r = convertLine(rr, "?h  livesIn  ?b  ?h  hasAcademicAdvisor  ?a   => ?a  isCitizenOf  ?b	0.041474654	0.327272727	0.350649351	54	165	154	?a");


        //println("Extracted rule: " +  r);
    }

    @Throws(IOException::class)
    fun convertFile(rr: RuleReader, pw: PrintWriter, inputPath: String) {
        val file = File(inputPath)
        val br = BufferedReader((InputStreamReader(FileInputStream(file), StandardCharsets.UTF_8)))

        var validRuleCounter = 0
        var invalidRuleCounter = 0



        try {
            var line = br.readLine()
            while (line != null) {
                if (line == null || line == "") break
                if (line.startsWith("?")) {
                    val r = convertLine(rr, line)

                    if (r == null) {
                        invalidRuleCounter++
                        // println(line);
                    } else {
                        pw.write(r.toString() + "\n")
                        validRuleCounter++
                    }
                }
                line = br.readLine()
            }
        } finally {
            br.close()
        }
        pw.flush()
        pw.close()
        println("converted amie rule file, valid rules = " + validRuleCounter + ", invalid rules = " + invalidRuleCounter + ".")
    }


    fun convertLine(rr: RuleReader, line: String): Rule? {
        val sb = StringBuilder("")
        // println(line);
        val parts = line.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val hb = parts[0].split("   => ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val body = hb[0]
        val head = hb[1]

        val correctlyPredicted = parts[4].toInt()
        val predicted = parts[5].toInt()

        sb.append(predicted.toString() + "\t")
        sb.append(correctlyPredicted.toString() + "\t")
        sb.append((correctlyPredicted.toDouble() / predicted.toDouble()).toString() + "\t")


        val headToken = head.split("  ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        val xVar = headToken[0]
        val targetRelation: String = headToken[1]
        val yVar: String = headToken[2]


        sb.append(targetRelation + "(X,Y) <= ")


        // println(targetRelation + "(X,Y) <=");
        val bodyToken = body.split("  ".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()

        var v = xVar
        var xc = "none"
        var counter = 0


        // println("xVar = " + xVar + "|yVar=" + yVar + "|");
        while (v != yVar) {
            // println("v = " + v);
            val ni = getIndexOfExcluded(v, xc, bodyToken)
            if (ni == -1) return null
            // println("ni = " + ni);
            val indexAtom = ni / 3
            val indexWithinAtom = ni % 3
            val otherIndexWithinAtom = (if (indexWithinAtom == 0) 2 else 0)
            val no = otherIndexWithinAtom + indexAtom * 3
            val relationIndex = indexAtom * 3 + 1

            val relation: String = bodyToken[relationIndex]
            xc = v
            v = bodyToken[no]

            var v1 = vars[counter]
            var v2 = vars[counter + 1]
            if (v == yVar) v2 = "Y"
            if (indexWithinAtom == 2) {
                val tmp = v1
                v1 = v2
                v2 = tmp
            }

            sb.append(relation + "(" + v1 + "," + v2 + ")")
            if (v != yVar) sb.append(", ")

            counter++
        }


        //println(sb.toString());

        //println("bodyToken: " + bodyToken.length);
        //println("counter: " + counter);
        if (bodyToken.size / 3 == counter) {
            val r = rr.getRule(sb.toString())
            return r
        } else {
            return null
        }
    }

    private fun getIndexOfExcluded(`var`: String, excluded: String, token: Array<String>): Int {
        for (i in token.indices) {
            if (token[i] == `var`) {
                val atomIndex = i / 3
                val indexWithinAtom = i % 3
                val otherIndexWithinAtom = atomIndex * 3 + (if (indexWithinAtom == 0) 2 else 0)
                if (token[otherIndexWithinAtom] != excluded) return i
            }
        }
        return -1
    }

    fun read() {
        val scan = Scanner(System.`in`)
        scan.nextLine()
    }
}
