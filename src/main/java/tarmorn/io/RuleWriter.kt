package tarmorn.io

import tarmorn.structure.Rule
import java.io.FileNotFoundException
import java.io.PrintWriter

class RuleWriter {
    @Throws(FileNotFoundException::class)
    fun write(rules: Iterable<Rule>, filepath: String) {
        var i = 0
        val pw = PrintWriter(filepath)
        for (rule in rules) {
            pw.println(rule)
            i++
        }
        pw.flush()
        pw.close()
        println("* wrote " + i + " rules to " + filepath)
    }
}
