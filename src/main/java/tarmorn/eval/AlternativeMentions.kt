package tarmorn.eval

import java.io.File
import java.io.IOException
import java.nio.charset.Charset
import java.nio.file.Files

class AlternativeMentions(filepath: String) {
    var alternatives: HashMap<String, HashSet<String>> = HashMap<String, HashSet<String>>()

    fun sameAs(e1: String, e2: String): Boolean {
        if (e1 == e2) {
            return true
        }
        if (!this.alternatives.containsKey(e1)) return false

        if (this.alternatives.get(e1)!!.contains(e2)) {
            return true
        }
        return false
    }

    init {
        val file = (File(filepath)).toPath()
        // Charset charset = Charset.forName("US-ASCII");
        val charset = Charset.forName("UTF8")
        var line: String? = null
        try {
            Files.newBufferedReader(file, charset).use { reader ->
                while ((reader.readLine().also { line = it }) != null) {
                    val token = line!!.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()


                    var subject = token[0]
                    var relation = token[1]
                    var `object` = token[2]
                    var subjectAlt = token[3]
                    var objectAlt = token[4]
                    subject = subject.replace(" ", "_")
                    relation = relation.replace(" ", "_")
                    `object` = `object`.replace(" ", "_")
                    subjectAlt = subjectAlt.replace(" ", "_")
                    objectAlt = objectAlt.replace(" ", "_")

                    if (!this.alternatives.containsKey(subject)) this.alternatives.put(subject, HashSet<String>())
                    if (!this.alternatives.containsKey(`object`)) this.alternatives.put(`object`, HashSet<String>())

                    for (sA in subjectAlt.split("\\|\\|\\|".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
                        this.alternatives.get(subject)!!.add(sA)
                    }
                    for (oA in objectAlt.split("\\|\\|\\|".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()) {
                        this.alternatives.get(`object`)!!.add(oA)
                    }
                }
            }
        } catch (x: IOException) {
            System.err.format("IOException: %s%n", x)
            System.err.format("Error occured for line: " + line + " LINE END")
        }
    }
}
