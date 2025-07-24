package tarmorn.eval

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException

class GoldStandard {
    private var headTriplesToCat: HashMap<String, String?>
    private var tailTriplesToCat: HashMap<String, String?>

    var triples: ArrayList<String>


    constructor() {
        this.headTriplesToCat = HashMap<String, String?>()
        this.tailTriplesToCat = HashMap<String, String?>()
        this.triples = ArrayList<String>()
    }

    constructor(filePath: String) {
        this.headTriplesToCat = HashMap<String, String?>()
        this.tailTriplesToCat = HashMap<String, String?>()
        this.triples = ArrayList<String>()
        try {
            val file = File(filePath)
            val fileReader = FileReader(file)
            val bufferedReader = BufferedReader(fileReader)
            var tripleLine: String
            var previousTriple = ""
            while ((bufferedReader.readLine().also { tripleLine = it }) != null) {
                if (tripleLine!!.length < 3) continue
                val token = tripleLine.split("\t".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
                if (token[1] == "head") {
                    this.headTriplesToCat.put(token[0], token[2])
                }
                if (token[1] == "tail") {
                    this.tailTriplesToCat.put(token[0], token[2])
                }
                if (token[0] != previousTriple) {
                    this.triples.add(token[0])
                }
                previousTriple = token[0]
            }
            fileReader.close()
        } catch (e: IOException) {
            e.printStackTrace()
        }
    }

    fun getCategory(triple: String, ifHead: Boolean): String? {
        if (ifHead) {
            return this.headTriplesToCat.get(triple)
        } else {
            return this.tailTriplesToCat.get(triple)
        }
    }

    fun getSubset(category: String): GoldStandard {
        val gs = GoldStandard()
        for (t in this.triples) {
            var addedTriple = false
            if (this.headTriplesToCat.get(t) == category) {
                addedTriple = true
                gs.headTriplesToCat.put(t, category)
            } else {
                gs.headTriplesToCat.put(t, null)
            }
            if (this.tailTriplesToCat.get(t) == category) {
                addedTriple = true
                gs.tailTriplesToCat.put(t, category)
            } else {
                gs.tailTriplesToCat.put(t, null)
            }
            if (addedTriple) {
                gs.triples.add(t)
            }
        }
        return gs
    }
}
