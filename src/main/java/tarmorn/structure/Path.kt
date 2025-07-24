package tarmorn.structure

import tarmorn.data.Triple

class Path(var nodes: Array<String>, var markers: CharArray) {
    override fun toString(): String {
        val p = StringBuilder("")
        for (i in 0..<this.nodes.size - 1) {
            p.append(this.markedNodeToString(i))
            p.append(" -> ")
        }
        p.append(this.markedNodeToString(this.nodes.size - 1))
        return p.toString()
    }


    private fun markedNodeToString(i: Int): String {
        if (i % 2 == 1) {
            return this.markers[(i - 1) / 2].toString() + this.nodes[i]
        } else {
            return this.nodes[i]
        }
    }

    // TODO write code
    override fun equals(that: Any?): Boolean {
        return false
    }

    // TODO write code
    override fun hashCode(): Int {
        return 7
    }

    val isValid: Boolean
        /**
         * Checks if a path is valid for strict object identity.
         *
         * @return False, if the x and y values appear at the wrong position in the path, or if the
         * same entities appears several times in the body part of the path.
         */
        get() {
            val xconst = this.nodes[0]
            val yconst = this.nodes[2]
            val visitedEntities = HashSet<String>()
            run {
                var i = 4
                while (i < nodes.size - 2) {
                    if (nodes[i] == xconst) {
                        return false
                    }
                    if (nodes[i] == yconst) {
                        return false
                    }
                    i += 2
                }
            }
            var i = 2
            while (i < nodes.size) {
                if (visitedEntities.contains(nodes[i])) return false
                visitedEntities.add(nodes[i])
                i += 2
            }
            return true
        }


    /**
     * Checks if a path is non cyclic, i.e, does not connect the entities of the given triple.
     *
     */
    fun isNonCyclic(t: Triple): Boolean {
        // println("path:   " + this);
        // println("triple: " + t);
        var i = 4
        while (i < nodes.size) {
            if (t.h == nodes[i]) return false
            if (t.t == nodes[i]) return false
            i += 2
        }
        return true
    }

    val isCyclic: Boolean
        /**
         * Checks if the path will result in a cyclic rule.
         *
         * @return True, if its a cyclic path.
         */
        get() {
            if (this.nodes[this.nodes.size - 1] == this.nodes[0] || this.nodes[this.nodes.size - 1] == this.nodes[2]) return true
            return false
        }
}
