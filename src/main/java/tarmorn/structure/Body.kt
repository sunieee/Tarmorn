package tarmorn.structure

class Body : Iterable<Atom> {
    private var hashcode = 0
    private var hashcodeInitialized = false

    protected var literals: ArrayList<Atom>

    init {
        this.literals = ArrayList<Atom>()
    }

    fun add(atom: Atom) {
        this.literals.add(atom)
    }

    fun get(index: Int): Atom {
        return this.literals.get(index)
    }

    fun set(index: Int, atom: Atom?) {
        this.literals.set(index, atom!!)
    }

    fun size(): Int {
        return this.literals.size
    }

    override fun iterator(): MutableIterator<Atom> {
        return this.literals.iterator()
    }


    fun contains(a: Atom): Boolean {
        for (lit in this.literals) {
            if (a == lit) return true
        }
        return false
    }


    override fun hashCode(): Int {
        if (this.hashcodeInitialized) return this.hashcode
        val sb = StringBuilder()
        for (a in this.literals) {
            sb.append(a.toString())
        }
        this.hashcode = sb.toString().hashCode()
        this.hashcodeInitialized = true
        return this.hashcode
    }

    override fun toString(): String {
        if (this.literals.size == 0) return ""
        val sb = StringBuilder()
        for (i in 0..<this.literals.size - 1) {
            sb.append(this.literals.get(i))
            sb.append(", ")
        }
        sb.append(this.literals.get(this.literals.size - 1))
        return sb.toString()
    }

    fun toString(indent: Int): String {
        val sb = StringBuilder()
        for (i in 0..<this.literals.size - 1) {
            sb.append(this.literals.get(i).toString(indent))
            sb.append(", ")
        }
        sb.append(this.literals.get(this.literals.size - 1).toString(indent))
        return sb.toString()
    }

    override fun equals(thatObject: Any?): Boolean {
        if (thatObject is Body) {
            val that = thatObject
            if (this.literals.size == that.literals.size) {
                val variablesThis2That = HashMap<String, String>()
                val variablesThat2This = HashMap<String, String>()
                for (i in this.literals.indices) {
                    val atom1 = this.literals.get(i)
                    val atom2 = that.literals.get(i)
                    if (atom1.relation != atom2.relation) {
                        return false
                    } else {
                        if (!checkValuesAndVariables(
                                variablesThis2That,
                                variablesThat2This,
                                atom1,
                                atom2,
                                true
                            )
                        ) return false
                        if (!checkValuesAndVariables(
                                variablesThis2That,
                                variablesThat2This,
                                atom1,
                                atom2,
                                false
                            )
                        ) return false
                    }
                }
                return true
            }
        }
        return false
    }

    private fun checkValuesAndVariables(
        variablesThis2That: HashMap<String, String>,
        variablesThat2This: HashMap<String, String>,
        atom1: Atom,
        atom2: Atom,
        leftNotRight: Boolean
    ): Boolean {
        if (atom1.isLRC(leftNotRight) && atom2.isLRC(leftNotRight)) {
            if (atom1.getLR(leftNotRight) != atom2.getLR(leftNotRight)) {
                return false
            }
        }
        if (atom1.isLRC(leftNotRight) != atom2.isLRC(leftNotRight)) {
            // one variable and one constants do not fit
            return false
        }
        if (!atom1.isLRC(leftNotRight) && !atom2.isLRC(leftNotRight)) {
            // special cases X must be at same position as X, Y at same as Y
            if (atom1.getLR(leftNotRight) == "X" && atom2.getLR(leftNotRight) != "X") return false
            if (atom2.getLR(leftNotRight) == "X" && atom1.getLR(leftNotRight) != "X") return false

            if (atom1.getLR(leftNotRight) == "Y" && atom2.getLR(leftNotRight) != "Y") return false
            if (atom2.getLR(leftNotRight) == "Y" && atom1.getLR(leftNotRight) != "Y") return false

            if (variablesThis2That.containsKey(atom1.getLR(leftNotRight))) {
                val thatV = variablesThis2That.get(atom1.getLR(leftNotRight))
                if (atom2.getLR(leftNotRight) != thatV) return false
            }
            if (variablesThat2This.containsKey(atom2.getLR(leftNotRight))) {
                val thisV = variablesThat2This.get(atom2.getLR(leftNotRight))
                if (atom1.getLR(leftNotRight) != thisV) return false
            }
            if (!variablesThis2That.containsKey(atom1.getLR(leftNotRight))) {
                variablesThis2That.put(atom1.getLR(leftNotRight), atom2.getLR(leftNotRight))
                variablesThat2This.put(atom2.getLR(leftNotRight), atom1.getLR(leftNotRight))
            }
        }
        return true
    }

    val numOfVariables: Int
        get() {
            val variables = HashSet<String>()
            for (a in this.literals) {
                variables.addAll(a.variables)
            }
            return variables.size
        }

    val last: Atom
        /**
         * Returns the last atom in this body.
         *
         * @return The last atom in this body.
         */
        get() = this.get(this.literals.size - 1)

    fun normalizeVariableNames() {
        val old2New = HashMap<String, String>()
        var indexNewVariableNames = 0


        for (i in 0..<this.size()) {
            val atom = this.get(i)
            val variables = atom.variables
            var block = 0
            for (v in variables) {
                if (v == "X" || v == "Y") continue
                if (!old2New.containsKey(v)) {
                    val vNew = Rule.variables[indexNewVariableNames]
                    old2New[v] = vNew
                    indexNewVariableNames++
                }
                block = atom.replace(v, old2New[v]!!, block)
            }
        }
    }

    /**
     * Replaces all atoms by deep copies of these atoms to avoid that references from the outside are affected by follow up changes.
     */
    fun detach() {
        for (i in this.literals.indices) {
            val atom = this.literals.get(i).copy()
            this.literals.set(i, atom!!)
        }
    }
}
