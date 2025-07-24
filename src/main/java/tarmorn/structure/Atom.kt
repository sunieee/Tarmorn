package tarmorn.structure

class Atom {
    @JvmField
    var relation: String
    @JvmField
    var left: String
    @JvmField
    var right: String

    var isLeftC: Boolean
    var isRightC: Boolean

    private var hashcode = 0
    private var hashcodeInitialized = false

    constructor(left: String, relation: String, right: String, leftC: Boolean, rightC: Boolean) {
        this.left = left
        this.right = right
        this.relation = relation
        this.isLeftC = leftC
        this.isRightC = rightC
    }

    val xYGeneralization: Atom
        get() {
            val copy = this.createCopy()
            copy.left = "X"
            copy.isLeftC = false
            copy.right = "Y"
            copy.isRightC = false
            return copy
        }

    constructor(a: String) {
        var a = a
        if (a.endsWith(" ")) a = a.substring(0, a.length - 1)
        if (a.endsWith(",")) a = a.substring(0, a.length - 1)
        if (a.endsWith(";")) a = a.substring(0, a.length - 1)

        var left: String = ""
        var right: String = ""


        val t1 = a.split("\\(".toRegex(), limit = 2).toTypedArray()
        val relation = t1[0]
        val aa = t1[1]

        if (aa.matches("[A-Z],.*\\)".toRegex())) {
            left = aa.substring(0, 1)
            right = aa.substring(2, aa.length - 1)
        } else {
            left = aa.substring(0, aa.length - 3)
            right = aa.substring(aa.length - 2, aa.length - 1)
        }
        this.relation = relation.intern()
        this.left = left.intern()
        this.right = right.intern()
        this.isLeftC = if (this.left.length == 1) false else true
        this.isRightC = if (this.right.length == 1) false else true
    }


    fun toString(indent: Int): String {
        var l = this.left
        var r = this.right
        if (indent > 0) {
            if (!this.isLeftC && (this.left != "X") && (this.left != "Y")) {
                val li: Int = Rule.variables2Indices.get(this.left)!!
                l = Rule.variables[li + indent]
            }
            if (!this.isRightC && (this.right != "X") && (this.right != "Y")) {
                val ri: Int = Rule.variables2Indices.get(this.right)!!
                r = Rule.variables[ri + indent]
            }
        }
        return this.relation + "(" + l + "," + r + ")"
    }

    override fun equals(thatObject: Any?): Boolean {
        if (thatObject is Atom) {
            val that = thatObject
            if (this.relation == that.relation && this.left == that.left && this.right == that.right) {
                return true
            }
        }
        return false
    }


    fun equals(that: Atom, vThis: String, vThat: String): Boolean {
        if (this.relation != that.relation) return false
        if ((this.left == vThis && that.left == vThat) || (this.left == vThis && that.left == vThat)) return true
        return false
    }


    /**
     * Returns true if this is more special than the given atom g.
     *
     * @param g
     * @return
     */
    fun moreSpecial(g: Atom): Boolean {
        if (this.relation == g.relation) {
            if (this == g) {
                return true
            }

            if (this.left == g.left) {
                if (!g.isRightC && this.isRightC) return true
                return false
            }
            if (this.right == g.right) {
                if (!g.isLeftC && this.isLeftC) return true
                return false
            }
            if (!g.isLeftC && !g.isRightC && this.isLeftC && this.isRightC) return true
            return false
        }
        return false
    }

    /**
     * Returns true if this is more special than the given atom g, given that vThis is substituted by vThat.
     *
     * @param g The more general atom.
     * @return
     */
    fun moreSpecial(that: Atom, vThis: String, vThat: String): Boolean {
        if (this.relation == that.relation) {
            if ((this.left == vThis && that.left == vThat)) {
                if (!that.isRightC && this.isRightC) return true
                if (that.right == this.right) return true
                return false
            }
            if ((this.right == vThis && that.right == vThat)) {
                if (!that.isLeftC && this.isLeftC) return true
                if (that.left == this.left) return true
                return false
            }
            return false
        }
        return false
    }


    override fun hashCode(): Int {
        if (!this.hashcodeInitialized) {
            this.hashcode = this.toString().hashCode()
            this.hashcodeInitialized = true
        }
        return this.hashcode
    }


    /**
     * Creates and returns a deep copy of this atom.
     *
     * @return A deep copy of this atom.
     */
    fun createCopy(): Atom {
        val copy = Atom(this.left, this.relation, this.right, this.isLeftC, this.isRightC)
        return copy
    }

    fun replaceByVariable(constant: String, variable: String): Int {
        var i = 0
        if (this.isLeftC && this.left == constant) {
            this.isLeftC = false
            this.left = variable
            i++
        }
        if (this.isRightC && this.right == constant) {
            this.isRightC = false
            this.right = variable
            i++
        }
        return i
    }

    fun replace(vOld: String, vNew: String, block: Int): Int {
        if (this.left == vOld && block != -1) {
            this.left = vNew
            return -1
        }
        if (this.right == vOld && block != 1) {
            this.right = vNew
            return 1
        }
        return 0
    }

    fun replace(vOld: String, vNew: String) {
        this.replace(vOld, vNew, 0)
    }

    fun uses(constantOrVariable: String): Boolean {
        if (this.left == constantOrVariable) {
            return true
        }
        if (this.right == constantOrVariable) {
            return true
        }
        return false
    }

    fun isLRC(leftNotRight: Boolean): Boolean {
        if (leftNotRight) return this.isLeftC
        else return this.isRightC
    }

    fun getLR(leftNotRight: Boolean): String {
        if (leftNotRight) return this.left
        else return this.right
    }

    fun contains(term: String): Boolean {
        if (this.left == term || this.right == term) return true
        return false
    }

    val constant: String
        get() {
            if (this.isLeftC) return this.left
            if (this.isRightC) return this.right
            return ""
        }

    fun isInverse(pos: Int): Boolean {
        val inverse: Boolean
        if (this.isRightC || this.isLeftC) {
            if (this.isRightC) inverse = false
            else inverse = true
        } else {
            if (this.right.compareTo(this.left) < 0) inverse = true
            else inverse = false
            if (pos == 0) return !inverse
        }
        return inverse
    }


    val variables: MutableSet<String>
        get() {
            val vars = HashSet<String>()
            if (!this.isLeftC && (this.left != "X") && (this.left != "Y")) {
                vars.add(this.left)
            }
            if (!this.isRightC && (this.right != "X") && (this.right != "Y")) {
                vars.add(this.right)
            }
            return vars
        }

//    fun getOtherTerm(v: String): String {
//        if (this.left == v) {
//            return this.right
//        } else if (this.right == v) {
//            return this.left
//        }
//        return null
//    }

    fun toString(c: String, v: String): String {
        return this.relation + "(" + (if (this.left == c) v else this.left) + "," + (if (this.right == c) v else this.right) + ")"
    }

    override fun toString(): String {
        return this.relation + "(" + this.left + "," + this.right + ")"
    }
}
