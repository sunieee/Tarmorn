package tarmorn.structure

data class Atom(
    var left: String,
    var relation: String,
    var right: String
) {
    val isLeftC: Boolean
        get() = left.length != 1
    val isRightC: Boolean
        get() = right.length != 1

    val xYGeneralization: Atom
        get() = this.copy(left = "X", right = "Y")

    constructor(input: String) : this("", "", "") {
        // 清理输入字符串，移除尾部的空格、逗号和分号
        val cleanInput = input.trimEnd(' ', ',', ';')
        
        // 分割关系和参数部分
        val (relation, argsWithParen) = cleanInput.split('(', limit = 2)
        val args = argsWithParen.removeSuffix(")")
        
        // 解析参数
        val (left, right) = if (args.matches(Regex("[A-Z],.+"))) {
            // 单字符变量格式: "X,entity"
            args[0].toString() to args.substring(2)
        } else {
            // 标准格式: "entity1,X" 
            val lastCommaIndex = args.lastIndexOf(',')
            args.substring(0, lastCommaIndex) to args.substring(lastCommaIndex + 1)
        }
        
        this.relation = relation.intern()
        this.left = left.intern()
        this.right = right.intern()
    }


    fun toString(indent: Int): String {
        val l = if (indent > 0 && !isLeftC && left !in setOf("X", "Y")) {
            val li = Rule.variables2Indices[left]!!
            Rule.variables[li + indent]
        } else left
        
        val r = if (indent > 0 && !isRightC && right !in setOf("X", "Y")) {
            val ri = Rule.variables2Indices[right]!!
            Rule.variables[ri + indent]
        } else right
        
        return "$relation($l,$r)"
    }

    /**
     * Returns true if this is more special than the given atom g.
     * An atom is more special if it has the same relation and:
     * 1. They are equal, or
     * 2. Same left side but this has constant right while g has variable right, or  
     * 3. Same right side but this has constant left while g has variable left, or
     * 4. This has constants on both sides while g has variables on both sides
     */
    fun moreSpecial(g: Atom): Boolean {
        if (this.relation != g.relation) return false
        
        return when {
            this == g -> true
            this.left == g.left -> !g.isRightC && this.isRightC
            this.right == g.right -> !g.isLeftC && this.isLeftC
            else -> !g.isLeftC && !g.isRightC && this.isLeftC && this.isRightC
        }
    }

    fun replaceByVariable(constant: String, variable: String): Int {
        var count = 0
        if (isLeftC && left == constant) {
            left = variable
            count++
        }
        if (isRightC && right == constant) {
            right = variable
            count++
        }
        return count
    }

    fun replace(vOld: String, vNew: String, block: Int): Int = when {
        left == vOld && block != -1 -> {
            left = vNew
            -1
        }
        right == vOld && block != 1 -> {
            right = vNew
            1
        }
        else -> 0
    }

    fun replace(vOld: String, vNew: String) = replace(vOld, vNew, 0)

    fun uses(constantOrVariable: String): Boolean = 
        left == constantOrVariable || right == constantOrVariable

    fun isLRC(leftNotRight: Boolean): Boolean = 
        if (leftNotRight) isLeftC else isRightC

    fun getLR(leftNotRight: Boolean): String = 
        if (leftNotRight) left else right

    fun contains(term: String): Boolean = 
        left == term || right == term

    val constant: String
        get() = when {
            isLeftC -> left
            isRightC -> right
            else -> ""
        }

    fun isInverse(pos: Int): Boolean {
        val inverse = when {
            isRightC || isLeftC -> !isRightC
            else -> {
                val baseInverse = right < left
                if (pos == 0) !baseInverse else baseInverse
            }
        }
        return inverse
    }


    val variables: MutableSet<String>
        get() = mutableSetOf<String>().apply {
            if (!isLeftC && left !in setOf("X", "Y")) add(left)
            if (!isRightC && right !in setOf("X", "Y")) add(right)
        }

    fun toString(c: String, v: String): String = 
        "$relation(${if (left == c) v else left},${if (right == c) v else right})"

    override fun toString(): String = "$relation($left,$right)"
}
