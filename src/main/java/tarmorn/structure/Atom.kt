package tarmorn.structure

import tarmorn.data.IdManager

data class Atom(
    var left: Int,          // entity or variable ID
    var relation: Long,     // relation ID
    var right: Int          // entity or variable ID
) {
    val isLeftC: Boolean
        get() = !IdManager.isVariable(left)
    val isRightC: Boolean
        get() = !IdManager.isVariable(right)

    val xYGeneralization: Atom
        get() = this.copy(left = IdManager.getXId(), right = IdManager.getYId())

    constructor(input: String) : this(0, 0, 0) {
        // 清理输入字符串，移除尾部的空格、逗号和分号
        val cleanInput = input.trimEnd(' ', ',', ';')
        
        // 分割关系和参数部分
        val (relationStr, argsWithParen) = cleanInput.split('(', limit = 2)
        val args = argsWithParen.removeSuffix(")")
        
        // 解析参数
        val (leftStr, rightStr) = if (args.matches(Regex("[A-Z],.+"))) {
            // 单字符变量格式: "X,entity"
            args[0].toString() to args.substring(2)
        } else {
            // 标准格式: "entity1,X" 
            val lastCommaIndex = args.lastIndexOf(',')
            args.substring(0, lastCommaIndex) to args.substring(lastCommaIndex + 1)
        }
        
        this.relation = IdManager.getRelationId(relationStr.intern())
        this.left = IdManager.getEntityId(leftStr.intern())
        this.right = IdManager.getEntityId(rightStr.intern())
    }

    // Constructor from string values (for backward compatibility)
    constructor(left: String, relation: String, right: String) : this(
        IdManager.getEntityId(left),
        IdManager.getRelationId(relation),
        IdManager.getEntityId(right)
    )




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

    fun replaceByVariable(constantId: Int, variableId: Int): Int {
        var count = 0
        if (isLeftC && left == constantId) {
            left = variableId
            count++
        }
        if (isRightC && right == constantId) {
            right = variableId
            count++
        }
        return count
    }

    // String version for backward compatibility
    fun replaceByVariable(constant: String, variable: Int): Int = 
        replaceByVariable(IdManager.getEntityId(constant), variable)

    fun replace(vOldId: Int, vNewId: Int, block: Int = 0): Int = when {
        left == vOldId && block != -1 -> {
            left = vNewId
            -1
        }
        right == vOldId && block != 1 -> {
            right = vNewId
            1
        }
        else -> 0
    }

    fun uses(constantOrVariableId: Int): Boolean = 
        left == constantOrVariableId || right == constantOrVariableId

    fun isLRC(leftNotRight: Boolean): Boolean = 
        if (leftNotRight) isLeftC else isRightC

    fun getLR(leftNotRight: Boolean): Int = 
        if (leftNotRight) left else right

    fun contains(termId: Int): Boolean =
        left == termId || right == termId

    val constant: Int
        get() = when {
            isLeftC -> left
            isRightC -> right
            else -> -1
        }

    fun isInverse(pos: Int): Boolean {
        val inverse = when {
            isRightC || isLeftC -> !isRightC
            else -> {
                val leftStr = IdManager.getEntityString(left)
                val rightStr = IdManager.getEntityString(right)
                val baseInverse = rightStr < leftStr
                if (pos == 0) !baseInverse else baseInverse
            }
        }
        return inverse
    }

    val variables: MutableSet<Int>
        get() = mutableSetOf<Int>().apply {
            if (!isLeftC && left != IdManager.getXId() && left != IdManager.getYId()) add(left)
            if (!isRightC && right != IdManager.getXId() && right != IdManager.getYId()) add(right)
        }

    fun toString(cId: Int, vId: Int): String {
        val relationStr = IdManager.getRelationString(relation)
        val leftStr = if (left == cId) IdManager.getEntityString(vId) else IdManager.getEntityString(left)
        val rightStr = if (right == cId) IdManager.getEntityString(vId) else IdManager.getEntityString(right)
        return "$relationStr($leftStr,$rightStr)"
    }

    override fun toString(): String {
        val relationStr = IdManager.getRelationString(relation)
        val leftStr = IdManager.getEntityString(left)
        val rightStr = IdManager.getEntityString(right)
        return "$relationStr($leftStr,$rightStr)"
    }
}
