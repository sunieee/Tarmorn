package tarmorn.structure

import tarmorn.data.IdManager
import tarmorn.data.TripleSet
import java.util.Objects

class Atom(
    var h: Int,          // entity or variable ID
    var r: Long,     // relation ID
    var t: Int          // entity or variable ID
) {
    val ishC: Boolean
        get() = !IdManager.isVariable(h)
    val istC: Boolean
        get() = !IdManager.isVariable(t)

    val xYGeneralization: Atom
        get() = this.copy(h = IdManager.getXId(), t = IdManager.getYId())

    // 判断是否为二元原子 (X,Y两个自由变量)
    val isBinary: Boolean
        get() = h == IdManager.getXId() && t == IdManager.getYId()

    // 获取二元原子的实例 r(X,Y) -> Set<Pair<Int,Int>>
    fun binaryInstances(tripleSet: TripleSet): Set<Pair<Int, Int>> {
        return if (isBinary) {
            tripleSet.r2tripleSet[r]?.map { it.h to it.t }?.toSet() ?: emptySet()
        } else {
            emptySet()
        }
    }

    // 获取一元原子的实例
    fun unaryInstances(tripleSet: TripleSet): Set<Int> {
        val inverseRelation = IdManager.getInverseRelation(r)
        return when {
            isBinary -> emptySet()
            // r(X,c): 获取所有以c为尾部的逆关系实例的头部
            h == IdManager.getXId() && istC -> {
                tripleSet.r2h2tSet[inverseRelation]?.get(t) ?: emptySet()
            }
            // r(X,·): 获取逆关系的所有尾部实体
            h == IdManager.getXId() && t == 0 -> {
                tripleSet.r2h2tSet[r]?.keys?: emptySet()
            }
            // r(c,Y): 获取所有以c为头部的该关系实例的尾部
            ishC && t == IdManager.getYId() -> {
                tripleSet.r2h2tSet[r]?.get(h) ?: emptySet()
            }
            // r(·,Y): 获取该关系的所有尾部实体
            h == 0 && t == IdManager.getYId() -> {
                tripleSet.r2h2tSet[inverseRelation]?.keys?: emptySet()
            }
            else -> emptySet()
        }
    }

    // 通用的instances属性，根据是否为二元返回不同类型
    @Suppress("UNCHECKED_CAST")
    fun <T> instances(tripleSet: TripleSet): Set<T> {
        return if (isBinary) {
            binaryInstances(tripleSet) as Set<T>
        } else {
            unaryInstances(tripleSet) as Set<T>
        }
    }

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
        
        this.r = IdManager.getRelationId(relationStr.intern())
        this.h = IdManager.getEntityId(leftStr.intern())
        this.t = IdManager.getEntityId(rightStr.intern())
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
        if (this.r != g.r) return false
        
        return when {
            this == g -> true
            this.h == g.h -> !g.istC && this.istC
            this.t == g.t -> !g.ishC && this.ishC
            else -> !g.ishC && !g.istC && this.ishC && this.istC
        }
    }

    fun replaceByVariable(constantId: Int, variableId: Int): Int {
        var count = 0
        if (ishC && h == constantId) {
            h = variableId
            count++
        }
        if (istC && t == constantId) {
            t = variableId
            count++
        }
        return count
    }

    fun replace(vOldId: Int, vNewId: Int, block: Int = 0): Int = when {
        h == vOldId && block != -1 -> {
            h = vNewId
            -1
        }
        t == vOldId && block != 1 -> {
            t = vNewId
            1
        }
        else -> 0
    }

    fun uses(constantOrVariableId: Int): Boolean = 
        h == constantOrVariableId || t == constantOrVariableId

    fun isLRC(leftNotRight: Boolean): Boolean = 
        if (leftNotRight) ishC else istC

    fun getLR(leftNotRight: Boolean): Int = 
        if (leftNotRight) h else t

    fun contains(termId: Int): Boolean =
        h == termId || t == termId

    val constant: Int
        get() = when {
            ishC -> h
            istC -> t
            else -> -1
        }

    val variables: MutableSet<Int>
        get() = mutableSetOf<Int>().apply {
            if (!ishC && h != IdManager.getXId() && h != IdManager.getYId()) add(h)
            if (!istC && t != IdManager.getXId() && t != IdManager.getYId()) add(t)
        }

    fun toString(cId: Int, vId: Int): String {
        val relationStr = if (tarmorn.Settings.REMOVE_INVERSE_PREFIX_IN_OUTPUT) {
            IdManager.getRelationString(r).removePrefix("INVERSE_")
        } else {
            IdManager.getRelationString(r)
        }
        val leftStr = if (h == cId) IdManager.getEntityString(vId) else IdManager.getEntityString(h)
        val rightStr = if (t == cId) IdManager.getEntityString(vId) else IdManager.getEntityString(t)
        return "$relationStr($leftStr,$rightStr)"
    }

    override fun toString(): String {
        val relationStr = if (tarmorn.Settings.REMOVE_INVERSE_PREFIX_IN_OUTPUT) {
            IdManager.getRelationString(r).removePrefix("INVERSE_")
        } else {
            IdManager.getRelationString(r)
        }
        val leftStr = IdManager.getEntityString(h)
        val rightStr = IdManager.getEntityString(t)
        return "$relationStr($leftStr,$rightStr)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Atom) return false
        
        // Direct match
        if (h == other.h && r == other.r && t == other.t) return true
        
        // Check inverse relation equivalence
        // relation(A,B) == INVERSE_relation(B,A)
        val inverseRelationId = IdManager.getInverseRelation(r)
        return h == other.t && inverseRelationId == other.r && t == other.h
    }

    override fun hashCode(): Int {
        // Normalize the representation to ensure equivalent atoms have the same hash
        // Always use the smaller relation ID and arrange h,t accordingly
        val originalRelationId = if (IdManager.isInverseRelation(r)) {
            IdManager.getInverseRelation(r)
        } else {
            r
        }
        
        // For consistent hashing, always put the smaller entity first
        val (leftEntity, rightEntity) = if (IdManager.isInverseRelation(r)) {
            t to h  // For inverse relations, swap h and t
        } else {
            h to t
        }
        
        // Ensure consistent ordering for hash computation
        val (hashLeft, hashRight) = if (leftEntity <= rightEntity) {
            leftEntity to rightEntity
        } else {
            rightEntity to leftEntity
        }
        
        return Objects.hash(originalRelationId, hashLeft, hashRight)
    }

    fun copy(h: Int = this.h, r: Long = this.r, t: Int = this.t): Atom {
        return Atom(h, r, t)
    }
}
