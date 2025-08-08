package tarmorn.topdown

import tarmorn.data.IdManager

/**
 * UnaryFormula 类，代理 Triple<Pair<Long, Int>>
 * 实现hashCode基于三个元素的hashCode相加
 * 实现equals基于hashCode相等判断
 */
class UnaryFormula : Triple<Pair<Long, Int>, Pair<Long, Int>, Pair<Long, Int>> {
    constructor(first: Pair<Long, Int>) : super(first, 0L to 0, 0L to 0)
    constructor(first: Pair<Long, Int>, second: Pair<Long, Int>) : super(first, second, 0L to 0)
    constructor(first: Pair<Long, Int>, second: Pair<Long, Int>, third: Pair<Long, Int>) : super(first, second, third)
    
    /**
     * hashCode实现：三个元素的hashCode直接相加
     */
    override fun hashCode(): Int {
        return first.hashCode() + second.hashCode() + third.hashCode()
    }
    
    /**
     * equals实现：hashCode相等
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is UnaryFormula) return false
        return this.hashCode() == other.hashCode()
    }
    
    override fun toString(): String {
        val nonZeroElements = listOfNotNull(
            if (first != 0L to 0) IdManager.UnaryAtomToString(first.first, first.second) else null,
            if (second != 0L to 0) IdManager.UnaryAtomToString(second.first, second.second) else null,
            if (third != 0L to 0) IdManager.UnaryAtomToString(third.first, third.second) else null
        )
        return nonZeroElements.joinToString(", ")
    }
}
