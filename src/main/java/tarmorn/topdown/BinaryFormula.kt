package tarmorn.topdown

import tarmorn.data.IdManager

/**
 * BinaryFormula 类，代理 Triple<Long>
 * 实现hashCode基于三个元素的hashCode相加
 * 实现equals基于hashCode相等判断
 */
class BinaryFormula : Triple<Long, Long, Long> {
    
    /**
     * 构造函数：接受1个元素
     */
    constructor(first: Long) : super(
        first,
        0L,
        0L
    )
    
    /**
     * 构造函数：接受2个元素
     */
    constructor(first: Long, second: Long) : super(
        first,
        second,
        0L
    )
    
    /**
     * 构造函数：接受3个元素
     */
    constructor(first: Long, second: Long, third: Long) : super(
        first,
        second,
        third
    )
    
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
        if (other !is BinaryFormula) return false
        return this.hashCode() == other.hashCode()
    }
    
    override fun toString(): String {
        val nonZeroElements = listOfNotNull(
            if (first != 0L) IdManager.BinaryAtomToString(first) else null,
            if (second != 0L) IdManager.BinaryAtomToString(second) else null,
            if (third != 0L) IdManager.BinaryAtomToString(third) else null
        )
        return nonZeroElements.joinToString(", ")
    }
}
