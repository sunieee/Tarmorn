package tarmorn.topdown


import tarmorn.data.TripleSet
import tarmorn.structure.Atom
/**
 * Formula 类表示一个逻辑公式，由多个原子（Atom）组成。
 * 提供了二元和一元实例的计算方法，以及偏序关系的判断。
 */

class Formula(private val atoms: Set<Atom>) : Set<Atom> by atoms {

    // 判断是否为二元公式（所有atom都是二元的）
    val isBinary: Boolean
        get() = atoms.isNotEmpty() && atoms.all { it.isBinary }

    // 存储的二元实例，避免重复计算
    private var _binaryInstances: Set<Pair<Int, Int>>? = null

    // 存储的一元实例，避免重复计算
    private var _unaryInstances: Set<Int>? = null

    /**
     * 获取二元实例：所有atom的二元实例的交集
     * 对于数量>=2的情况，使用缓存避免重复计算
     */
    fun binaryInstances(tripleSet: TripleSet): Set<Pair<Int, Int>> {
        if (!isBinary) return emptySet()

        return when (atoms.size) {
            0 -> emptySet()
            1 -> atoms.first().binaryInstances(tripleSet)
            else -> {
                if (_binaryInstances == null) {
                    _binaryInstances = atoms
                        .map { it.binaryInstances(tripleSet) }
                        .reduce { acc, instances -> acc.intersect(instances) }
                }
                _binaryInstances!!
            }
        }
    }

    /**
     * 获取一元实例：所有atom的一元实例的交集
     * 对于数量>=2的情况，使用缓存避免重复计算
     */
    fun unaryInstances(tripleSet: TripleSet): Set<Int> {
        if (isBinary) return emptySet()

        return when (atoms.size) {
            0 -> emptySet()
            1 -> atoms.first().unaryInstances(tripleSet)
            else -> {
                if (_unaryInstances == null) {
                    _unaryInstances = atoms
                        .map { it.unaryInstances(tripleSet) }
                        .reduce { acc, instances -> acc.intersect(instances) }
                }
                _unaryInstances!!
            }
        }
    }

    /**
     * 通用的instances属性，根据是否为二元返回不同类型
     */
    @Suppress("UNCHECKED_CAST")
    fun <T> instances(tripleSet: TripleSet): Set<T> {
        return if (isBinary) {
            binaryInstances(tripleSet) as Set<T>
        } else {
            unaryInstances(tripleSet) as Set<T>
        }
    }

    /**
     * 判断当前公式是否是另一个公式的子集（偏序关系）
     * 利用Set的containsAll方法实现子集判断
     */
    fun isSubsetOf(other: Formula): Boolean {
        return other.atoms.containsAll(this.atoms)
    }

    /**
     * 判断当前公式是否包含另一个公式（偏序关系的反向）
     */
    fun contains(other: Formula): Boolean {
        return this.atoms.containsAll(other.atoms)
    }

    /**
     * 清除缓存的实例，当atoms发生变化时应该调用
     */
    private fun clearCache() {
        _binaryInstances = null
        _unaryInstances = null
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Formula) return false
        return atoms == other.atoms
    }

    override fun hashCode(): Int {
        return atoms.hashCode()
    }

    override fun toString(): String {
        return atoms.joinToString(" ∧ ", "(", ")")
    }

    companion object {
        /**
         * 创建空公式
         */
        fun empty(): Formula = Formula(emptySet())

        /**
         * 从单个Atom创建公式
         */
        fun of(atom: Atom): Formula = Formula(setOf(atom))

        /**
         * 从多个Atom创建公式
         */
        fun of(vararg atoms: Atom): Formula = Formula(atoms.toSet())

        /**
         * 从Atom集合创建公式
         */
        fun of(atoms: Collection<Atom>): Formula = Formula(atoms.toSet())
    }
}
