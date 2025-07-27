package tarmorn.structure

import tarmorn.data.IdManager

// 委托模式：Body 实现了 MutableList<Atom> 接口，但将所有接口方法的实现委托给一个 ArrayList<Atom> 实例
class Body : MutableList<Atom> by ArrayList() {

    // 保留自定义的 toString 格式（无括号）
    override fun toString(): String = 
        if (isEmpty()) "" else joinToString(", ")

    // 必须保留：包含特殊的变量映射逻辑，ArrayList 的默认 equals 无法处理
    override fun equals(thatObject: Any?): Boolean {
        if (thatObject !is Body) return false
        
        val that = thatObject
        if (size != that.size) return false
        
        val variablesThis2That = HashMap<Int, Int>()
        val variablesThat2This = HashMap<Int, Int>()
        
        for (i in indices) {
            val atom1 = this[i]
            val atom2 = that[i]
            
            if (atom1.r != atom2.r) return false
            
            if (!checkValuesAndVariables(variablesThis2That, variablesThat2This, atom1, atom2, true)) return false
            if (!checkValuesAndVariables(variablesThis2That, variablesThat2This, atom1, atom2, false)) return false
        }
        return true
    }

    private fun checkValuesAndVariables(
        variablesThis2That: HashMap<Int, Int>,
        variablesThat2This: HashMap<Int, Int>,
        atom1: Atom,
        atom2: Atom,
        leftNotRight: Boolean
    ): Boolean {
        if (atom1.isLRC(leftNotRight) && atom2.isLRC(leftNotRight)) {
            return atom1.getLR(leftNotRight) == atom2.getLR(leftNotRight)
        }
        
        if (atom1.isLRC(leftNotRight) != atom2.isLRC(leftNotRight)) {
            // one variable and one constants do not fit
            return false
        }
        
        if (!atom1.isLRC(leftNotRight) && !atom2.isLRC(leftNotRight)) {
            val value1 = atom1.getLR(leftNotRight)
            val value2 = atom2.getLR(leftNotRight)
            
            // special cases X must be at same position as X, Y at same as Y
            if (value1 == IdManager.getXId() && value2 != IdManager.getXId()) return false
            if (value2 == IdManager.getXId() && value1 != IdManager.getXId()) return false
            if (value1 == IdManager.getYId() && value2 != IdManager.getYId()) return false
            if (value2 == IdManager.getYId() && value1 != IdManager.getYId()) return false

            variablesThis2That[value1]?.let { thatV ->
                if (value2 != thatV) return false
            }
            
            variablesThat2This[value2]?.let { thisV ->
                if (value1 != thisV) return false
            }
            
            if (value1 !in variablesThis2That) {
                variablesThis2That[value1] = value2
                variablesThat2This[value2] = value1
            }
        }
        return true
    }

    val numOfVariables: Int
        get() = this.flatMap { it.variables }.toSet().size

    val last: Atom
        /**
         * Returns the last atom in this body.
         *
         * @return The last atom in this body.
         */
        get() = this.last()

    fun normalizeVariableNames() {
        val old2New = mutableMapOf<Int, Int>()
        var indexNewVariableNames = 0

        repeat(size) { i ->
            val atom = this[i]
            val variables = atom.variables
            var block = 0
            
            for (v in variables) {
                if (v == IdManager.getXId() || v == IdManager.getYId()) continue
                
                if (v !in old2New) {
                    val vNew = Rule.variables[indexNewVariableNames]
                    old2New[v] = vNew
                    indexNewVariableNames++
                }
                val block = atom.replace(v, old2New[v]!!, block)
            }
        }
    }

    /**
     * Replaces all atoms by deep copies of these atoms to avoid that references from the outside are affected by follow up changes.
     */
    fun detach() {
        indices.forEach { i ->
            this[i] = this[i].copy()!!
        }
    }
}
