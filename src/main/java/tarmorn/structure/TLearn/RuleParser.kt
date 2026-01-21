package tarmorn.structure.TLearn

import tarmorn.data.IdManager
import tarmorn.data.RelationPath

/**
 * 规则解析器，支持多种规则格式和简写
 * 
 * 支持的规则格式：
 * 1. 简写格式：
 *    /award/award_category/winners./award/award_honor/ceremony <= 
 *    /award/award_category/winners./award/award_honor/ceremony* /award/award_ceremony/awards_presented./award/award_honor/award_winner*INVERSE_/award/award_ceremony/awards_presented./award/award_honor/award_winner
 * 
 * 2. 带括号格式：
 *    /award/award_category/winners./award/award_honor/ceremony(X,Y) <= 
 *    /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)
 * 
 * 3. 支持单变量规则（一元）和双变量规则（二元）
 */
object RuleParser {
    
    /** 调试模式开关 */
    var DEBUG = false
    
    /** 调试输出函数 */
    private fun debug(message: String) {
        if (DEBUG) {
            println("[DEBUG] $message")
        }
    }
    
    /** 判断参数是否是变量（单字母或me_myself_i） */
    fun isVariable(arg: String) = arg.length == 1 || arg == "me_myself_i"
    
    /** 判断实体名称是否包含特殊字符（括号或逗号） */
    private fun hasSpecialChars(entity: String): Boolean {
        return ('(' in entity || ')' in entity || ',' in entity) && !entity.startsWith("/m/")
    }
    
    /** 判断是否为实体占位符 (E开头后跟数字) */
    private fun isEntityPlaceholder(arg: String): Boolean {
        return arg.matches(Regex("E\\d+"))
    }
    
    /**
     * 预处理规则字符串：将括号内包含特殊字符的实体替换为占位符
     * 例如：playsFor(Tom_Kelly_(footballer,born_1964),Y) -> playsFor(E123,Y)
     * 
     * 策略：只处理作为参数出现的实体（在括号内），不处理关系名
     */
    private fun preprocessRule(ruleStr: String): String {
        val result = StringBuilder()
        var i = 0
        
        while (i < ruleStr.length) {
            // 查找关系名后的左括号
            if (ruleStr[i] == '(') {
                result.append('(')
                i++
                
                // 现在我们在参数列表内，解析每个参数
                val argsStart = i
                var parenDepth = 1
                val argsEnd = run {
                    var pos = i
                    while (pos < ruleStr.length && parenDepth > 0) {
                        when (ruleStr[pos]) {
                            '(' -> parenDepth++
                            ')' -> parenDepth--
                        }
                        if (parenDepth > 0) pos++
                    }
                    pos
                }
                
                // 提取参数部分并处理
                val argsString = ruleStr.substring(argsStart, argsEnd)
                val processedArgs = preprocessArguments(argsString)
                result.append(processedArgs)
                
                i = argsEnd
            } else {
                result.append(ruleStr[i])
                i++
            }
        }
        
        return result.toString()
    }
    
    /**
     * 预处理参数列表：将包含特殊字符的实体替换为占位符
     * 只在顶层逗号处分割，尊重嵌套括号
     */
    private fun preprocessArguments(argsString: String): String {
        // 直接按逗号拆分，若出现多于一个逗号，说明某一实体名中包含逗号/括号
        // 一元/二元规则：依据变量位置合并
        var args = argsString.split(',').map { it.trim() }.filter { it.isNotEmpty() }
        if (args.size > 2) {
            args = when {
                isVariable(args.first()) && !isVariable(args[1]) -> {
                    val mergedSecond = args.drop(1).joinToString(",").trim()
                    listOf(args.first().trim(), mergedSecond)
                }
                isVariable(args.last()) -> {
                    val mergedFirst = args.dropLast(1).joinToString(",").trim()
                    listOf(mergedFirst, args.last().trim())
                }
                else -> {
                    val mergedFirst = args.dropLast(1).joinToString(",").trim()
                    listOf(mergedFirst, args.last().trim())
                }
            }
        }
        val processedArgs = args.map { arg ->
            val trimmedArg = arg.trim()
            // 如果参数包含特殊字符且不是变量，替换为占位符
            if (hasSpecialChars(trimmedArg) && !isVariable(trimmedArg)) {
                // 在 IdManager 中注册这个实体并获取ID
                val entityId = IdManager.getEntityId(trimmedArg)
                "E${entityId}"
            } else {
                trimmedArg
            }
        }
        return processedArgs.joinToString(",")
    }
    
    /**
     * 解析规则字符串，返回head和body的DepAtom对
     * 
     * 统一规则格式为简写模式：
     * - 一元规则：/rel(const) <= /rel1*rel2(const2)
     * - 二元规则：/rel <= /rel1*INVERSE_/rel2
     * 
     * @param ruleStr 规则字符串
     * @return Pair<DepAtom, DepAtom?> head和body的原子表示
     */
    fun parseRule(ruleStr: String): Pair<DepAtom, DepAtom?> {
        require(ruleStr.contains("<=")) { "规则格式错误：缺少 '<='" }
        
        debug("原始规则: $ruleStr")
        
        // 预处理：替换包含特殊字符的实体
        var preprocessedRule = preprocessRule(ruleStr)
        // 替换 me_myself_i,Y 或 Y,me_myself_i 或 me_myself_i,X 或 X,me_myself_i 为 X,X
        preprocessedRule = preprocessedRule
            .replace("me_myself_i,Y", "X,X")
            .replace("Y,me_myself_i", "X,X")
            .replace("me_myself_i,X", "X,X")
            .replace("X,me_myself_i", "X,X")

        debug("预处理后: $preprocessedRule")
        
        val (headPart, bodyPart) = preprocessedRule.split("<=", limit = 2).map { it.trim() }
        
        // 对 head/body 分别归一化，保证调用相同函数
        val normHead = normalizeAtomToSimplified(headPart)
        val normBody = normalizeAtomToSimplified(bodyPart)
        debug("规范化后: $normHead <= $normBody")
        
        // 解析head和body为DepAtom
        val headAtom = parseSimplifiedAtom(normHead)
        debug("HeadAtom解析: relationId=${headAtom.relationId}, entityId=${headAtom.entityId}")
        
        val bodyAtom = if (normBody.isNotBlank()) {
            val atom = parseSimplifiedAtom(normBody)
            debug("BodyAtom解析: relationId=${atom.relationId}, entityId=${atom.entityId}")
            atom
        } else null
        
        return Pair(headAtom, bodyAtom)
    }
    
    /**
     * 解析简化格式的原子
     * 格式：
     * 1. relation(constant) - 一元原子，有常量约束
     * 2. relation(*) - 存在性原子，有中间变量但无常量约束
     * 3. relation - 二元原子
     * 4. rel1*rel2*rel3(constant) - 关系路径，有常量约束
     * 5. rel1*rel2*rel3(*) - 关系路径，存在性原子
     * 
     * @return DepAtom
     */
    private fun parseSimplifiedAtom(atomStr: String): DepAtom {
        // 检查是否有括号
        val hasParens = '(' in atomStr && ')' in atomStr
        
        val (relationPath, constantPart) = if (hasParens) {
            val relationPart = atomStr.substringBefore('(').trim()
            val constantPart = atomStr.substringAfter('(').substringBefore(')').trim()
            Pair(relationPart, constantPart)
        } else {
            Pair(atomStr.trim(), null)
        }
        
        // 解析关系路径（可能包含*连接的多个关系）
        val relations = if ('*' in relationPath) {
            relationPath.split('*').map { it.trim() }
        } else {
            listOf(relationPath)
        }
        
        // 获取关系ID
        val relationIds = relations.map { IdManager.getRelationId(it) }
        val finalRelationId = if (relationIds.size == 1) {
            relationIds[0]
        } else {
            RelationPath.encode(relationIds.toLongArray())
        }
        
        // 获取实体ID
        val entityId = when {
            constantPart == null -> IdManager.getYId() // 无括号，二元规则
            constantPart == "*" -> 0 // 存在性原子，有中间变量但无常量
            isEntityPlaceholder(constantPart) -> constantPart.substring(1).toInt() // 占位符，去掉E
            else -> IdManager.getEntityId(constantPart) // 普通实体名
        }
        
        return DepAtom(finalRelationId, entityId)
    }
    

    /**
     * 将完整格式的规则转换为简写格式
     * 
     * 转换规则：
     * 1. 一元规则：rel(X,/m/const) <= body1(X,A), body2(A,/m/const2)
     *    -> rel(/m/const) <= body_path(/m/const2)
     * 2. 二元规则：rel(X,Y) <= body1(X,A), body2(Y,A)
     *    -> rel <= body_path
     */
    fun normalizeAtomToSimplified(atomPart: String): String {
        val atom = atomPart.trim()
        if (atom.isBlank()) return ""

        val atoms = parseBodyAtoms(atom)
        if (atoms.size > 1) {
            return normalizeAtomListToSimplified(atoms)
        }

        // 单个 atom 处理
        if ('(' !in atom || ')' !in atom) {
            return atom
        }

        val parenContent = atom.substringAfter('(').substringBefore(')')
        if (',' !in parenContent) {
            return atom
        }

        val relation = atom.substringBefore('(').trim()
        val args = parenContent.split(',').map { it.trim() }

        val isSelfLoop = args.size == 2 && args[0] == args[1]
        if (isSelfLoop) return "$relation(X)"

        val constant = args.firstOrNull { !isVariable(it) }
        if (constant != null && args.size == 2) {
            val varPos = args.indexOfFirst { isVariable(it) }
            return when (varPos) {
                0 -> "$relation($constant)"
                1 -> "INVERSE_$relation($constant)"
                else -> "$relation($constant)"
            }
        }

        if (args.size == 2) {
            if (args[0] == "X" && args[1] == "Y") {
                return relation
            }
            if (args[0] == "Y" && args[1] == "X") {
                return "INVERSE_$relation"
            }

            val freeVars = args.filter { it == "X" || it == "Y" }
            if (freeVars.size == 1) {
                val freeVar = freeVars[0]
                val inverse = args.indexOf(freeVar) == 1
                val rel = if (inverse) "INVERSE_$relation" else relation
                return "$rel(*)"
            }
        }

        return relation
    }

    private fun normalizeAtomListToSimplified(atoms: List<String>): String {
        val parsedAtoms = atoms.map { atom ->
            mapOf(
                "relation" to extractRelationFromAtom(atom),
                "args" to extractVariables(atom)
            )
        }

        val varCounts = mutableMapOf<String, Int>()
        val constants = mutableListOf<String>()
        parsedAtoms.forEach { atom ->
            val args = atom["args"] as List<String>
            args.forEach { arg ->
                if (isVariable(arg)) {
                    varCounts[arg] = (varCounts[arg] ?: 0) + 1
                } else {
                    constants.add(arg)
                }
            }
        }

        val freeVars = listOf("X", "Y").filter { varCounts.containsKey(it) }
        val fallbackPath = parsedAtoms.joinToString("*") { it["relation"] as String }

        return when {
            freeVars.size >= 2 -> {
                val path = buildRelationPath(parsedAtoms, freeVars[0], freeVars[1]) ?: fallbackPath
                path
            }
            freeVars.size == 1 -> {
                val freeVar = freeVars[0]
                val target = constants.firstOrNull()
                    ?: varCounts.keys.firstOrNull { it != freeVar }
                val path = buildRelationPath(parsedAtoms, freeVar, target) ?: fallbackPath
                if (target != null && !isVariable(target)) "$path($target)" else "$path(*)"
            }
            else -> fallbackPath
        }
    }

    private fun buildRelationPath(
        parsedAtoms: List<Map<String, Any>>,
        start: String?,
        end: String?
    ): String? {
        if (start == null || end == null) return null

        data class Edge(val to: String, val relation: String)

        val graph = mutableMapOf<String, MutableList<Edge>>()
        parsedAtoms.forEach { atom ->
            val relation = atom["relation"] as String
            val args = atom["args"] as List<String>
            if (args.size < 2) return@forEach
            val a0 = args[0]
            val a1 = args[1]
            graph.getOrPut(a0) { mutableListOf() }.add(Edge(a1, relation))
            graph.getOrPut(a1) { mutableListOf() }.add(Edge(a0, "INVERSE_$relation"))
        }

        val visited = mutableSetOf<String>()
        val queue: ArrayDeque<Pair<String, List<String>>> = ArrayDeque()
        queue.add(start to emptyList())
        visited.add(start)

        while (queue.isNotEmpty()) {
            val (node, path) = queue.removeFirst()
            if (node == end) return path.joinToString("*")
            val edges = graph[node].orEmpty()
            edges.forEach { edge ->
                if (edge.to !in visited) {
                    visited.add(edge.to)
                    queue.add(edge.to to (path + edge.relation))
                }
            }
        }

        return null
    }
    
    
    /**
     * 从原子中提取关系名
     */
    fun extractRelationFromAtom(atom: String): String {
        return if ('(' in atom) {
            atom.substringBefore('(').trim()
        } else {
            atom.trim()
        }
    }
    
    /**
     * 从原子中提取变量（包括规范化 me_myself_i）
     * 改进版本：正确处理实体名称中包含括号和逗号的情况
     */
    fun extractVariables(atom: String): List<String> {
        if ('(' !in atom || ')' !in atom) {
            return emptyList()
        }
        
        // 找到最外层的括号对
        val firstParen = atom.indexOf('(')
        val lastParen = atom.lastIndexOf(')')
        
        if (firstParen >= lastParen || firstParen == -1 || lastParen == -1) {
            return emptyList()
        }
        
        val varPart = atom.substring(firstParen + 1, lastParen)
        
        // 使用智能分割：只在括号层级为0时按逗号分割
        val variables = smartSplit(varPart)
        
        // 规范化 me_myself_i
        return variables
    }
    
    /**
     * 智能分割字符串：只在括号层级为0时按逗号分割
     * 这样可以正确处理 "Tom_Kelly_(footballer,_born_1964),Y" 这样的字符串
     */
    private fun smartSplit(text: String): List<String> {
        val result = mutableListOf<String>()
        var current = StringBuilder()
        var parenDepth = 0
        
        for (char in text) {
            when (char) {
                '(' -> {
                    parenDepth++
                    current.append(char)
                }
                ')' -> {
                    parenDepth--
                    current.append(char)
                }
                ',' -> {
                    if (parenDepth == 0) {
                        // 只在括号外的逗号处分割
                        result.add(current.toString().trim())
                        current = StringBuilder()
                    } else {
                        // 括号内的逗号保留
                        current.append(char)
                    }
                }
                else -> current.append(char)
            }
        }
        
        // 添加最后一个部分
        if (current.isNotEmpty()) {
            result.add(current.toString().trim())
        }
        
        return result
    }
    
    /**
     * 解析身体部分的原子列表
     * 由于预处理已替换特殊字符，可以安全地使用智能分割
     */
    fun parseBodyAtoms(bodyPart: String): List<String> {
        // 使用智能分割，只在括号层级为0时按逗号分割
        return smartSplit(bodyPart).filter { it.isNotBlank() }
    }
}