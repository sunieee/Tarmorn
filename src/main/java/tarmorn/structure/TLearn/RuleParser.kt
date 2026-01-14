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
        val args = smartSplit(argsString)
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
    
    
    /** 规范化me_myself_i为实际变量名 */
    fun normalizeMeMyselfI(args: List<String>, context: String = "head"): List<String> {
        if ("me_myself_i" !in args) return args
        
        val firstVar = args.firstOrNull { isVariable(it) && it != "me_myself_i" } ?: "X"
        return args.map { if (it == "me_myself_i") firstVar else it }
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
        val preprocessedRule = preprocessRule(ruleStr)
        debug("预处理后: $preprocessedRule")
        
        val (headPart, bodyPart) = preprocessedRule.split("<=", limit = 2).map { it.trim() }
        
        // 转换为简写模式
        val normalizedRule = normalizeToSimplified(headPart, bodyPart)
        debug("规范化后: $normalizedRule")
        
        val (normHead, normBody) = normalizedRule.split("<=", limit = 2).map { it.trim() }
        
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
     * 2. relation(*) - 一元原子，无常量约束
     * 3. relation - 二元原子
     * 4. rel1*rel2*rel3(constant) - 关系路径，有常量约束
     * 5. rel1*rel2*rel3 - 关系路径，无常量约束
     * 
     * @return DepAtom
     */
    private fun parseSimplifiedAtom(atomStr: String): DepAtom {
        // 检查是否有括号
        val hasParens = '(' in atomStr && ')' in atomStr
        
        val (relationPath, constant) = if (hasParens) {
            val relationPart = atomStr.substringBefore('(').trim()
            val constantPart = atomStr.substringAfter('(').substringBefore(')').trim()
            val actualConstant = if (constantPart == "*") null else constantPart
            Pair(relationPart, actualConstant)
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
            constant == null -> IdManager.getYId() // 无常量，二元规则
            isEntityPlaceholder(constant) -> constant.substring(1).toInt() // 占位符，去掉E
            else -> IdManager.getEntityId(constant) // 普通实体名
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
    fun normalizeToSimplified(headPart: String, bodyPart: String): String {
        // 检查是否已经是简写格式
        if ('(' !in headPart || ')' !in headPart) {
            return "$headPart <= $bodyPart"
        }
        
        val parenContent = headPart.substringAfter('(').substringBefore(')')
        if (',' !in parenContent) {
            return "$headPart <= $bodyPart"
        }
        
        // 解析完整格式
        val headRelation = headPart.substringBefore('(').trim()
        // 由于特殊字符已在预处理中被替换，现在可以安全地使用简单分割
        val headArgs = normalizeMeMyselfI(parenContent.split(',').map { it.trim() }, "head")
        val bodyAtoms = parseBodyAtoms(bodyPart)
        
        // 判断规则类型
        val isSelfLoop = headArgs.size == 2 && headArgs[0].length == 1 && headArgs[0] == headArgs[1]
        val freeVarsCount = headArgs.filter { isVariable(it) }.toSet().size
        
        return when {
            freeVarsCount == 1 || isSelfLoop -> convertUnaryToSimplified(headRelation, headArgs, bodyAtoms, isSelfLoop)
            else -> convertBinaryToSimplified(headRelation, headArgs, bodyAtoms)
        }
    }
    
    /** 将一元规则转换为简写格式 */
    fun convertUnaryToSimplified(
        headRelation: String, 
        headArgs: List<String>, 
        bodyAtoms: List<String>, 
        isSelfLoop: Boolean = false
    ): String {
        val freeVar = headArgs.firstOrNull { isVariable(it) } ?: "X"
        val (bodyPath, bodyConstant) = buildUnaryBodyPath(bodyAtoms, freeVar)
        
        val simplifiedHead = when {
            isSelfLoop -> "$headRelation(X)"
            else -> {
                val headConstant = headArgs.firstOrNull { !isVariable(it) }
                val varPos = headArgs.indexOfFirst { isVariable(it) }
                when {
                    headConstant != null && varPos == 0 -> "$headRelation($headConstant)"
                    headConstant != null -> "INVERSE_$headRelation($headConstant)"
                    else -> "$headRelation(X)"
                }
            }
        }
        
        val simplifiedBody = formatUnaryBody(bodyPath, bodyConstant, bodyAtoms)
        return "$simplifiedHead <= $simplifiedBody"
    }
    
    /** 格式化一元规则的body部分 */
    private fun formatUnaryBody(bodyPath: String, bodyConstant: String?, bodyAtoms: List<String>): String {
        return when {
            bodyConstant != null -> "$bodyPath($bodyConstant)"
            hasIntermediateVars(bodyAtoms) -> "$bodyPath(*)"
            else -> bodyPath
        }
    }
    
    /** 检查是否有中间变量 */
    private fun hasIntermediateVars(bodyAtoms: List<String>): Boolean {
        if (bodyAtoms.size > 1) return true
        if (bodyAtoms.isEmpty()) return false
        val args = normalizeMeMyselfI(extractVariables(bodyAtoms[0]), "body")
        return args.size == 2 && args.all { isVariable(it) }
    }
    
    /** 构建一元规则的body路径 */
    fun buildUnaryBodyPath(bodyAtoms: List<String>, freeVar: String): Pair<String, String?> {
        if (bodyAtoms.isEmpty()) return Pair("", null)
        
        val parsedAtoms = bodyAtoms.map { atom ->
            val relation = extractRelationFromAtom(atom)
            val args = extractVariables(atom)
            mapOf("relation" to relation, "args" to args)
        }
        
        val bodyConstant = parsedAtoms.flatMap { it["args"] as List<String> }
            .firstOrNull { it.length > 1 }
        
        if (parsedAtoms.size == 1) {
            val args = parsedAtoms[0]["args"] as List<String>
            val relation = parsedAtoms[0]["relation"] as String
            val inversePrefix = if (args.indexOf(freeVar) == 0) "" else "INVERSE_"
            return Pair("$inversePrefix$relation", bodyConstant)
        }
        
        return analyzeUnaryConnection(parsedAtoms, freeVar, bodyConstant)
    }
    
    /** 分析一元规则中多个原子的连接方式 */
    @Suppress("UNCHECKED_CAST")
    private fun analyzeUnaryConnection(
        parsedAtoms: List<Map<String, Any>>, 
        freeVar: String, 
        bodyConstant: String?
    ): Pair<String, String?> {
        if (parsedAtoms.size <= 1) {
            return Pair(parsedAtoms[0]["relation"] as String, bodyConstant)
        }
        
        val startIdx = parsedAtoms.indexOfFirst { freeVar in (it["args"] as List<String>) }.takeIf { it >= 0 } ?: 0
        val pathRelations = mutableListOf<String>()
        val usedAtoms = mutableSetOf(startIdx)
        
        var currentAtom = parsedAtoms[startIdx]
        val args = currentAtom["args"] as List<String>
        val freeVarPos = args.indexOf(freeVar)
        
        var currentVar = if (freeVarPos == 0) {
            pathRelations.add(currentAtom["relation"] as String)
            args[1]
        } else {
            pathRelations.add("INVERSE_${currentAtom["relation"]}")
            args[0]
        }
        
        while (usedAtoms.size < parsedAtoms.size) {
            val nextAtom = parsedAtoms.withIndex()
                .firstOrNull { (i, atom) -> i !in usedAtoms && currentVar in (atom["args"] as List<String>) }
                ?: break
                
            val (i, atom) = nextAtom
            usedAtoms.add(i)
            
            val atomArgs = atom["args"] as List<String>
            val varPos = atomArgs.indexOf(currentVar)
            
            currentVar = if (varPos == 0) {
                pathRelations.add(atom["relation"] as String)
                atomArgs.getOrElse(1) { "" }
            } else {
                pathRelations.add("INVERSE_${atom["relation"]}")
                atomArgs[0]
            }
        }
        
        return Pair(pathRelations.joinToString("*"), bodyConstant)
    }
    
    /** 将二元规则转换为简写格式 */
    fun convertBinaryToSimplified(
        headRelation: String, 
        headArgs: List<String>, 
        bodyAtoms: List<String>
    ): String {
        val normalizedHeadArgs = normalizeMeMyselfI(headArgs, "head")
        val freeVars = normalizedHeadArgs.filter { isVariable(it) }
        require(freeVars.size == 2) { "二元规则必须有两个自由变量，当前有 ${freeVars.size} 个" }
        
        val bodyPath = buildBinaryBodyPath(bodyAtoms, freeVars)
        return "$headRelation <= $bodyPath"
    }
    
    /** 构建二元规则的body路径 */
    @Suppress("UNCHECKED_CAST")
    fun buildBinaryBodyPath(bodyAtoms: List<String>, freeVars: List<String>): String {
        if (bodyAtoms.isEmpty()) return ""
        
        val parsedAtoms = bodyAtoms.map { atom ->
            mapOf("relation" to extractRelationFromAtom(atom), "args" to extractVariables(atom))
        }
        
        if (parsedAtoms.size == 1) {
            return buildSingleAtomPath(parsedAtoms[0], freeVars)
        }
        
        require(freeVars.size == 2) { "二元规则需要两个自由变量" }
        return buildMultiAtomPath(parsedAtoms, freeVars)
    }
    
    /** 构建单原子的路径 */
    @Suppress("UNCHECKED_CAST")
    private fun buildSingleAtomPath(atom: Map<String, Any>, freeVars: List<String>): String {
        val relation = atom["relation"] as String
        if (freeVars.size != 2) return relation
        
        val args = atom["args"] as List<String>
        val (X, Y) = freeVars
        return when {
            args[0] == X && args[1] == Y -> relation
            args[0] == Y && args[1] == X -> "INVERSE_$relation"
            else -> relation
        }
    }
    
    /** 构建多原子的连接路径 */
    @Suppress("UNCHECKED_CAST")
    private fun buildMultiAtomPath(parsedAtoms: List<Map<String, Any>>, freeVars: List<String>): String {
        val (X, Y) = freeVars
        val startIdx = parsedAtoms.indexOfFirst { X in (it["args"] as List<String>) }
            .takeIf { it >= 0 } ?: return parsedAtoms.joinToString("*") { it["relation"] as String }
        
        val pathRelations = mutableListOf<String>()
        val usedAtoms = mutableSetOf(startIdx)
        
        var currentAtom = parsedAtoms[startIdx]
        val args = currentAtom["args"] as List<String>
        val xPos = args.indexOf(X)
        
        var currentVar = if (xPos == 0) {
            pathRelations.add(currentAtom["relation"] as String)
            args[1]
        } else {
            pathRelations.add("INVERSE_${currentAtom["relation"]}")
            args[0]
        }
        
        while (currentVar != Y && usedAtoms.size < parsedAtoms.size) {
            val nextAtom = parsedAtoms.withIndex()
                .firstOrNull { (i, atom) -> i !in usedAtoms && currentVar in (atom["args"] as List<String>) }
                ?: break
            
            val (i, atom) = nextAtom
            usedAtoms.add(i)
            
            val atomArgs = atom["args"] as List<String>
            val varPos = atomArgs.indexOf(currentVar)
            
            currentVar = if (varPos == 0) {
                pathRelations.add(atom["relation"] as String)
                atomArgs[1]
            } else {
                pathRelations.add("INVERSE_${atom["relation"]}")
                atomArgs[0]
            }
        }
        
        return pathRelations.joinToString("*")
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
        return normalizeMeMyselfI(variables, "extracted")
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