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
    
    /** 判断参数是否是变量（单字母或me_myself_i） */
    fun isVariable(arg: String) = arg.length == 1 || arg == "me_myself_i"
    
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
     * - 一元规则：/rel(/m/const) <= /rel1* /rel2(/m/const2)
     * - 二元规则：/rel <= /rel1*INVERSE_/rel2
     * 
     * @param ruleStr 规则字符串
     * @return Pair<DepAtom, DepAtom?> head和body的原子表示
     */
    fun parseRule(ruleStr: String): Pair<DepAtom, DepAtom?> {
        require(ruleStr.contains("<=")) { "规则格式错误：缺少 '<='" }
        
        val (headPart, bodyPart) = ruleStr.split("<=", limit = 2).map { it.trim() }
        
        // 转换为简写模式
        val normalizedRule = normalizeToSimplified(headPart, bodyPart)
        val (normHead, normBody) = normalizedRule.split("<=", limit = 2).map { it.trim() }
        
        // 构造head的DepAtom
        val (headRelation, headEntity, headVarPos) = parseSimplifiedHead(normHead)
        val headAtom = createDepAtom(listOf(headRelation), headEntity, headVarPos)
        
        // 构造body的DepAtom
        val bodyAtom = if (normBody.isNotBlank()) {
            val (bodyRelations, bodyConstant) = parseSimplifiedBody(normBody)
            val adjustedConstant = if (normBody.contains("(*)")) null else bodyConstant
            createDepAtom(bodyRelations, adjustedConstant)
        } else null
        
        return Pair(headAtom, bodyAtom)
    }
    
    /**
     * 从关系字符串列表和常量创建DepAtom
     * @param relations 关系名称列表（可能包含INVERSE_前缀）
     * @param constant 常量实体，null表示无常量
     * @param varPos 变量位置标记："self_loop"表示自环，null表示二元规则或其他
     */
    private fun createDepAtom(relations: List<String>, constant: String?, varPos: String? = null): DepAtom {
        if (relations.isEmpty()) {
            // 空关系，使用特殊表示
            return DepAtom(0L, 0)
        }
        
        // 处理关系路径（IdManager已经在加载数据时创建了INVERSE关系的ID）
        val relationIds = relations.map { IdManager.getRelationId(it) }
        
        // 如果是多个关系，需要编码为关系路径
        val finalRelationId = if (relationIds.size == 1) {
            relationIds[0]
        } else {
            RelationPath.encode(relationIds.toLongArray())
        }
        
        // 确定entityId
        val entityId = when {
            varPos == "self_loop" -> IdManager.getXId() // 自环
            constant != null -> IdManager.getEntityId(constant) // 带常量约束
            else -> IdManager.getYId() // 二元规则或自由变量
        }
        
        return DepAtom(finalRelationId, entityId)
    }
    
    /**
     * 解析简写格式的body部分
     */
    private fun parseSimplifiedBody(bodyPart: String): Pair<List<String>, String?> {
        var bodyConstant: String? = null
        var processedBodyPart = bodyPart
        
        if (bodyPart.isBlank()) return Pair(emptyList(), null)
        
        // 检查是否有括号约束
        if ('(' in bodyPart && ')' in bodyPart) {
            val lastParenStart = bodyPart.lastIndexOf('(')
            val lastParenEnd = bodyPart.lastIndexOf(')')
            
            if (lastParenStart < lastParenEnd) {
                val entityPart = bodyPart.substring(lastParenStart + 1, lastParenEnd).trim()
                when {
                    entityPart == "*" -> processedBodyPart = bodyPart.substring(0, lastParenStart).trim()
                    entityPart.startsWith("/m/") -> {
                        bodyConstant = entityPart
                        processedBodyPart = bodyPart.substring(0, lastParenStart).trim()
                    }
                }
            }
        }
        
        // 解析关系路径
        val bodyRelations = if ('*' in processedBodyPart) {
            processedBodyPart.split('*').map { it.trim() }
        } else {
            listOf(processedBodyPart.trim())
        }
        
        return Pair(bodyRelations, bodyConstant)
    }
    
    /**
     * 解析简写格式的头部
     */
    private fun parseSimplifiedHead(headPart: String): Triple<String, String?, String?> {
        if ('(' in headPart && ')' in headPart) {
            val relation = headPart.substringBefore('(').trim()
            val entityPart = headPart.substringAfter('(').substringBefore(')').trim()
            
            return when {
                isVariable(entityPart) -> {
                    val normalizedVar = if (entityPart == "me_myself_i") "X" else entityPart
                    Triple(relation, normalizedVar, "self_loop")
                }
                entityPart.startsWith("/m/") -> {
                    Triple(relation, entityPart, if (relation.startsWith("INVERSE_")) "head" else "tail")
                }
                else -> Triple(relation, null, null)
            }
        } else {
            return Triple(headPart.trim(), null, null)
        }
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
     */
    fun extractVariables(atom: String): List<String> {
        if ('(' !in atom || ')' !in atom) {
            return emptyList()
        }
        
        val varPart = atom.substringAfter('(').substringBefore(')')
        var variables = varPart.split(',').map { it.trim() }
        // 规范化 me_myself_i
        variables = normalizeMeMyselfI(variables, "extracted")
        return variables
    }
    
    /**
     * 解析身体部分的原子列表
     */
    fun parseBodyAtoms(bodyPart: String): List<String> {
        val atoms = mutableListOf<String>()
        var currentAtom = StringBuilder()
        var parenCount = 0
        
        for (char in bodyPart) {
            when (char) {
                '(' -> parenCount++
                ')' -> parenCount--
                ',' -> {
                    if (parenCount == 0) {
                        atoms.add(currentAtom.toString().trim())
                        currentAtom = StringBuilder()
                        continue
                    }
                }
            }
            currentAtom.append(char)
        }
        
        if (currentAtom.isNotBlank()) {
            atoms.add(currentAtom.toString().trim())
        }
        
        return atoms
    }
}