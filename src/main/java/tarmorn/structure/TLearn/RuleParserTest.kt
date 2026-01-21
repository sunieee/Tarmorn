package tarmorn.structure.TLearn

import tarmorn.data.IdManager

/**
 * RuleParser测试程序
 * 测试RuleParser解析各种规则格式
 */
object RuleParserTest {
    
    /**
     * 测试单条规则并输出详细信息
     */
    fun testRule(ruleStr: String) {
        println("=" .repeat(80))
        println("原始规则: $ruleStr")
        println("-".repeat(80))
        
        try {
            // 解析规则
            val (headAtom, bodyAtom) = RuleParser.parseRule(ruleStr)
            
            // 输出HeadAtom信息
            println("HeadAtom:")
            println("  - toString: $headAtom")
            println("  - toRuleString: ${headAtom.getRuleString()}")
            println("  - relationId: ${headAtom.relationId}")
            println("  - entityId: ${headAtom.entityId}")
            println("  - isBinary: ${headAtom.isBinary}")
            println("  - isL1Atom: ${headAtom.isL1Atom}")
            
            // 输出BodyAtom信息
            if (bodyAtom != null) {
                println("\nBodyAtom:")
                println("  - toString: $bodyAtom")
                println("  - toRuleString: ${bodyAtom.getRuleString()}")
                println("  - relationId: ${bodyAtom.relationId}")
                println("  - entityId: ${bodyAtom.entityId}")
                println("  - isBinary: ${bodyAtom.isBinary}")
                println("  - isL1Atom: ${bodyAtom.isL1Atom}")
            } else {
                println("\nBodyAtom: null")
            }

            var isVariableY = !headAtom.isBinary && headAtom.isInverseRelation
            if (bodyAtom?.entityId == IdManager.getXId()) isVariableY = false
            val currentString = if (bodyAtom != null)  "${headAtom.getRuleString(isVariableY)} <= ${bodyAtom.getRuleString(isVariableY)}"
            else "${headAtom.getRuleString(isVariableY)} <= "
            // require(currentString == ruleString) {
            //     "Parsed rule does not match original string: $ruleString"
            // }
            if (ruleStr != currentString) {
                println("[parseAndAddRule] Warning: Parsed rule does not match original string:")
                println("  Original: $ruleStr")
                println("  Parsed:   $currentString")
            }
            
        } catch (e: Exception) {
            println("ERROR: ${e.message}")
            e.printStackTrace()
        }
        println()
    }
    
    /**
     * 初始化测试环境（注册必要的实体和关系）
     */
    fun initializeTestEnvironment() {
        println("初始化测试环境...")
        
        // 注册测试用的关系
        val testRelations = listOf(
            "P37", "P530", "P19", "P27", "P551",
            "playsFor", "isAffiliatedTo", "isConnectedTo",
            "/award/award_category/winners./award/award_honor/ceremony",
            "/award/award_category/winners./award/award_honor/award_winner",
            "/award/award_ceremony/awards_presented./award/award_honor/award_winner",
            "/award/award_category/nominees./award/award_nomination/nominated_for",
            "/award/award_nominee/award_nominations./award/award_nomination/award",
            "/award/award_ceremony/awards_presented./award/award_honor/award_winner",
            "/film/film/release_date_s./film/film_regional_release_date/film_release_region",
            "/music/genre/artists",
            "/music/performance_role/regular_performances./music/group_membership/group",
            "/education/university/domestic_tuition./measurement_unit/dated_money_value/currency",
            "/education/university/local_tuition./measurement_unit/dated_money_value/currency",
            "/time/event/instance_of_recurring_event",
            "/award/award_category/category_of",
            "/award/award_winner/awards_won./award/award_honor/award_winner",
            "/education/educational_institution_campus/educational_institution",
            "/film/film/country",
            "/media_common/netflix_genre/titles",
            "/location/location/contains",
            "/location/hud_county_place/place"
        )
        
        testRelations.forEach { relation ->
            IdManager.getRelationId(relation)
        }
        IdManager.addInverseRelations()
        
        // 注册测试用的实体
        val testEntities = listOf(
            "/m/01xqqp", "/m/0257w4", "/m/02cg41", "/m/05pd94v", "/m/0m2l9",
            "/m/0gs9p", "/m/0j8f09z", "/m/0gs96", "/m/02r79_h", "/m/0f4x7",
            "/m/02_fj", "/m/01ck6v", "/m/07z31v", "/m/0b90_r", "/m/07ylj",
            "/m/06by7", "/m/02gsvk", "/m/0l2vz",
            "Tom_Kelly_(footballer,born_1964)", "Shaun_Taylor",
            "Scott_Brown(footballer,born_May_1985)", "Danny_Welbeck",
            "Evenes", "Trondheim_Airport,_Værnes",
        )
        
        testEntities.forEach { entity ->
            IdManager.getEntityId(entity)
        }
        
        println("测试环境初始化完成\n")
    }
    
    @JvmStatic
    fun main(args: Array<String>) {
        // 启用调试模式
        RuleParser.DEBUG = true
        
        // 初始化测试环境
        initializeTestEnvironment()
        
        // 测试规则列表
        val testRules = listOf(
            // 来自 analysis_rule.py 的样例
            "/award/award_category/winners./award/award_honor/ceremony(X,/m/01xqqp) <= /award/award_category/winners./award/award_honor/ceremony(X,A), /award/award_category/winners./award/award_honor/ceremony(/m/0257w4,A)",
            
            "/award/award_category/winners./award/award_honor/ceremony(X,/m/02cg41) <= /award/award_category/winners./award/award_honor/ceremony(X,/m/05pd94v)",
            
            "/award/award_category/winners./award/award_honor/ceremony(X,/m/05pd94v) <= /award/award_category/winners./award/award_honor/ceremony(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(A,/m/0m2l9)",
            
            "/award/award_category/winners./award/award_honor/ceremony(/m/05pd94v) <= /award/award_category/winners./award/award_honor/ceremony*/award/award_ceremony/awards_presented./award/award_honor/award_winner(/m/0m2l9)",
            
            "INVERSE_/award/award_category/winners./award/award_honor/ceremony(/m/0gs9p) <= INVERSE_/award/award_category/winners./award/award_honor/ceremony*/award/award_category/nominees./award/award_nomination/nominated_for(/m/0j8f09z)",
            
            "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)",
            
            "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/ceremony(X,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(A,B), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,B)",
            
            "/award/award_category/winners./award/award_honor/ceremony <= /award/award_category/winners./award/award_honor/ceremony*/award/award_ceremony/awards_presented./award/award_honor/award_winner*INVERSE_/award/award_ceremony/awards_presented./award/award_honor/award_winner",
            
            "/award/award_category/winners./award/award_honor/ceremony(/m/0gs96,X) <= /award/award_category/winners./award/award_honor/ceremony(A,X), /award/award_category/nominees./award/award_nomination/nominated_for(A,/m/02r79_h)",
            
            "/award/award_category/winners./award/award_honor/ceremony(/m/0f4x7,X) <= /award/award_category/winners./award/award_honor/ceremony(A,X), /award/award_nominee/award_nominations./award/award_nomination/award(/m/02_fj,A)",
            
            "/award/award_category/winners./award/award_honor/ceremony(/m/01ck6v,Y) <= /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,A)",
            
            "/award/award_category/winners./award/award_honor/ceremony(X,/m/07z31v) <= /award/award_nominee/award_nominations./award/award_nomination/award(A,X)",
            
            "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/category_of(X,A), /time/event/instance_of_recurring_event(Y,A)",
            
            "/award/award_category/winners./award/award_honor/ceremony(X,Y) <= /award/award_category/winners./award/award_honor/award_winner(X,A), /award/award_winner/awards_won./award/award_honor/award_winner(B,A), /award/award_ceremony/awards_presented./award/award_honor/award_winner(Y,B)",
            
            "/film/film/release_date_s./film/film_regional_release_date/film_release_region(X,/m/0b90_r) <= /film/film/release_date_s./film/film_regional_release_date/film_release_region(X,/m/07ylj)",
            
            "INVERSE_/music/genre/artists(/m/06by7) <= INVERSE_/music/performance_role/regular_performances./music/group_membership/group(*)",
            
            "/education/university/domestic_tuition./measurement_unit/dated_money_value/currency <= /education/university/local_tuition./measurement_unit/dated_money_value/currency * INVERSE_/education/university/local_tuition./measurement_unit/dated_money_value/currency * /education/university/local_tuition./measurement_unit/dated_money_value/currency",
            
            // 用户提供的新样例
            "P37(X,Y) <= P530(A,X), P530(A,B), P37(B,Y)",
            
            "P19(X,Y) <= P27(X,A), P19(B,A), P551(B,Y)",
            
            "playsFor(Tom_Kelly_(footballer,born_1964),Y) <= isAffiliatedTo(Shaun_Taylor,Y)",
            
            "isAffiliatedTo(Scott_Brown(footballer,born_May_1985),Y) <= playsFor(Danny_Welbeck,Y)",

            "/education/educational_institution_campus/educational_institution(me_myself_i,Y) <= /education/university/domestic_tuition./measurement_unit/dated_money_value/currency(Y,/m/02gsvk)",
            "/education/educational_institution_campus/educational_institution(X,me_myself_i) <= /education/university/domestic_tuition./measurement_unit/dated_money_value/currency(X,/m/02gsvk)",
            "/award/award_category/category_of(X,me_myself_i) <= /award/award_category/category_of(me_myself_i,X)",
            "/award/award_category/category_of(me_myself_i,Y) <= /award/award_category/category_of(Y,me_myself_i)",

            "/film/film/country(X,Y) <= /media_common/netflix_genre/titles(Y,X)",
            "/location/location/contains(/m/0l2vz,Y) <= /location/hud_county_place/place(me_myself_i,Y)",
            "isConnectedTo(X,Evenes) <= isConnectedTo(Trondheim_Airport,_Værnes,X)",
            "isConnectedTo(Trondheim_Airport,_Værnes,X) <= isConnectedTo(X,Evenes)"
        )
        
        // 测试每条规则
        println("开始测试规则解析...\n")
        testRules.forEachIndexed { index, rule ->
            println("\n测试规则 ${index + 1}/${testRules.size}")
            testRule(rule)
        }
        
        println("\n测试完成！")
    }
}