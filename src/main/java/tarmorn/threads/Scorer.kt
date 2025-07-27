package tarmorn.threads

import tarmorn.Learn.active
import tarmorn.Learn.areAllThere
import tarmorn.Learn.heyYouImHere
import tarmorn.Learn.isStored
import tarmorn.Learn.storeRule
import tarmorn.Settings
import tarmorn.data.IdManager
import tarmorn.data.RelationPath
import tarmorn.data.Triple
import tarmorn.data.TripleSet
import tarmorn.structure.*
import kotlin.math.pow
import kotlin.random.Random


/**
 * The worker thread responsible for learning rules in the reinforced learning setting.
 */
class Scorer(val triples: TripleSet, val id: Int) : Thread() {
    private val rand = Random.Default

    // private int entailedCounter = 1;
    private var createdRules = 0
    private var storedRules = 0
    private var producedScore = 0.0

    // Unified rule type representation using Int instead of multiple booleans
    private var ruleType = 1

    private var ready = false

    private var onlyXY = false


    /**
     * Set the search parameters using unified rule type representation
     * @param ruleType The unified rule type (see Dice.RULE_TYPE_* constants)
     */
    fun setSearchParameters(ruleType: Int) {
        require(Dice.isValidRuleType(ruleType)) { "Invalid rule type: $ruleType" }
        this.ruleType = ruleType
        this.ready = true
        this.onlyXY = false
        
        // Validate rule type constraints
        if (ruleType >=1 && ruleType <= 10) {
            val length = ruleType % 10
            if (length > Settings.MAX_LENGTH_GROUNDED_CYCLIC) {
                this.onlyXY = true
            }
        }
    }

    override fun run() {
        while (!areAllThere()) {
            heyYouImHere(this.id)
            try {
                sleep(20)
            } catch (e: InterruptedException) {
                // TODO Auto-generated catch block
                e.printStackTrace()
            }
            // println("THREAD-" + this.id + " waiting for the others");
        }
        println("THREAD-" + this.id + " starts to work with type=" + Dice.getRuleTypeName(ruleType) + " ")


        // outer loop is missing
        val done = false
        while (done == false) {
            if (!active(
                    this.id,
                    this.storedRules,
                    this.createdRules,
                    this.producedScore,
                    ruleType,
                ) || !ready
            ) {
                this.createdRules = 0
                this.storedRules = 0
                this.producedScore = 0.0
                try {
                    sleep(10)
                } catch (e: InterruptedException) {
                    e.printStackTrace()
                }
            } else {
                val start = System.currentTimeMillis()
                
                // Determine rule mining parameters from unified type
                val isZero = ruleType==0
                val isCyclic = ruleType>=1 && ruleType<=10
                val isAcyclic = ruleType>10
                val length = ruleType % 10
                
                // search for zero rules
                if (isZero) {
                    val path = samplePath(length + 1, false)

                    // println("zero (sample with steps=" + (this.mineParamLength+1) + "):" + path);
                    if (path != null) {
                        val learnedRules = getGeneralizations(path, false)
                        println("Scorer-${this.id}: Generated ${learnedRules.size} zero rule generalizations")
                        if (!active) {
                            try {
                                sleep(10)
                            } catch (e: InterruptedException) {
                                e.printStackTrace()
                            }
                        } else {
                            for (learnedRule in learnedRules) {
                                this.createdRules++
                                println("Scorer-${this.id}: Processing zero rule: $learnedRule")
                                if (learnedRule.isTrivial) {
                                    println("  -> Rule is trivial, skipping")
                                    continue
                                }
                                if (isStored(learnedRule)) {
                                    println("  -> Rule is new, processing")
                                    
                                    println("  -> Computing scores for rule")
                                    //long t1 = System.currentTimeMillis();
                                    learnedRule.computeScores(this.triples)
                                    println("  -> Scores computed: confidence=${learnedRule.confidence}, correctlyPredicted=${learnedRule.correctlyPredicted}")

                                    //long t2 = System.currentTimeMillis();
                                    //if (t2 - t1 > 500) {
                                    //	println("* elapsed: " + (t2 - t1) + " >>> " + learnedRule);
                                    //}
                                    
                                    if (learnedRule.confidence >= Settings.THRESHOLD_CONFIDENCE && learnedRule.correctlyPredicted >= Settings.THRESHOLD_CORRECT_PREDICTIONS && (learnedRule !is RuleZero || learnedRule.correctlyPredicted > Settings.THRESHOLD_CORRECT_PREDICTIONS_ZERO)) {
                                        if (active) {
                                            println("  -> Rule passed thresholds, storing")
                                            storeRule(learnedRule)
                                            // println(">>> " +  learnedRule);
                                            this.producedScore += getScoringGain(
                                                learnedRule,
                                                learnedRule.correctlyPredicted,
                                                learnedRule.confidence,
                                                learnedRule.appliedConfidence
                                            )
                                            this.storedRules++
                                        }
                                    } else {
                                        println("  -> Rule failed thresholds: conf=${learnedRule.confidence}>=${Settings.THRESHOLD_CONFIDENCE}? pred=${learnedRule.correctlyPredicted}>=${Settings.THRESHOLD_CORRECT_PREDICTIONS}?")
                                    }
                                } else {
                                    println("  -> Rule already exists, skipping")
                                }
                            }
                        }
                    }


                    /*
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					*/
                }


                // search for cyclic rules
                if (isCyclic) {
                    val path = samplePath(length + 1, true)
                    if (path != null && path.isValid) {
                        // println(path);
                        val learnedRules = getGeneralizations(path, this.onlyXY)
                        println("Scorer-${this.id}: Generated ${learnedRules.size} cyclic rule generalizations")
                        // println(learnedRules.size());
                        if (!active) {
                            try {
                                sleep(10)
                            } catch (e: InterruptedException) {
                                e.printStackTrace()
                            }
                        } else {
                            for (learnedRule in learnedRules) {
                                this.createdRules++
                                if (learnedRule.isTrivial) continue
                                // if (learnedRule.isRedundantACRule(triples)) continue;
                                // long l2;
                                // long l1 = System.currentTimeMillis();
                                if (isStored(learnedRule)) {
                                    //long t1 = System.currentTimeMillis();
                                    learnedRule.computeScores(this.triples)

                                    //long t2 = System.currentTimeMillis();
                                    //if (t2 - t1 > 500) {
                                    //	println("* elapsed: " + (t2 - t1) + " >>> " + learnedRule);
                                    //}
                                    if (learnedRule.confidence >= Settings.THRESHOLD_CONFIDENCE && learnedRule.correctlyPredicted >= Settings.THRESHOLD_CORRECT_PREDICTIONS && (learnedRule !is RuleZero || learnedRule.correctlyPredicted > Settings.THRESHOLD_CORRECT_PREDICTIONS_ZERO)) {
                                        if (active) {
                                            storeRule(learnedRule)

                                            // this.producedScore += getScoringGain(learnedRule.getCorrectlyPredictedMax(), learnedRule.getConfidenceMax());
                                            this.producedScore += getScoringGain(
                                                learnedRule,
                                                learnedRule.correctlyPredicted,
                                                learnedRule.confidence,
                                                learnedRule.appliedConfidence
                                            )
                                            this.storedRules++
                                        }
                                    }
                                } else {
                                    // l2 = System.currentTimeMillis();
                                }


                                // if (l2 - l1 > 100) println("uppps");
                            }
                        }
                    }
                }
                // search for acyclic rules
                if (isAcyclic) {
                    val path = samplePath(length + 1, false)
                    if (path != null && path.isValid) {
                        val learnedRules = getGeneralizations(path, false)
                        println("Scorer-${this.id}: Generated ${learnedRules.size} acyclic rule generalizations")
                        if (!active) {
                            try {
                                sleep(10)
                            } catch (e: InterruptedException) {
                                e.printStackTrace()
                            }
                        } else {
                            for (learnedRule in learnedRules) {
                                this.createdRules++
                                if (learnedRule.isTrivial) continue
                                // long l2;
                                //long l1 = System.currentTimeMillis();
                                if (isStored(learnedRule)) {
                                    // l2 = System.currentTimeMillis();

                                    //long t1 = System.currentTimeMillis();
                                    learnedRule.computeScores(this.triples)


                                    //long t2 = System.currentTimeMillis();
                                    //if (t2 - t1 > 500) {
                                    //	println("* elapsed: " + (t2 - t1) + " >>> " + learnedRule);
                                    //}
                                    if (learnedRule.confidence >= Settings.THRESHOLD_CONFIDENCE && learnedRule.correctlyPredicted >= Settings.THRESHOLD_CORRECT_PREDICTIONS && (learnedRule !is RuleZero || learnedRule.correctlyPredicted > Settings.THRESHOLD_CORRECT_PREDICTIONS_ZERO)) {
                                        if (active) {
                                            storeRule(learnedRule)
                                            this.producedScore += getScoringGain(
                                                learnedRule,
                                                learnedRule.correctlyPredicted,
                                                learnedRule.confidence,
                                                learnedRule.appliedConfidence
                                            )
                                            this.storedRules++
                                        }
                                    }
                                } else {
                                }
                                // if (l2 - l1 > 200) println("uppps");
                            }
                        }
                    }
                }
            }
        }
    }
    /**
     * Sample a path from the triple set (simplified with inverse relations)
     * No longer needs markers since all relations are represented uniformly
     */
    private fun samplePath(steps: Int, cyclic: Boolean, chosenHeadTriple: Triple? = null): Path? {
        val entityNodes = IntArray(1 + steps)
        val relationNodes = LongArray(steps)
        
        val chosenTriples = Settings.SINGLE_RELATIONS?.let { relations ->
            val singleRelation = relations.random(rand)
            val singleRelationId = IdManager.getRelationId(singleRelation)
            triples.getTriplesByRelation(singleRelationId).also { tripleList ->
                if (tripleList.isEmpty()) {
                    System.err.println("chosen a SINGLE_RELATION=$singleRelation that is not instantiated in the training data")
                    System.exit(0)
                }
            }
        } ?: triples
        
        val triple = chosenHeadTriple ?: chosenTriples.random(rand)

        // TODO hardcoded test to avoid reflexive relations in the head
        if (triple.h == triple.t) return null
        
        // Always use the natural direction of the triple
        // The inverse direction is already represented by inverse triples in the dataset
        entityNodes[0] = triple.h
        relationNodes[0] = triple.r
        entityNodes[1] = triple.t

        // Add next hop - simplified since we only need to search head->tail
        // Reverse direction is handled by inverse triples
        for (index in 1 until steps) {
            val currentNodeId = entityNodes[index]
            
            // Only search from head to tail (forward direction)
            val candidateTriples = triples.getTriplesByHead(currentNodeId)
            
            if (candidateTriples.isEmpty()) return null
            
            // Filter out inverse relations that would create adjacent forward/inverse pairs
            val previousRelation = relationNodes[index - 1]
            val filteredCandidates = candidateTriples.filter { triple ->
                // Don't allow consecutive inverse relations
                val currentRel = triple.r
                val prevInverse = IdManager.getInverseRelationId(previousRelation)
                currentRel != prevInverse
            }
            
            if (filteredCandidates.isEmpty()) return null
            
            val nextTriple = if (cyclic && index + 1 == steps) {
                val targetNodeId = entityNodes[0]
                val cyclicCandidates = filteredCandidates.filter { triple ->
                    triple.t == targetNodeId // Always head->tail direction
                }
                if (cyclicCandidates.isEmpty()) return null
                cyclicCandidates.random(rand)
            } else {
                filteredCandidates.random(rand)
            }
            
            relationNodes[index] = nextTriple.r
            entityNodes[index + 1] = nextTriple.t
        }
        
        // Use new Path constructor with single relation encoding
        val path = Path(entityNodes, relationNodes)
        return when {
            steps == 1 -> path
            !cyclic && path.isCyclic -> null
            else -> path
        }
    }

    /**
     * Generate rule generalizations from a path using the new relation path approach.
     * Now creates simple Atom <- Atom rules instead of complex Body structures.
     */
    private fun getGeneralizations(p: Path, onlyXY: Boolean): ArrayList<Rule> {
        val generalizations = ArrayList<Rule>()
        
        // For single-hop paths (Zero rules), use the single relation directly
        if (p.relationNodes.size == 1) {
            val relation = p.relationNodes[0]
            
            // Normalize relation to use original relation
            val (headAtom, headEntitySwapped) = if (IdManager.isInverseRelation(relation)) {
                val originalRelation = IdManager.getInverseRelationId(relation)
                Atom(p.entityNodes[1], originalRelation, p.entityNodes[0]) to true  // Swap entities for inverse
            } else {
                Atom(p.entityNodes[0], relation, p.entityNodes[1]) to false
            }
            
            // Single-hop rules have no body
            // 2. X Acyclic Rule: r(X,c) <- (no body)
            val xHead = Atom(IdManager.getXId(), headAtom.r, headAtom.t)
            generalizations.add(RuleZero(RuleUntyped(xHead)))
            
            // 3. Y Acyclic Rule: r(c,Y) <- (no body)
            val yHead = Atom(headAtom.h, headAtom.r, IdManager.getYId())
            generalizations.add(RuleZero(RuleUntyped(yHead)))
            
            return generalizations
        }
        
        // For multi-hop paths, create rules with single atom bodies
        val headRelation = p.relationNodes[0] // Use first relation as head
        val headAtom = if (IdManager.isInverseRelation(headRelation)) {
            val originalRelation = IdManager.getInverseRelationId(headRelation)
            Atom(p.entityNodes[1], originalRelation, p.entityNodes[0])
        } else {
            Atom(p.entityNodes[0], headRelation, p.entityNodes[1])
        }
        
        println("Scorer.getGeneralizations: Multi-hop path with ${p.relationNodes.size} relations")
        println("Scorer.getGeneralizations: Head relation: ${IdManager.getRelationString(headRelation)}")
        println("Scorer.getGeneralizations: Entity path: ${p.entityNodes.map { IdManager.getEntityString(it) }}")
        
        // For multi-hop rules, the correct semantics should be:
        // If there's a connection through bodyRelation, then headRelation also holds
        // But we need to ensure both relations connect compatible entity types
        
        // 1. Cyclic Rule: headRelation(X,Y) <- bodyRelation(X,Y)
        // This only makes sense if both relations can connect the same entity types
        if (!onlyXY || p.relationNodes.size > 1) {
            val bodyRelation = p.relationNodes[1]
            println("Scorer.getGeneralizations: Body relation: ${IdManager.getRelationString(bodyRelation)}")
            
            // Check if the relations are semantically compatible
            // For now, skip rules where head and body relations are clearly incompatible
            val headRelationStr = IdManager.getRelationString(headRelation)
            val bodyRelationStr = IdManager.getRelationString(bodyRelation)
            
            // Skip rules with obviously incompatible semantics
            val isCompatible = when {
                // Genre-artist vs genre-parent_genre is incompatible
                headRelationStr.contains("parent_genre") && bodyRelationStr.contains("artists") -> false
                headRelationStr.contains("artists") && bodyRelationStr.contains("parent_genre") -> false
                
                // Person-place vs film-producer is incompatible  
                headRelationStr.contains("place_of_death") && bodyRelationStr.contains("produced_by") -> false
                headRelationStr.contains("produced_by") && bodyRelationStr.contains("place_of_death") -> false
                
                // Award relations should be compatible with each other
                headRelationStr.contains("award") && bodyRelationStr.contains("award") -> true
                
                // For now, allow other combinations but add more checks later
                else -> true
            }
            
            if (isCompatible) {
                val cyclicHead = Atom(IdManager.getXId(), headAtom.r, IdManager.getYId())
                val cyclicBody = if (IdManager.isInverseRelation(bodyRelation)) {
                    val originalBodyRelation = IdManager.getInverseRelationId(bodyRelation)
                    Atom(IdManager.getYId(), originalBodyRelation, IdManager.getXId())
                } else {
                    Atom(IdManager.getXId(), bodyRelation, IdManager.getYId())
                }
                println("Scorer.getGeneralizations: Creating compatible cyclic rule")
                generalizations.add(RuleCyclic(RuleUntyped(cyclicHead, cyclicBody), 0.0))
            } else {
                println("Scorer.getGeneralizations: Skipping incompatible cyclic rule")
            }
        }
        
        if (onlyXY) {
            return generalizations
        }
        
        // Similar compatibility checks for acyclic rules
        val bodyRelation = p.relationNodes[1]
        val headRelationStr = IdManager.getRelationString(headRelation)
        val bodyRelationStr = IdManager.getRelationString(bodyRelation)
        
        val isCompatible = when {
            headRelationStr.contains("parent_genre") && bodyRelationStr.contains("artists") -> false
            headRelationStr.contains("artists") && bodyRelationStr.contains("parent_genre") -> false
            headRelationStr.contains("place_of_death") && bodyRelationStr.contains("produced_by") -> false
            headRelationStr.contains("produced_by") && bodyRelationStr.contains("place_of_death") -> false
            headRelationStr.contains("award") && bodyRelationStr.contains("award") -> true
            else -> true
        }
        
        if (isCompatible) {
            // 2. X Acyclic Rule: headRelation(X,c) <- bodyRelation(X,?)
            val xHead = Atom(IdManager.getXId(), headAtom.r, headAtom.t)
            val xBody = if (IdManager.isInverseRelation(bodyRelation)) {
                val originalBodyRelation = IdManager.getInverseRelationId(bodyRelation)
                Atom(IdManager.getEntityId("·"), originalBodyRelation, IdManager.getXId())
            } else {
                Atom(IdManager.getXId(), bodyRelation, IdManager.getEntityId("·"))
            }
            generalizations.add(RuleAcyclic(RuleUntyped(xHead, xBody)))
            
            // 3. Y Acyclic Rule: headRelation(c,Y) <- bodyRelation(?,Y)  
            val yHead = Atom(headAtom.h, headAtom.r, IdManager.getYId())
            val yBody = if (IdManager.isInverseRelation(bodyRelation)) {
                val originalBodyRelation = IdManager.getInverseRelationId(bodyRelation)
                Atom(IdManager.getYId(), originalBodyRelation, IdManager.getEntityId("·"))
            } else {
                Atom(IdManager.getEntityId("·"), bodyRelation, IdManager.getYId())
            }
            generalizations.add(RuleAcyclic(RuleUntyped(yHead, yBody)))
        } else {
            println("Scorer.getGeneralizations: Skipping incompatible acyclic rules")
        }
        
        return generalizations
    }

    fun getScoringGain(rule: Rule, correctlyPredicted: Int, confidence: Double, appliedConfidence: Double): Double {
        if (Settings.REWARD == 1) return correctlyPredicted.toDouble()
        if (Settings.REWARD == 2) return correctlyPredicted.toDouble() * confidence
        if (Settings.REWARD == 3) return correctlyPredicted.toDouble() * appliedConfidence
        if (Settings.REWARD == 4) return correctlyPredicted.toDouble() * appliedConfidence * appliedConfidence
        if (Settings.REWARD == 5) return correctlyPredicted.toDouble() * appliedConfidence / 2.0.pow((rule.bodySize - 1).toDouble())
        return 0.0
    }
}
