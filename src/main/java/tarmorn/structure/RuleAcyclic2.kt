package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.IdManager
import tarmorn.data.Triple
import tarmorn.data.TripleSet

class RuleAcyclic2(r: RuleUntyped) : RuleAcyclic(r) {
    private var unboundVariable0: Int? = null

    override val unboundVariable: Int?
        get() {
            if (unboundVariable0 != null) return unboundVariable0
            
            val counter = mutableMapOf<Int, Int>()
            
            body.forEach { atom ->
                listOf(atom.left, atom.right)
                    .filter { it != IdManager.getXId() && it != IdManager.getYId() }
                    .forEach { variable -> 
                        counter[variable] = counter.getOrDefault(variable, 0) + 1
                    }
            }
            
            unboundVariable0 = counter.entries.find { it.value == 1 }?.key
            return unboundVariable0
        }


    override val appliedConfidence: Double
        get() =  Settings.RULE_AC2_WEIGHT * super.appliedConfidence


    override fun isSingleton(triples: TripleSet): Boolean {
        return false
    }


    /* probably out
	public boolean isRedundantACRule(TripleSet triples) {
		Atom last = this.body.getLast();
		if (last.isRightC()) {
			if (triples.getTriplesByRelation(last.getRelation()).size() < Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) {
				return true;
			}
		}
		else {
			if (triples.getTriplesByRelation(last.getRelation()).size() < Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) {
				return true;
			}
		}
		return false;
	}
	*/
    /**
     * Returns a lower border the number of groundings w.r.t the given triple set for the bound variable in the last atom.
     * @param triples The triples set to check for groundings.
     * @return The number of groundings if its lower  to Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS,
     * otherwise Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS
     */
    override fun getGroundingsLastAtom(triples: TripleSet): Int {
        val unboundVariable = this.unboundVariable
        val last = body.last
        
        val values = hashSetOf<Int>()
        val targetTriples = triples.getTriplesByRelation(last.relation)
        
        return when {
            last.right == unboundVariable -> {
                targetTriples.forEach { t ->
                    values.add(t.h)
                    if (values.size >= Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) 
                        return values.size
                }
                values.size
            }
            else -> {
                targetTriples.forEach { t ->
                    values.add(t.t)
                    if (values.size >= Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) 
                        return values.size
                }
                values.size
            }
        }
    }


    override fun getTripleExplanation(
        xValue: Int,
        yValue: Int,
        excludedTriples: Set<Triple>,
        triples: TripleSet
    ): Set<Triple> {
        System.err.println("You are asking for a triple explanation using an AC2 rule (a.k.a. U_d rule). Triple explanations for this rule are not yet implemented.")
        return emptySet()
    }

    override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        System.err.println("Method not yet available for an untyped rule")
        return intArrayOf(0, 0)
    }
}
