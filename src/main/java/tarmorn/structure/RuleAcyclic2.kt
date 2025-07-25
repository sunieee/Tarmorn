package tarmorn.structure

import tarmorn.Settings
import tarmorn.data.IdManager
import tarmorn.data.Triple
import tarmorn.data.TripleSet

class RuleAcyclic2(r: RuleUntyped) : RuleAcyclic(r) {
    private var unboundVariable0: Int? = null

    override val unboundVariable: Int?
        get() {
        if (this.unboundVariable0 != null) return this.unboundVariable0
        // if (this.body.get(this.body.size()-1).isLeftC() || this.body.get(this.body.size()-1).isRightC()) return null;
        val counter = HashMap<Int, Int>()
        for (atom in this.body) {
            if (atom.left != IdManager.getXId() && atom.left != IdManager.getYId()) {
                if (counter.containsKey(atom.left)) counter.put(atom.left, 2)
                else counter.put(atom.left, 1)
            }
            if (atom.right != IdManager.getXId() && atom.right != IdManager.getYId()) {
                if (counter.containsKey(atom.right)) counter.put(atom.right, 2)
                else counter.put(atom.right, 1)
            }
        }
        for (variable in counter.keys) {
            if (counter.get(variable) == 1) {
                this.unboundVariable0 = variable
                return variable
            }
        }
        // this can never happen
        return this.unboundVariable0
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
        val last = this.body.last
        if (last.right == unboundVariable) {
            val values = HashSet<Int>()
            for (t in triples.getTriplesByRelation(last.relation)) {
                values.add(t.h)
                if (values.size >= Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) return values.size
            }
            return values.size
        } else {
            val values = HashSet<Int>()
            for (t in triples.getTriplesByRelation(last.relation)) {
                values.add(t.t)
                if (values.size >= Settings.AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS) return values.size
            }
            return values.size
        }
    }


    override fun getTripleExplanation(
        xValue: Int,
        yValue: Int,
        excludedTriples: java.util.HashSet<Triple>,
        triples: TripleSet
    ): java.util.HashSet<Triple> {
        System.err.println("Your are asking for a triple explanation using an AC2 rule (a.k.a. U_d rule). Triple explanations for this rule are so far not implemented.")
        return HashSet<Triple>()
    }

    public override fun computeScores(that: Rule, triples: TripleSet): IntArray {
        System.err.println("method not yet available for an untyped rule")
        return IntArray(2)
    }
}
