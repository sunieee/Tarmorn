package tarmorn.structure

import tarmorn.Settings

object RuleFactory {
    fun getGeneralizations(p: Path, onlyXY: Boolean): ArrayList<Rule> {
        val rv = RuleUntyped()
        rv.body = Body()
        if (p.markers[0] == '+') {
            rv.head = Atom(p.nodes[0], p.nodes[1], p.nodes[2])
        } else {
            rv.head = Atom(p.nodes[2], p.nodes[1], p.nodes[0])
        }
        for (i in 1..<p.markers.size) {
            if (p.markers[i] == '+') {
                rv.body.add(Atom(p.nodes[i * 2], p.nodes[i * 2 + 1], p.nodes[i * 2 + 2]))
            } else {
                rv.body.add(Atom(p.nodes[i * 2 + 2], p.nodes[i * 2 + 1], p.nodes[i * 2]))
            }
        }
        val generalizations = ArrayList<Rule>()
        val leftright = rv.leftRightGeneralization
        if (leftright != null) {
            leftright.replaceAllConstantsByVariables()
            generalizations.add(RuleCyclic(leftright, 0.0))
        }
        if (onlyXY) return generalizations
        // acyclic rule
        val left = rv.leftGeneralization

        if (left != null) {
            if (left.bodysize() == 0) {
                generalizations.add(RuleZero(left))
            } else {
                val leftFree = left.createCopy()
                if (leftright == null) leftFree.replaceAllConstantsByVariables()
                left.replaceNearlyAllConstantsByVariables()
                if (!Settings.EXCLUDE_AC2_RULES) if (leftright == null) generalizations.add(RuleAcyclic2(leftFree))
                generalizations.add(RuleAcyclic1(left))
            }
        }
        val right = rv.rightGeneralization
        if (right != null) {
            if (right.bodysize() == 0) {
                generalizations.add(RuleZero(right))
            } else {
                val rightFree = right.createCopy()
                if (leftright == null) rightFree.replaceAllConstantsByVariables()
                right.replaceNearlyAllConstantsByVariables()
                if (!Settings.EXCLUDE_AC2_RULES) if (leftright == null) generalizations.add(RuleAcyclic2(rightFree))
                generalizations.add(RuleAcyclic1(right))
            }
        }
        return generalizations
    }
}
