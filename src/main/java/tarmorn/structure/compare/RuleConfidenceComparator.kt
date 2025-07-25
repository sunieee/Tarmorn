package tarmorn.structure.compare

import tarmorn.structure.Rule

class RuleConfidenceComparator : Comparator<Rule> {
    override fun compare(o1: Rule, o2: Rule): Int {
        // double prob1;
        // double prob2;
        val prob1 = o1.appliedConfidence
        val prob2 = o2.appliedConfidence


        if (prob1 < prob2) return 1
        else if (prob1 > prob2) return -1
        return 0
    }
}
