package tarmorn.threads

import tarmorn.Settings
import tarmorn.data.TripleSet
import tarmorn.structure.Rule

/**
 * The predictor predicts the candidates for a knowledge base completion task.
 *
 *
 */
class Predictor(
    private val testSet: TripleSet,
    private val trainingSet: TripleSet,
    private val validationSet: TripleSet,
    private val k: Int,
    private val relation2Rules4Prediction: MutableMap<String, MutableList<Rule>>
) : Thread() {
    override fun run() {
        var triple = RuleEngine.nextPredictionTask
        // Rule rule = null;
        while (triple != null) {
            // println(this.getName() + " making prediction for " + triple);
            if (Settings.AGGREGATION_ID == 1) {
                RuleEngine.predictMax(testSet, trainingSet, validationSet, k, relation2Rules4Prediction, triple)
            }
            triple = RuleEngine.nextPredictionTask
        }
    }
}