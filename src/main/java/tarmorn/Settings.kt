package tarmorn

import org.yaml.snakeyaml.Yaml
import java.io.FileInputStream
import java.io.IOException
import java.io.PrintWriter

object Settings {
    // Path to the file that contains the triple set used for learning the rules.
    @JvmField
    var PATH_TRAINING: String = "data/FB15k/train.txt"
    // Path to the file that contains the triple set used for to test the rules.
    @JvmField
    var PATH_TEST: String = "data/FB15k/test.txt"
    // Path to the file that contains the triple set used validation purpose (e.g. learning hyper parameter).
    @JvmField
    var PATH_VALID: String = "data/FB15k/valid.txt"
    // Path to the file that contains the rules that will be refined or will be sued for prediction.
    @JvmField
    var PATH_RULES: String = "out/FB15k/rules-100"
    // Path to the file that contains the rules that will be used as base,
    // i.e. this rule set will be added to all other rule sets loaded.
    @JvmField
    var PATH_RULES_BASE: String = ""
    // Path to the output file where the rules / predictions  will be stored.
    @JvmField
    var PATH_OUTPUT: String = "out/FB15k/preds-100"
    // The number of worker threads which compute the scores of the constructed rules, should be one less then the number of available cores.
    @JvmField
    var WORKER_THREADS: Int = 20
    // The top-k results that are after filtering kept in the results.
    @JvmField
    var TOP_K_OUTPUT: Int = 100

    // If set to true, which is the default value, the OI constraints are activated in cyclic rules.
    // This value is changed only for experimental reasons. Its default should not be changed.
    @JvmField
    var OI_CONSTRAINTS_ACTIVE: Boolean = true

    @JvmField
    var BEAM_NOT_DFS: Boolean = true
    @JvmField
    var DFS_SAMPLING_ON: Boolean = true

    // If set to true, in computeScores the starting point is a randomly chosen entity.
    // This entity is randomly chosen from the set of all entities that are a possible starting point.
    // If set to false its a randomly chosen triples that instantiates the first body atom.
    // This means that in this setting a starting entity that appears in many such triples, will be more frequently a starting entity.
    @JvmField
    var BEAM_TYPE_EDIS: Boolean = true

    // If set to true, it adds a prefix in front of each entity and relation id, when reading triplesets from files
    // to avoid problem related to input that uses numbers as ids only.
    @JvmField
    var SAFE_PREFIX_MODE: Boolean = false

    @JvmField
    var PREFIX_ENTITY: String = "e"
    @JvmField
    var PREFIX_RELATION: String = "r"

    // Suppresses any rules with constants.
    @JvmField
    var CONSTANTS_OFF: Boolean = false

    @JvmField
    var EPSILON: Double = 0.1

    // In the first batch the decisions are completely randomized. This random influence becomes less
    // at will be stable at  RANDOMIZED_DECISIONS after this number of batches have been carried out.
    @JvmField
    var RANDOMIZED_DECISIONS_ANNEALING: Double = 5.0

    // REACTIVATED!
    // This number defines if a rule to be redundant if the number of groundings for its last atom is less than this parameter.
    // It avoid that rules with constants are too specific and thus redundant compared to shorter rules
    //    head(X,c) <= hasGender(X, female)
    //    head(X,c) <= hasGender(X, A), hasGender(berta, A)
    // The second rule will be filtered out, because berta has only 1 gender, which is female.
    @JvmField
    var AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS: Int = 5

    // PROBABLY OUT
    // The specialization confidence interval determines that a rule shall only be accepted as a specialization of a more general rule, if
    // it has a higher confidence and if the probability that its confidence is really higher is at least that chosen value.
    // Possible values are 0.9, 0.99 and 0.99.
    // -1 = turned-off
    @JvmField
    var SPECIALIZATION_CI: Double = -1.0

    // Relevant for reinforced learning, how to compute the scores created by a thread.
    // 1 = correct predictions
    // 2 = correct predictions weighted by confidence
    // 3 = correct predictions weighted by applied confidence
    // 4 = correct predictions weighted by applied confidence^2
    // 5 = correct predictions weighted by applied confidence divided by (rule length-1)^2
    @JvmField
    var REWARD: Int = 5

    // Relevant for reinforced learning, how to use the scores created by a thread within the decision.
    // 1 = GREEDY = Epsilon greedy: Focus only on the best.
    // 2 = WEIGHTED = Weighted policy: focus as much as much a a path type, as much as it gave you.
    @JvmField
    var POLICY: Int = 2

    // Defines the prediction type which also influences the usage of the other parameters.
    // Possible values are currently aRx and xRy.
    @JvmField
    var PREDICTION_TYPE: String = "aRx"

    // Path to the output file where statistics of the dice are stored.
    // Can be used in reinforcement learning only. If "", nothing is stored.
    @JvmField
    var PATH_DICE: String = ""

    // Path to the output file where the explanations are stored. If "", no explanations are stored.
    @JvmField
    var PATH_EXPLANATION: String = ""

    // Takes a snapshot of the rules refined after each time interval specified in seconds.
    @JvmField
    var SNAPSHOTS_AT: IntArray? = intArrayOf(10, 100, 200, 400)

    // Number of maximal attempts to create body grounding. Every partial body grounding is counted.
    // NO LONGER IN USE (maybe)
    @JvmField
    var TRIAL_SIZE: Int = 1000000

    // Returns only results for head or tail computation if the results set has less elements than this bound.
    // The idea is that any results set which has more elements is anyhow not useful for a top-k ranking.
    // Should be set to a value thats higher than the k of the requested top-k (however, the higher the value,
    // the more runtime is required)
    @JvmField
    var DISCRIMINATION_BOUND: Int = 10000

    // This is the upper limit that is allowed as branching factor in a cyclic rule.
    // If more than this number of children would be created in the search tree, the
    // branch is not visited. Note that this parameter is nor relevant for the last
    // step, where the DISCRIMINATION_BOUND is the relevant parameter.
    @JvmField
    var BRANCHINGFACTOR_BOUND: Int = 1000

    // The time that is reserved for one batch in milliseconds.
    @JvmField
    var BATCH_TIME: Int = 5000

    // The maximal number of body atoms in cyclic rules (inclusive this number). If this number is exceeded all computation time
    // is used for acyclic rules only from that time on.
    @JvmField
    var MAX_LENGTH_CYCLIC: Int = 3

    // Determines whether or not the zero rules e.g. (gender(X, male) <= [0.5]) are active
    @JvmField
    var ZERO_RULES_ACTIVE: Boolean = true

    // is used for cyclic rules only from that time on.
    @JvmField
    var MAX_LENGTH_ACYCLIC: Int = 1

    // The maximal number of body atoms in partially grounded cyclic rules (inclusive this number). If this number is exceeded than a
    // cyclic path that would allow to construct such a rule (where the constant in the head and in the body is the same) is used for constructing
    // general rules only, partially grounded rules are not constructed from such a path.
    @JvmField
    var MAX_LENGTH_GROUNDED_CYCLIC: Int = 1

    // Experiments have shown that AC2 rules seem make results worse for most datasets. Setting this parameter to true, will result into not learning
    // AC2 rules at all. The rules are not even constructed and computation time is this spent on the other rules-
    @JvmField
    var EXCLUDE_AC2_RULES: Boolean = false

    // The saturation defined when to stop the refinement process. Once the saturation is reached, no further refinements are searched for.
    @JvmField
    var SATURATION: Double = 0.99

    // The threshold for the number of correct prediction created with the refined rule.
    @JvmField
    var THRESHOLD_CORRECT_PREDICTIONS: Int = 2

    // The threshold for the number of correct prediction created with a zero rule.
    @JvmField
    var THRESHOLD_CORRECT_PREDICTIONS_ZERO: Int = 100

    // The threshold for the number of correct predictions. Determines which rules are read from a file and which are ignored.
    @JvmField
    var READ_THRESHOLD_CORRECT_PREDICTIONS: Int = 2

    // The number of negative examples for which we assume that they exist, however, we have not seen them. Rules with high coverage are favored the higher the chosen number.
    @JvmField
    var UNSEEN_NEGATIVE_EXAMPLES: Int = 5

    // The number of negative examples for which we assume that they exist, however, we have not seen them.
    // This number is for each refinements step, including the refinement of a refined rule.
    @JvmField
    var UNSEEN_NEGATIVE_EXAMPLES_REFINE: Int = 5

    // If set to true, the rule application is done on the rule set and each subset that consists of one type as well as each subset that removed one type.
    // This setting should be used in an ablation study.
    @JvmField
    var TYPE_SPLIT_ANALYSIS_: Boolean = false

    // The threshold for the confidence of the refined rule
    @JvmField
    var THRESHOLD_CONFIDENCE: Double = 0.0001

    // The threshold for the confidence of the rule. Determines which rules are read from a file by the rule reader.
    @JvmField
    var READ_THRESHOLD_CONFIDENCE: Double = 0.0001

    // The maximal size of the rules that are stored when reading them from a file.
    // Determines which rules are read from a file by the rule reader.
    // All rules with a body length > then this number are ignored.
    @JvmField
    var READ_THRESHOLD_MAX_RULE_LENGTH: Double = 10.0

    // Defines how to combine probabilities that come from different rules
    // Possible values are: maxplus, max2, noisyor, maxgroup (not yet implemented)
    @JvmField
    var AGGREGATION_TYPE: String = "maxplus"

    // This value is overwritten by the choice made vie the AGGREGATION_TYPE parameter
    @JvmField
    var AGGREGATION_ID: Int = 1

    // This parameter is only relevant if noisyor aggregation is chosen. In default it is set to -1 which means that all rules that
    // fired are combined in a noisy-or product.
    @JvmField
    var AGGREGATION_MAX_NUM_RULES_PER_CANDIDATE: Int = -1

    // Used for restricting the number of samples drawn for computing scores as confidence.
    @JvmField
    var SAMPLE_SIZE: Int = 2000

    // The maximal number of body groundings. Once this number of body groundings has been reached,
    // the sampling process stops and confidence score is computed.
    @JvmField
    var BEAM_SAMPLING_MAX_BODY_GROUNDINGS: Int = 1000

    // The maximal number of attempts to create a body grounding. Once this number of attempts has been reached
    // the sampling process stops and confidence score is computed.
    @JvmField
    var BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS: Int = 100000

    // If a rule has only few different body groundings this parameter prevents that all attempts are conducted.
    // The value of this parameter determines how often a same grounding is drawn, before the sampling process stops
    // and the and confidence score is computed, e.g, 5 means that the algorithm stops if 5 times a grounding is
    // constructed tat has been found previously. The higher the value the mor probably it is that the sampling
    // computes the correct value for rules with few body groundings.
    @JvmField
    var BEAM_SAMPLING_MAX_REPETITIONS: Int = 5

    // The weights that is multiplied to compute the applied confidence of a zero rule. 1.0 means that zero rules are treated in the same way as the other rules.
    @JvmField
    var RULE_ZERO_WEIGHT: Double = 0.01

    // The weights that is multiplied to compute the applied confidence of an AC2 rule. 1.0 means that AC2 rules are treated in the same way as the other rules.
    @JvmField
    var RULE_AC2_WEIGHT: Double = 0.1

    // If the default is the confidence of cyclic rules of length 1 is multiplied by 0.75^0=1,
    // length 2 is multiplied by 0.75^1=0.75, length 3 is multiplied by 0.75^2=0.65
    @JvmField
    var RULE_LENGTH_DEGRADE: Double = 1.0

    @JvmField
    var READ_CYCLIC_RULES: Int = 1
    @JvmField
    var READ_ACYCLIC1_RULES: Int = 1
    @JvmField
    var READ_ACYCLIC2_RULES: Int = 1
    @JvmField
    var READ_ZERO_RULES: Int = 1

    @JvmField
    var EXPLANATION_WRITER: PrintWriter? = null
    @JvmField
    var SINGLE_RELATIONS: Array<String>? = null

    @JvmStatic
    fun load() {
        val yamlPath = "config.yaml"
        try {
            FileInputStream(yamlPath).use { `in` ->
                val yaml = Yaml()
                val config = yaml.load<MutableMap<String, Any>>(`in`)
                loadFromYaml(config)
            }
        } catch (e: IOException) {
            throw RuntimeException("Failed to load config from " + yamlPath, e)
        }

        if (AGGREGATION_TYPE == "maxplus") AGGREGATION_ID = 1
        if (AGGREGATION_TYPE == "max2") AGGREGATION_ID = 2
        if (AGGREGATION_TYPE == "noisyor") AGGREGATION_ID = 3
        if (AGGREGATION_TYPE == "maxgroup") AGGREGATION_ID = 4
    }

    fun loadFromYaml(config: MutableMap<String, Any>) {
        for (field in Settings::class.java.getFields()) {
            val value = config.get(field.getName())
            if (value != null) {
                val type = field.getType()
                try {
                    if (type == Int::class.javaPrimitiveType) {
                        field.setInt(null, value.toString().toInt())
                    } else if (type == Double::class.javaPrimitiveType) {
                        field.setDouble(null, value.toString().toDouble())
                    } else if (type == Boolean::class.javaPrimitiveType) {
                        field.setBoolean(null, value.toString().toBoolean())
                    } else if (type == String::class.java) {
                        field.set(null, value.toString())
                    } else if (type == IntArray::class.java && value is MutableList<*>) {
                        val list = value
                        val arr = list.stream().mapToInt { o: Any? -> o.toString().toInt() }.toArray()
                        field.set(null, arr)
                    }
                } catch (e: IllegalAccessException) {
                    throw RuntimeException("Failed to set field: " + field.getName(), e)
                }
            }
        }
    }
}