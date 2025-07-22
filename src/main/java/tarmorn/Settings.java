package tarmorn;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class Settings {
    public static String PATH_TRAINING;
    public static String PATH_TEST;
    public static String PATH_VALID;
    public static String PATH_RULES;
    public static String PATH_RULES_BASE;
    public static String PATH_OUTPUT;
    public static int WORKER_THREADS;
    public static int TOP_K_OUTPUT;
    public static boolean OI_CONSTRAINTS_ACTIVE;
    public static boolean REWRITE_REFLEXIV;
    public static String REWRITE_REFLEXIV_TOKEN;
    public static boolean BEAM_NOT_DFS;
    public static boolean DFS_SAMPLING_ON;
    public static boolean BEAM_TYPE_EDIS;
    public static boolean SAFE_PREFIX_MODE;
    public static String PREFIX_ENTITY;
    public static String PREFIX_RELATION;
    public static boolean CONSTANTS_OFF;
    public static double EPSILON;
    public static double RANDOMIZED_DECISIONS_ANNEALING;
    public static int AC_MIN_NUM_OF_LAST_ATOM_GROUNDINGS;
    public static double SPECIALIZATION_CI;
    public static int REWARD;
    public static int POLICY;
    public static String PREDICTION_TYPE;
    public static String PATH_DICE;
    public static String PATH_EXPLANATION;
    public static int[] SNAPSHOTS_AT;
    public static int TRIAL_SIZE;
    public static int DISCRIMINATION_BOUND;
    public static int BRANCHINGFACTOR_BOUND;
    public static int BATCH_TIME;
    public static int MAX_LENGTH_CYCLIC;
    public static boolean ZERO_RULES_ACTIVE;
    public static int MAX_LENGTH_ACYCLIC;
    public static int MAX_LENGTH_GROUNDED_CYCLIC;
    public static boolean EXCLUDE_AC2_RULES;
    public static double SATURATION;
    public static int THRESHOLD_CORRECT_PREDICTIONS;
    public static int THRESHOLD_CORRECT_PREDICTIONS_ZERO;
    public static int READ_THRESHOLD_CORRECT_PREDICTIONS;
    public static int UNSEEN_NEGATIVE_EXAMPLES;
    public static int UNSEEN_NEGATIVE_EXAMPLES_REFINE;
    public static boolean TYPE_SPLIT_ANALYSIS_;
    public static double THRESHOLD_CONFIDENCE;
    public static double READ_THRESHOLD_CONFIDENCE;
    public static double READ_THRESHOLD_MAX_RULE_LENGTH;
    public static String AGGREGATION_TYPE;
    public static int AGGREGATION_ID;
    public static int AGGREGATION_MAX_NUM_RULES_PER_CANDIDATE;
    public static int SAMPLE_SIZE;
    public static int BEAM_SAMPLING_MAX_BODY_GROUNDINGS;
    public static int BEAM_SAMPLING_MAX_BODY_GROUNDING_ATTEMPTS;
    public static int BEAM_SAMPLING_MAX_REPETITIONS;
    public static double RULE_ZERO_WEIGHT;
    public static double RULE_AC2_WEIGHT;
    public static double RULE_LENGTH_DEGRADE;
    public static int READ_CYCLIC_RULES;
    public static int READ_ACYCLIC1_RULES;
    public static int READ_ACYCLIC2_RULES;
    public static int READ_ZERO_RULES;

	public static PrintWriter EXPLANATION_WRITER = null;
	public static String[] SINGLE_RELATIONS = null;

    public static void load() {
        String yamlPath = "config.yaml";
        try (FileInputStream in = new FileInputStream(yamlPath)) {
            Yaml yaml = new Yaml();
            Map<String, Object> config = yaml.load(in);
            loadFromYaml(config);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load config from " + yamlPath, e);
        }

        if (Settings.AGGREGATION_TYPE.equals("maxplus")) Settings.AGGREGATION_ID = 1;
        if (Settings.AGGREGATION_TYPE.equals("max2")) Settings.AGGREGATION_ID = 2;
        if (Settings.AGGREGATION_TYPE.equals("noisyor")) Settings.AGGREGATION_ID = 3;
        if (Settings.AGGREGATION_TYPE.equals("maxgroup")) Settings.AGGREGATION_ID = 4;
    }

    public static void loadFromYaml(Map<String, Object> config) {
        for (Field field : Settings.class.getFields()) {
            Object value = config.get(field.getName());
            if (value != null) {
                Class<?> type = field.getType();
                try {
                    if (type == int.class) {
                        field.setInt(null, Integer.parseInt(value.toString()));
                    } else if (type == double.class) {
                        field.setDouble(null, Double.parseDouble(value.toString()));
                    } else if (type == boolean.class) {
                        field.setBoolean(null, Boolean.parseBoolean(value.toString()));
                    } else if (type == String.class) {
                        field.set(null, value.toString());
                    } else if (type == int[].class && value instanceof java.util.List) {
                        java.util.List<?> list = (java.util.List<?>) value;
                        int[] arr = list.stream().mapToInt(o -> Integer.parseInt(o.toString())).toArray();
                        field.set(null, arr);
                    }
                    // 可扩展支持更多类型
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to set field: " + field.getName(), e);
                }
            }
        }
    }
    
}
