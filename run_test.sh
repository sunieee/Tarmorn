#!/usr/bin/env bash
set -euo pipefail

# Configuration
# export dataset="FB15k-237"
export dataset="YAGO3-10"
# export dataset="WN18RR"
# export dataset="codex-m"
# export dataset="codex-s"

mkdir -p "out/${dataset}"
echo "Log file: out/${dataset}/run.log"

# JVM memory settings for Maven
export MAVEN_OPTS="-Xms256g -Xmx256g -XX:MaxMetaspaceSize=2g"

mvn clean compile
mvn exec:java -Dexec.mainClass="tarmorn.TLearn" > "out/${dataset}/run.log" 2>&1

# python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --disable_combo > "out/${dataset}/eval.log"
# python script/compare_rules.py --dataset "${dataset}"

# python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" > "out/${dataset}/eval-maxplus-combo.log"
# python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --disable_combo > "out/${dataset}/eval-maxplus.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/eval-noisyor.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --noisyor_positive_method mst > "out/${dataset}/eval-noisyor+mst.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --noisyor_positive_method matching1 > "out/${dataset}/eval-noisyor+matching1.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --noisyor_positive_method matching2 > "out/${dataset}/eval-noisyor+matching2.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --noisyor_negative_method prune > "out/${dataset}/eval-noisyor-prune.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --noisyor_negative_method cluster > "out/${dataset}/eval-noisyor-cluster.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --noisyor_negative_method prune --noisyor_positive_method mst > "out/${dataset}/eval-noisyor-prune+mst.log"

# python eval.py --dataset "${dataset}" --test_valid_split RP --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/eval-RP-noisyor.log"
# python eval.py --dataset "${dataset}" --test_valid_split RP --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --combo_noisyor_method greed > "out/${dataset}/eval-RP-noisyor-greed.log"

# python evaltc.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/evaltc-noisyor.log"
# python evaltc.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --noisyor_positive_method mst > "out/${dataset}/evaltc-noisyor+mst.log"
# python evaltc.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/evaltc.txt" --aggregation_function noisyor --noisyor_positive_method matching1 > "out/${dataset}/evaltc-noisyor+matching1.log"
# python evaltc.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/evaltc.txt" --aggregation_function noisyor --noisyor_positive_method matching2 > "out/${dataset}/evaltc-noisyor+matching2.log"
# python evaltc.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/evaltc.txt" --aggregation_function noisyor --noisyor_negative_method prune > "out/${dataset}/evaltc-noisyor-prune.log"
# python evaltc.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/evaltc.txt" --aggregation_function noisyor --noisyor_negative_method cluster > "out/${dataset}/evaltc-noisyor-cluster.log"
# python evaltc.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/evaltc.txt" --aggregation_function noisyor --noisyor_negative_method prune --noisyor_positive_method mst > "out/${dataset}/evaltc-noisyor-prune+mst.log"


python eval.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}" --ranking_file "out/${dataset}/eval.txt" --applied_rules "out/${dataset}/applied_rules.json" --ranking_dump "out/${dataset}/ranking_dump.json"

python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --compare_eval_ranking  "out/${dataset}/ranking_dump.json"

