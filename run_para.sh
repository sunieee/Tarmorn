#!/usr/bin/env bash
set -euo pipefail

# Configuration
export dataset="FB15k-237"
# export dataset="FB15k"
# export dataset="KG20C"
# export dataset="YAGO3-10"
# export dataset="WN18RR"
# export dataset="codex-l"
# export dataset="codex-s"

mkdir -p "out/${dataset}"
echo "Log file: out/${dataset}/run.log"

# JVM memory settings for Maven
export MAVEN_OPTS="-Xms256g -Xmx256g -XX:MaxMetaspaceSize=2g"
export MODE=0
# mvn clean compile exec:java > "out/${dataset}/run.log" 2>&1

# baseline: aggregation_function: "noisyor"
# if_grouping: false
# binary_weight: 1.0
# aggregate_sharpness: 0
# negative_weight: 0.0
# positive_weight: 0.0
# positive_method: "matching1"
# d_weight: 0.1
# num_unseen: 5

# if_grouping
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" > "out/${dataset}/eval-noisyor.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus > "out/${dataset}/eval-maxplus.log"

# aggregate_sharpness
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 0.5 > "out/${dataset}/eval-aggregate_sharpness0.5.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 1 > "out/${dataset}/eval-aggregate_sharpness1.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 2 > "out/${dataset}/eval-aggregate_sharpness2.log"

# negative_weight
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --negative_weight 0.5 > "out/${dataset}/eval-negative_weight0.5.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --negative_weight 1 > "out/${dataset}/eval-negative_weight1.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --negative_weight 2 > "out/${dataset}/eval-negative_weight2.log"

# positive_weight
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --positive_weight 0.5 > "out/${dataset}/eval-positive_weight0.5.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --positive_weight 1 > "out/${dataset}/eval-positive_weight1.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --positive_weight 2 > "out/${dataset}/eval-positive_weight2.log"

# positive_method
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --positive_method mst > "out/${dataset}/eval-positive_method_mst.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --positive_method matching2 > "out/${dataset}/eval-positive_method_matching2.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --positive_method all > "out/${dataset}/eval-positive_method_all.log"

# d_weight
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --d_weight 0.3 > "out/${dataset}/eval-d_weight0.3.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --d_weight 0.5 > "out/${dataset}/eval-d_weight0.5.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --d_weight 1 > "out/${dataset}/eval-d_weight1.log"

# num_unseen
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --num_unseen 1 > "out/${dataset}/eval-num_unseen1.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --num_unseen 3 > "out/${dataset}/eval-num_unseen3.log"
python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --num_unseen 10 > "out/${dataset}/eval-num_unseen10.log"

python extract_metrics.py --dataset "${dataset}"