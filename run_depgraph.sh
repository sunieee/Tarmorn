#!/usr/bin/env bash
set -euo pipefail

# Define the datasets to process
datasets=("FB15k-237" "FB15k" "KG20C" "WN18" "WN18RR" "codex-l" "YAGO3-10")
#  

# Loop through each dataset
for dataset in "${datasets[@]}"; do
    echo "=========================================="
    echo "Processing dataset: ${dataset}"
    echo "=========================================="
    
    # Export dataset name
    export dataset="${dataset}"
    
    # Create output directory for this dataset
    mkdir -p "out/${dataset}"
    
    # Set JVM memory settings for Maven
    export MAVEN_OPTS="-Xms240g -Xmx240g -XX:MaxMetaspaceSize=2g"
    
    # Run Maven build and execution
    echo "Running Maven compile and exec for ${dataset}..."
    mvn clean compile exec:java > "out/${dataset}/run.log" 2>&1
    
    echo "Maven execution completed for ${dataset}"
    echo "Running evaluations for ${dataset}..."
    
    # All evaluation commands from run_para.sh
    rm -rf out/${dataset}/eval-*.log

    # python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --dependency_graph "out/${dataset}/dependency_graph.csv" --valid > "out/${dataset}/eval-valid.log"
    
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}" --ranking_file "out/${dataset}/eval.txt" --applied_rules "out/${dataset}/applied_rules.json" --ranking_dump "out/${dataset}/ranking_dump.json" > "out/${dataset}/eval-noisyor.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}" --aggregation_function maxplus --ranking_dump "out/${dataset}/ranking_dump_maxplus.json" > "out/${dataset}/eval-maxplus.log"

    for num_unseen in 0 1 3 5 10; do
        python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --num_unseen ${num_unseen} --aggregation maxplus > "out/${dataset}/eval-maxplus-numunseen${num_unseen}.log"
        python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --num_unseen ${num_unseen} --aggregation noisyor > "out/${dataset}/eval-noisyor-numunseen${num_unseen}.log"
    done

    for decay in 01 1 3 5 7 9; do
        python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --num_unseen 0 --aggregation decay${decay} > "out/${dataset}/eval-decay${decay}.log"
    done

    for num in 0 1 2 3 5; do
        python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --dependency_json "out/${dataset}/dependency.json" --num_unseen 0 --aggregation maxplus+dep${num} > "out/${dataset}/eval-maxplus+dep${num}.log"
        python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --dependency_json "out/${dataset}/dependency.json" --num_unseen 0 --aggregation noisyor+dep${num} > "out/${dataset}/eval-noisyor+dep${num}.log"
        python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --dependency_json "out/${dataset}/dependency.json" --num_unseen 0 --aggregation noisyor+depm${num} > "out/${dataset}/eval-noisyor+depm${num}.log"
        python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --dependency_json "out/${dataset}/dependency.json" --num_unseen 0 --aggregation noisyor-dep${num} > "out/${dataset}/eval-noisyor-dep${num}.log"
        python eval_base_ranker.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}"  --applied_rules "out/${dataset}/applied_rules.json" --dependency_json "out/${dataset}/dependency.json" --num_unseen 0 --aggregation noisyor-depm${num} > "out/${dataset}/eval-noisyor-depm${num}.log"
    done    

    python extract_metrics.py --dataset "${dataset}"
    echo "Completed all evaluations for ${dataset}"
    echo ""
done

echo "=========================================="
echo "All datasets processed successfully!"
echo "=========================================="

