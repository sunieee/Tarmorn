#!/usr/bin/env bash
set -euo pipefail

# Define the datasets to process
datasets=("FB15k-237" "FB15k" "KG20C" "WN18" "WN18RR" "codex-l" "YAGO3-10")
ruleset="rules-100-3"

# Loop through each dataset
for dataset in "${datasets[@]}"; do
    echo "=========================================="
    echo "Processing dataset: ${dataset}"
    echo "=========================================="
    
    # Export dataset name
    export dataset="${dataset}"
    
    # Create output directory for this dataset
    mkdir -p "out/${dataset}"
    rm -rf out/${dataset}/eval-*.log
    
    echo "Running validation evaluation for ${dataset} to generate dependency graph..."
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}" --ranking_file "out/${dataset}/eval.txt" --applied_rules "out/${dataset}/applied_rules.json" > "out/${dataset}/eval.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus > "out/${dataset}/eval-maxplus.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}" --ranking_file "out/${dataset}/eval.txt" --applied_rules "out/${dataset}/applied_rules_valid.json" --valid > "out/${dataset}/eval_valid.log"
    

    # Run Maven build and execution
    echo "Running Maven compile and exec for ${dataset}..."
    export MAVEN_OPTS="-Xms240g -Xmx240g -XX:MaxMetaspaceSize=2g"
    mvn clean compile exec:java > "out/${dataset}/run.log"

    echo "Learn XGBoost ranker for ${dataset}..."
    python train_xgb_pairwise.py --dataset "${dataset}" > "out/${dataset}/train_xgboost.log"

    echo "Running evaluations for ${dataset}..."
    python eval_xgb_ranker.py --dataset "${dataset}" > "out/${dataset}/eval-xgboost.log"

done

echo "=========================================="
echo "All datasets processed successfully!"
echo "=========================================="
