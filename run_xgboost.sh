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
    
    # Set JVM memory settings for Maven
    export MAVEN_OPTS="-Xms240g -Xmx240g -XX:MaxMetaspaceSize=2g"
    
    # Run Maven build and execution
    echo "Running Maven compile and exec for ${dataset}..."
    # mvn clean compile exec:java > "out/${dataset}/run.log" 2>&1
    
    echo "Running validation evaluation for ${dataset} to generate dependency graph..."
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --dependency_graph "out/${dataset}/dependency_graph.csv" > "out/${dataset}/eval-valid.log"
    
    echo "Learn XGBoost ranker for ${dataset}..."
    python train_xgboost.py --dataset "${dataset}" > "out/${dataset}/train_xgboost.log" 2>&1

    echo "Running evaluations for ${dataset}..."
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/${ruleset}" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function xgboost --xgboost_model "out/${dataset}/dependency_graph.json" > "out/${dataset}/eval-xgboost.log"

done

echo "=========================================="
echo "All datasets processed successfully!"
echo "=========================================="
