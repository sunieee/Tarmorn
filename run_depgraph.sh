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
    
    # if_grouping
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --no_grouping > "out/${dataset}/eval-no_grouping.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" > "out/${dataset}/eval-noisyor.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus > "out/${dataset}/eval-maxplus.log"
    
    # binary_weight
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --binary_weight 0 > "out/${dataset}/eval-binary_weight0.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --binary_weight 0.25 > "out/${dataset}/eval-binary_weight0.25.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --binary_weight 0.5 > "out/${dataset}/eval-binary_weight0.5.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --binary_weight 0.75 > "out/${dataset}/eval-binary_weight0.75.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --binary_weight 1.5 > "out/${dataset}/eval-binary_weight1.5.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --binary_weight 2 > "out/${dataset}/eval-binary_weight2.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --binary_weight 4 > "out/${dataset}/eval-binary_weight4.log"
    
    # aggregate_sharpness
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 0.5 > "out/${dataset}/eval-aggregate_sharpness0.5.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 1 > "out/${dataset}/eval-aggregate_sharpness1.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 2 > "out/${dataset}/eval-aggregate_sharpness2.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 0.5 --no_grouping > "out/${dataset}/eval-no_grouping-aggregate_sharpness0.5.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 1 --no_grouping > "out/${dataset}/eval-no_grouping-aggregate_sharpness1.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --aggregate_sharpness 2 --no_grouping > "out/${dataset}/eval-no_grouping-aggregate_sharpness2.log"
    
    # negative_weight
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --negative_weight 0.5 > "out/${dataset}/eval-negative_weight0.5.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --negative_weight 1 > "out/${dataset}/eval-negative_weight1.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --negative_weight 2 > "out/${dataset}/eval-negative_weight2.log"
    
    # positive_weight
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --positive_weight 0 > "out/${dataset}/eval-positive_weight0.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rule.txt" --ranking_file "out/${dataset}/eval.txt" --positive_weight 0.5 > "out/${dataset}/eval-positive_weight0.5.log"
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
    
    echo "Completed all evaluations for ${dataset}"
    echo ""
done

echo "=========================================="
echo "All datasets processed successfully!"
echo "=========================================="
