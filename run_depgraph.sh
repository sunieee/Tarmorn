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
    
    # if_grouping
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus > "out/${dataset}/eval-maxplus.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/eval-noisyor.log"

    # aggregate_sharpness
    for aggregate_sharpness in 0 0.1 0.25 0.5 1 2 4; do
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness > "out/${dataset}/eval-${aggregate_sharpness}.log"
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method positive --positive_weight 0.5 > "out/${dataset}/eval-positive${aggregate_sharpness}_weight0.5.log"
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method positive --positive_weight 1.0 > "out/${dataset}/eval-positive${aggregate_sharpness}_weight1.0.log"
        
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method negative --negative_weight 0.5 > "out/${dataset}/eval-negative${aggregate_sharpness}_weight0.5.log"
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method negative --negative_weight 1.0 > "out/${dataset}/eval-negative${aggregate_sharpness}_weight1.0.log"

        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method all --positive_weight 0.5 --negative_weight 0.5 > "out/${dataset}/eval-all${aggregate_sharpness}_weight0.5.log"
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method all --positive_weight 1.0 --negative_weight 1.0 > "out/${dataset}/eval-all${aggregate_sharpness}_weight1.0.log"
        
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method positive_first --positive_weight 0.5 --negative_weight 0.5 > "out/${dataset}/eval-positive_first${aggregate_sharpness}_weight0.5.log"
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method positive_first --positive_weight 1.0 --negative_weight 1.0 > "out/${dataset}/eval-positive_first${aggregate_sharpness}_weight1.0.log"
        
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method negative_first --positive_weight 0.5 --negative_weight 0.5 > "out/${dataset}/eval-negative_first${aggregate_sharpness}_weight0.5.log"
        python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --dependency "out/${dataset}/dependency.txt" --ranking_file "out/${dataset}/eval.txt" --aggregation_sharpness $aggregate_sharpness --dependency_method negative_first --positive_weight 1.0 --negative_weight 1.0 > "out/${dataset}/eval-negative_first${aggregate_sharpness}_weight1.0.log"
    done

    

    # d_weight
    # python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --d_weight 0.3 > "out/${dataset}/eval-d_weight0.3.log"
    # python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --d_weight 0.5 > "out/${dataset}/eval-d_weight0.5.log"
    # python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --d_weight 1 > "out/${dataset}/eval-d_weight1.log"

    # # num_unseen
    # python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --num_unseen 1 > "out/${dataset}/eval-num_unseen1.log"
    # python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --num_unseen 3 > "out/${dataset}/eval-num_unseen3.log"
    # python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --num_unseen 10 > "out/${dataset}/eval-num_unseen10.log"

    python extract_metrics.py --dataset "${dataset}"
    echo "Completed all evaluations for ${dataset}"
    echo ""
done

echo "=========================================="
echo "All datasets processed successfully!"
echo "=========================================="



# Define the datasets to process
datasets=("FB15k-237" "FB15k" "KG20C" "WN18" "WN18RR" "codex-l" "YAGO3-10")
#  

# Loop through each dataset
for dataset in "${datasets[@]}"; do
    export dataset="${dataset}"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/eval-noisyor.log"
    python extract_metrics.py --dataset "${dataset}"
done