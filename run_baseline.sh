# Define the datasets to process
datasets=("FB15k-237" "FB15k" "KG20C" "WN18" "WN18RR" "codex-l")
# "YAGO3-10"

# Loop through each dataset
for dataset in "${datasets[@]}"; do
    echo "======================================"
    echo "Processing dataset: ${dataset}"
    echo "======================================"
    
    # Create output directory for this dataset
    mkdir -p out/$dataset
    
    # Generate dataset-specific config-learn.properties
    cat > "out/${dataset}/config-learn.properties" << EOF
PATH_TRAINING = data/${dataset}/train.txt

PATH_OUTPUT   = out/${dataset}/rules

SNAPSHOTS_AT = 10,50,100,400,1000

WORKER_THREADS = 20
EOF
    
    echo "Created config file: out/${dataset}/config-learn.properties"
    
    # Run AnyBURL learning with the dataset-specific config
    echo "Running AnyBURL learning for ${dataset}..."
    java -Xmx240G -cp AnyBURL-23-1x.jar de.unima.ki.anyburl.Learn "out/${dataset}/config-learn.properties"
    
    # Run evaluations
    echo "Running evaluations for ${dataset}..."
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-1000" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/eval-1000-noisyor.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-1000" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus > "out/${dataset}/eval-1000-maxplus.log"
    
    echo "Completed processing ${dataset}"
    echo ""
done

echo "All datasets processed successfully!"