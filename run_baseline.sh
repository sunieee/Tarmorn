# Define the datasets to process
datasets=("FB15k-237" "FB15k" "KG20C" "WN18" "WN18RR" "codex-l" "YAGO3-10")
# 

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

SNAPSHOTS_AT = 10,100,400,1000

WORKER_THREADS = 20
EOF
    
    echo "Created config file: out/${dataset}/config-learn.properties"
    
    # Run AnyBURL learning with the dataset-specific config
    echo "Running AnyBURL learning for ${dataset}..."
    java -Xmx240G -cp AnyBURL-23-1x.jar de.unima.ki.anyburl.Learn "out/${dataset}/config-learn.properties"

    # 生成相应阈值的rule（过滤规则文件，阈值为3）
    echo "Filtering rules for ${dataset} with threshold 3..."
    cd "out/${dataset}" || continue
    rm -rf rules-*-3
    for input_file in rules-*; do
        [ ! -f "$input_file" ] && continue
        # 跳过已经是过滤文件的
        [[ "$input_file" == *-3 ]] && continue
        output_file="${input_file}-3"
        awk -v threshold=3 '$2 >= threshold' "$input_file" > "$output_file"
        original_lines=$(wc -l < "$input_file" 2>/dev/null || echo 0)
        filtered_lines=$(wc -l < "$output_file" 2>/dev/null || echo 0)
        diff=$((original_lines - filtered_lines))
        echo "  $input_file -> $output_file, 原始行数: $original_lines, 过滤后: $filtered_lines, 减少: $diff 行"
    done
    cd - > /dev/null || exit
    
    # Run evaluations
    echo "Running evaluations for ${dataset}..."
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/eval-100-noisyor.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus > "out/${dataset}/eval-100-maxplus.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-400" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/eval-400-noisyor.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-400" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus > "out/${dataset}/eval-400-maxplus.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-1000" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor > "out/${dataset}/eval-1000-noisyor.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-1000" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus > "out/${dataset}/eval-1000-maxplus.log"
    
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --aggregation_function noisyor --applied_rules "out/${dataset}/applied_rules.json" > "out/${dataset}/eval-100-3-noisyor.log"
    python eval.py --dataset "${dataset}" --rules "out/${dataset}/rules-100-3" --ranking_file "out/${dataset}/eval.txt" --aggregation_function maxplus --applied_rules "out/${dataset}/applied_rules.json" > "out/${dataset}/eval-100-3-maxplus.log"
    python extract_metrics.py --dataset "${dataset}"
    echo "Completed processing ${dataset}"
    echo ""
done

echo "All datasets processed successfully!"