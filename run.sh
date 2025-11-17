#!/bin/bash

# Configuration
dataset="FB15k-237"
split="louvain"
export MAVEN_OPTS="-Xms48g -Xmx48g -XX:MaxMetaspaceSize=2g"

# Set base DATASET environment variable
export DATASET="$dataset"

# Create output directories
mkdir -p "out/${dataset}/${split}/atom2formula2metric"
mkdir -p "out/${dataset}/${split}/rules"
mkdir -p "out/${dataset}/${split}/log"

# Loop through all .tsv files in the partitions directory
for filepath in out/${dataset}/${split}/partitions/*.tsv; do
    filename=$(basename "$filepath" .tsv)
    echo ""
    echo "================================================================================"
    echo "Processing partition: $filename"
    echo "Log file: out/${dataset}/${split}/log/${filename}.log"
    echo "================================================================================"
    
    # Set environment variables for this partition
    export PATH_TRAINING="out/${dataset}/${split}/partitions/${filename}.tsv"
    export PATH_RULES_JSON="out/${dataset}/${split}/atom2formula2metric/${filename}.json"
    export PATH_RULES_TXT="out/${dataset}/${split}/rules/${filename}.txt"
    
    # Run Maven and redirect output to log file
    mvn exec:java -Dexec.mainClass="tarmorn.TLearn" > "out/${dataset}/${split}/log/${filename}.log" 2>&1
    
    if [ $? -eq 0 ]; then
        echo "Successfully completed partition: $filename"
    else
        echo "ERROR: Failed to process partition $filename"
        echo "Check out/${dataset}/${split}/log/${filename}.log for details"
        echo "Continuing with next partition..."
    fi
done

echo ""
echo "================================================================================"
echo "All partitions processed"
echo "================================================================================"


python3 merge_rules.py "out/${dataset}/${split}/atom2formula2metric"