dataset=FB15k
rule_file=preds-100-400
supp_threshold=20
conf_threshold=0

awk -v supp="$supp_threshold" -v conf="$conf_threshold" '$2 >= supp && $3+0 >= conf {print; count++} END {print "Total:", count > "/dev/stderr"}' "out/${dataset}/${rule_file}" \
    > "out/${dataset}/rules_${supp_threshold}_${conf_threshold}.txt"

# python eval.py --dataset $dataset --rules out/${dataset}/rules_${supp_threshold}_${conf_threshold}.txt \
#     --ranking_file out/${dataset}/ranking_${supp_threshold}_${conf_threshold}.txt \
#     > out/${dataset}/log_${supp_threshold}_${conf_threshold}.txt

# python eval.py --dataset FB15k --rules out/FB15k/rules_20_0.txt --ranking_file out/FB15k/ranking_20_0.txt > out/FB15k/log_20_0.txt