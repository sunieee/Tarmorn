dataset=FB15k
rule_file=preds-100-400
supp_threshold=20
conf_threshold=0

awk -v supp="$supp_threshold" -v conf="$conf_threshold" '$2 >= supp && $3+0 >= conf {print; count++} END {print "Total:", count > "/dev/stderr"}' "out/${dataset}/${rule_file}" \
    > "out/${dataset}/rules_${supp_threshold}_${conf_threshold}.txt"

# python eval.py --dataset $dataset --rules out/${dataset}/rules_${supp_threshold}_${conf_threshold}.txt \
#     --ranking_file out/${dataset}/ranking_${supp_threshold}_${conf_threshold}.txt \
#     > out/${dataset}/log_${supp_threshold}_${conf_threshold}.txt

python eval.py --dataset FB15k-237 --rules out/FB237/rule.txt --ranking_file out/FB237/eval.txt > out/FB237/eval.log