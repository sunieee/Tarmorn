dataset=FB15k
rule_file=preds-100-400
supp_threshold=20
conf_threshold=0

awk -v supp="$supp_threshold" -v conf="$conf_threshold" '$2 >= supp && $3+0 >= conf {print; count++} END {print "Total:", count > "/dev/stderr"}' "out/${dataset}/${rule_file}" \
    > "out/${dataset}/rules_${supp_threshold}_${conf_threshold}.txt"

# python eval.py --dataset $dataset --rules out/${dataset}/rules_${supp_threshold}_${conf_threshold}.txt \
#     --ranking_file out/${dataset}/ranking_${supp_threshold}_${conf_threshold}.txt \
#     > out/${dataset}/log_${supp_threshold}_${conf_threshold}.txt

python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt > out/FB15k-237/eval.log


java -Xmx64G -cp AnyBURL-23-1.jar de.unima.ki.anyburl.Learn config-learn.properties
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100 --ranking_file out/FB15k-237/eval-baseline.txt > out/FB15k-237/eval-baseline.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10 --ranking_file out/FB15k-237/eval-10.txt > out/FB15k-237/eval-10.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-20 --ranking_file out/FB15k-237/eval-20.txt > out/FB15k-237/eval-20.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-40 --ranking_file out/FB15k-237/eval-40.txt > out/FB15k-237/eval-40.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-50 --ranking_file out/FB15k-237/eval-50.txt > out/FB15k-237/eval-50.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-filtered --ranking_file out/FB15k-237/eval-filtered.txt > out/FB15k-237/eval-filtered.log