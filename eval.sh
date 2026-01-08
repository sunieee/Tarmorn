dataset=FB15k
rule_file=preds-100-400
supp_threshold=20
conf_threshold=0

awk -v supp="$supp_threshold" -v conf="$conf_threshold" '$2 >= supp && $3+0 >= conf {print; count++} END {print "Total:", count > "/dev/stderr"}' "out/${dataset}/${rule_file}" \
    > "out/${dataset}/rules_${supp_threshold}_${conf_threshold}.txt"

# python eval.py --dataset $dataset --rules out/${dataset}/rules_${supp_threshold}_${conf_threshold}.txt \
#     --ranking_file out/${dataset}/ranking_${supp_threshold}_${conf_threshold}.txt \
#     > out/${dataset}/log_${supp_threshold}_${conf_threshold}.txt

python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt --combo_debug > out/FB15k-237/eval.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule-common.txt --ranking_file out/FB15k-237/eval.txt  > out/FB15k-237/eval-common.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt --disable_u_d --disable_u_c --disable_u_xxc --disable_u_xxd --disable_zero > out/FB15k-237/eval-b.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt --disable_u_d --disable_b --disable_u_xxc --disable_u_xxd --disable_zero > out/FB15k-237/eval-u_c.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt --disable_u_c --disable_b --disable_u_xxc --disable_u_xxd --disable_zero > out/FB15k-237/eval-u_d.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt --disable_b --disable_u_c --disable_u_d > out/FB15k-237/eval-other.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt --disable_u_xxc --disable_u_xxd --disable_zero > out/FB15k-237/eval-main.log

python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule-test.txt --ranking_file out/FB15k-237/eval.txt --combo_debug > out/FB15k-237/eval-combo_debug.log

python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt --disable_u_d --disable_u_c --disable_u_xxc --disable_u_xxd --disable_zero


java -Xmx64G -cp AnyBURL-23-1.jar de.unima.ki.anyburl.Learn config-learn.properties

python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100 --ranking_file out/FB15k-237/eval-baseline.txt > out/FB15k-237/eval-baseline.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10 --ranking_file out/FB15k-237/eval-10.txt > out/FB15k-237/eval-10.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-20 --ranking_file out/FB15k-237/eval-20.txt > out/FB15k-237/eval-20.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-40 --ranking_file out/FB15k-237/eval-40.txt > out/FB15k-237/eval-40.log

python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10 --ranking_file out/FB15k-237/eval-10.txt --disable_u_d --disable_u_c --disable_u_xxc --disable_u_xxd --disable_zero > out/FB15k-237/eval-10-b.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10 --ranking_file out/FB15k-237/eval-10.txt --disable_u_d --disable_b --disable_u_xxc --disable_u_xxd --disable_zero > out/FB15k-237/eval-10-u_c.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10 --ranking_file out/FB15k-237/eval-10.txt --disable_u_c --disable_b --disable_u_xxc --disable_u_xxd --disable_zero > out/FB15k-237/eval-10-u_d.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10 --ranking_file out/FB15k-237/eval-10.txt --disable_b --disable_u_c --disable_u_d > out/FB15k-237/eval-10-other.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10 --ranking_file out/FB15k-237/eval-10.txt --disable_u_xxc --disable_u_xxd --disable_zero > out/FB15k-237/eval-10-main.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10 --ranking_file out/FB15k-237/eval-10.txt --b_max_length 2 > out/FB15k-237/eval-10-b2.log
python eval.py --dataset FB15k-237 --rules out/FB15k-237/rules-100-10-common --ranking_file out/FB15k-237/eval-10.txt --b_max_length 2 > out/FB15k-237/eval-10-common.log

python evaltc.py --dataset FB15k-237 --rules out/FB15k-237/rules-100 > out/FB15k-237/evaltc-baseline-maxplus.log
python evaltc.py --dataset FB15k-237 --rules out/FB15k-237/rules-100 --aggregation_function noisyor > out/FB15k-237/evaltc-baseline-noisyor.log