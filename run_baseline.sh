# export dataset=FB15k-237
# export dataset=KG20C
export dataset=YAGO3-10

mkdir -p baseline/$dataset
java -Xmx240G -cp AnyBURL-23-1x.jar de.unima.ki.anyburl.Learn config-learn.properties

# python eval.py --dataset "${dataset}" --rules "baseline/${dataset}/rules-400" --ranking_file "baseline/${dataset}/eval.txt" --aggregation_function noisyor > "baseline/${dataset}/eval-400-noisyor.log"
# python eval.py --dataset "${dataset}" --rules "baseline/${dataset}/rules-400" --ranking_file "baseline/${dataset}/eval.txt" --aggregation_function maxplus > "baseline/${dataset}/eval-400-maxplus.log"

python eval.py --dataset "${dataset}" --rules "baseline/${dataset}/rules-1000" --ranking_file "baseline/${dataset}/eval.txt" --aggregation_function noisyor > "baseline/${dataset}/eval-1000-noisyor.log"
python eval.py --dataset "${dataset}" --rules "baseline/${dataset}/rules-1000" --ranking_file "baseline/${dataset}/eval.txt" --aggregation_function maxplus > "baseline/${dataset}/eval-1000-maxplus.log"