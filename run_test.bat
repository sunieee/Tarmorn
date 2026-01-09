@echo off
setlocal enabledelayedexpansion

REM Configuration
set dataset=FB15k-237
@REM set dataset=WN18RR
@REM set dataset=codex-m
@REM set dataset=codex-s
mkdir out\%dataset%

REM Loop through all .tsv files in the partitions directory
echo Log file: out\%dataset%\run.log

REM Set environment variables and run Maven with log redirection
set MAVEN_OPTS=-Xms48g -Xmx48g -XX:MaxMetaspaceSize=2g

REM Run Maven with output redirected to log file
call mvn clean compile
call mvn exec:java -Dexec.mainClass="tarmorn.TLearn" > "out\%dataset%\run.log" 2>&1

@REM call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --disable_combo > out/%dataset%/eval.log
@REM call python script\compare_rules.py --dataset %dataset%

@REM call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt > out/%dataset%/eval-maxplus-combo.log
@REM call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --disable_combo > out/%dataset%/eval-maxplus.log
@REM call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor > out/%dataset%/eval-noisyor.log
@REM call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor --combo_noisyor_method greed > out/%dataset%/eval-noisyor-greed.log
@REM call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --jaccard_file out/%dataset%/atomPair2Jaccard.json --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor > out/%dataset%/eval-noisyor-cluster.log
@REM call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --jaccard_file out/%dataset%/atomPair2Jaccard.json --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor --combo_noisyor_method greed > out/%dataset%/eval-noisyor-greed-cluster.log

@REM call python eval.py --dataset %dataset% --test_valid_split RP --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor > out/%dataset%/eval-RP-noisyor.log
@REM call python eval.py --dataset %dataset% --test_valid_split RP --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor --combo_noisyor_method greed > out/%dataset%/eval-RP-noisyor-greed.log

@REM call python evaltc.py --dataset %dataset% --rules out/%dataset%/rule.txt --disable_combo > out/%dataset%/evaltc-maxplus.log
@REM call python evaltc.py --dataset %dataset% --rules out/%dataset%/rule.txt --aggregation_function noisyor > out/%dataset%/evaltc-noisyor.log
@REM call python evaltc.py --dataset %dataset% --rules out/%dataset%/rule.txt --aggregation_function noisyor --combo_noisyor_method greed > out/%dataset%/evaltc-noisyor-greed.log
@REM call python evaltc.py --dataset %dataset% --rules out/%dataset%/rule.txt --jaccard_file out/%dataset%/atomPair2Jaccard.json --aggregation_function noisyor > out/%dataset%/evaltc-noisyor-cluster.log
@REM call python evaltc.py --dataset %dataset% --rules out/%dataset%/rule.txt --jaccard_file out/%dataset%/atomPair2Jaccard.json --aggregation_function noisyor --combo_noisyor_method greed > out/%dataset%/evaltc-noisyor-greed-cluster.log
endlocal 

@REM set dataset=FB15k-237
