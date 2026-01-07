@echo off
setlocal enabledelayedexpansion

REM Configuration
@REM set dataset=FB15k-237
@REM set dataset=WN18RR
set dataset=codex-m
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
call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --disable_combo > out/%dataset%/eval-maxplus.log
call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor > out/%dataset%/eval-noisyor.log
call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor --combo_noisyor_method greed > out/%dataset%/eval-noisyor-greed.log
call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --jaccard_file out/%dataset%/atomPair2Jaccard.json --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor > out/%dataset%/eval-noisyor-cluster.log
call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --jaccard_file out/%dataset%/atomPair2Jaccard.json --ranking_file out/%dataset%/eval.txt --aggregation_function noisyor --combo_noisyor_method greed > out/%dataset%/eval-noisyor-greed-cluster.log
endlocal 

@REM set dataset=FB15k-237
