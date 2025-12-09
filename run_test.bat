@echo off
setlocal enabledelayedexpansion

REM Configuration
if not defined dataset set dataset=FB15k-237


REM Loop through all .tsv files in the partitions directory
echo Log file: out\%dataset%\run.log

REM Set environment variables and run Maven with log redirection
set MAVEN_OPTS=-Xms48g -Xmx48g -XX:MaxMetaspaceSize=2g

REM Run Maven with output redirected to log file
call mvn clean compile
call mvn exec:java -Dexec.mainClass="tarmorn.TLearn" > "out\%dataset%\run.log" 2>&1

call python eval.py --dataset %dataset% --rules out/%dataset%/rule.txt --ranking_file out/%dataset%/eval.txt > out/%dataset%/eval.log

@REM call python script\compare_rules.py --dataset %dataset%

endlocal 