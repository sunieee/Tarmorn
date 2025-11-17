@echo off
setlocal enabledelayedexpansion

REM Configuration
if not defined dataset set dataset=FB15k-237
if not defined split set split=louvain

REM Create output directories
if not exist "out\%dataset%\%split%\atom2formula2metric" mkdir "out\%dataset%\%split%\atom2formula2metric"
if not exist "out\%dataset%\%split%\rules" mkdir "out\%dataset%\%split%\rules"
if not exist "out\%dataset%\%split%\log" mkdir "out\%dataset%\%split%\log"

REM Loop through all .tsv files in the partitions directory
for %%f in (out\%dataset%\%split%\partitions\*.tsv) do (
    echo ================================================================================
    echo Processing partition: %%~nf
    echo Log file: out\%dataset%\%split%\log\%%~nf.log
    echo ================================================================================
    
    REM Set environment variables and run Maven with log redirection
    set DATASET=%dataset%
    set PATH_TRAINING=out\%dataset%\%split%\partitions\%%~nf.tsv
    set PATH_RULES_JSON=out\%dataset%\%split%\atom2formula2metric\%%~nf.json
    set PATH_RULES_TXT=out\%dataset%\%split%\rules\%%~nf.txt
    set MAVEN_OPTS=-Xms48g -Xmx48g -XX:MaxMetaspaceSize=2g
    
    REM Run Maven with output redirected to log file
    mvn exec:java -Dexec.mainClass="tarmorn.TLearn" > "out\%dataset%\%split%\log\%%~nf.log" 2>&1
)

echo.
echo ================================================================================
echo All partitions processed
echo ================================================================================

endlocal 

python merge_rules.py "out\%dataset%\%split%\atom2formula2metric" > "out\%dataset%\%split%\merge_rules.log"

python eval.py --dataset %dataset% --rules "out\%dataset%\%split%\rule.txt" --ranking_file "out\%dataset%\%split%\eval.txt" > "out\%dataset%\%split%\eval.log"