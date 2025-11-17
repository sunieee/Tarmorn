@echo off
setlocal enabledelayedexpansion

REM Configuration
set dataset=FB15k-237
set split=louvain

REM Create output directories
if not exist "out\%dataset%\%split%\atom2formula2metric" mkdir "out\%dataset%\%split%\atom2formula2metric"
if not exist "out\%dataset%\%split%\rules" mkdir "out\%dataset%\%split%\rules"
if not exist "out\%dataset%\%split%\log" mkdir "out\%dataset%\%split%\log"

REM Loop through all .tsv files in the partitions directory
for %%f in (out\%dataset%\%split%\partitions\*.tsv) do (
    set filename=%%~nf
    echo.
    echo ================================================================================
    echo Processing partition: !filename!
    echo Log file: out\%dataset%\%split%\log\!filename!.log
    echo ================================================================================
    
    REM Set environment variables and run Maven with log redirection
    set DATASET=%dataset%
    set PATH_TRAINING=out\%dataset%\%split%\partitions\!filename!.tsv
    set PATH_RULES_JSON=out\%dataset%\%split%\atom2formula2metric\!filename!.json
    set PATH_RULES_TXT=out\%dataset%\%split%\rules\!filename!.txt
    set MAVEN_OPTS=-Xms48g -Xmx48g -XX:MaxMetaspaceSize=2g
    
    REM Run Maven with output redirected to log file
    mvn exec:java -Dexec.mainClass="tarmorn.TLearn" > "out\%dataset%\%split%\log\!filename!.log" 2>&1
    
    if errorlevel 1 (
        echo ERROR: Failed to process partition !filename!
        echo Check out\%dataset%\%split%\log\!filename!.log for details
        echo Continuing with next partition...
    ) else (
        echo Successfully completed partition: !filename!
    )
)

echo.
echo ================================================================================
echo All partitions processed
echo ================================================================================

endlocal 

python merge_rules.py "out\%dataset%\%split%\atom2formula2metric" > "out\%dataset%\%split%\merge_rules.log"

python eval.py --dataset %dataset% --rules "out\%dataset%\%split%\rule.txt" --ranking_file "out\%dataset%\%split%\eval.txt" > "out\%dataset%\%split%\eval.log"
