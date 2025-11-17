@echo off
setlocal enabledelayedexpansion

set dataset=FB15k-237
@REM set splits=hub_replication_t50 louvain bfs_expansion_r2 edge_cut random_multi_k2 random_multi_k3 random_nonoverlap relation_centric vertex_cut
set splits=random_nonoverlap random_multi_k2 random_multi_k3 edge_cut vertex_cut louvain hub_replication_t50 bfs_expansion_r2 relation_centric

echo ################################################################################
echo Starting batch processing for dataset: %dataset%
echo Split methods: %splits%
echo ################################################################################

REM Iterate through all split methods
for %%s in (%splits%) do (
echo.
echo ################################################################################
echo Processing split: %%s
echo ################################################################################

REM Call run.bat with environment variables (no spaces around &&)
cmd /c "set dataset=%dataset%&& set split=%%s&& run.bat"

python merge_rules.py "out\%dataset%\%%s\atom2formula2metric" > "out\%dataset%\%%s\merge_rules.log"

python eval.py --dataset %dataset% --rules "out\%dataset%\%%s\rule.txt" --ranking_file "out\%dataset%\%%s\eval.txt" > "out\%dataset%\%%s\eval.log"

if errorlevel 1 (
echo ERROR: Failed to process split %%s
echo Continuing with next split...
) else (
echo Successfully completed split: %%s
)

echo.
)

echo.
echo ################################################################################
echo All split methods completed for %dataset%
echo ################################################################################

endlocal
