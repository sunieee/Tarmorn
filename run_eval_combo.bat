@echo off
echo Running eval.py with different combo_max_depth and combo_max_branch values > eval-test.log

for %%d in (2 3) do (
    for %%b in (2 3) do (
        echo. >> eval-test.log
        echo === Testing depth=%%d, branch=%%b === >> eval-test.log
        echo. >> eval-test.log
        python eval.py --dataset FB15k-237 --rules out/FB15k-237/rule.txt --ranking_file out/FB15k-237/eval.txt --combo_max_depth %%d --combo_max_branch %%b 2>&1 >> eval-test.log
    )
)

echo.
echo All tests completed! Results saved to eval-test.log
