import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from xgboost import XGBRanker

from train_xgb_pairwise import FEATURE_COLUMNS


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply XGBoost model and compute MRR")
    parser.add_argument("--dataset", type=str, default="FB15k-237", help="dataset name")
    parser.add_argument(
        "--csv",
        type=str,
        default="",
        help="path to dependency_graph.csv (default: out/{dataset}/dependency_graph.csv)",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="",
        help="path to xgboost model (default: same folder as csv, dependency_graph.json)",
    )
    parser.add_argument(
        "--test",
        type=str,
        default="",
        help="path to test.txt (default: data/{dataset}/test.txt)",
    )
    return parser.parse_args()


def _count_lines(path: Path) -> int:
    with path.open("r", encoding="utf-8") as f:
        return sum(1 for _ in f)


def main() -> None:
    args = _parse_args()
    csv_path = Path(args.csv) if args.csv else Path(f"out/{args.dataset}/dependency_graph.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    model_path = (
        Path(args.model)
        if args.model
        else csv_path.with_suffix(".json")
    )
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")

    test_path = Path(args.test) if args.test else Path(f"data/{args.dataset}/test.txt")
    if not test_path.exists():
        raise FileNotFoundError(f"Test file not found: {test_path}")

    df = pd.read_csv(csv_path)
    required_cols = {"query", "candidate", "label"} | set(FEATURE_COLUMNS)
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns in CSV: {sorted(missing_cols)}")

    df = df.copy()
    df["label"] = pd.to_numeric(df["label"], errors="coerce").fillna(0).astype(int)

    X = df[FEATURE_COLUMNS]

    model = XGBRanker()
    model.load_model(str(model_path))
    scores = model.predict(X)
    df["score"] = scores

    total = 0.0
    for _, group in df.groupby("query"):
        gt = group[group["label"] == 1]
        if gt.empty:
            continue
        ranked = group.sort_values("score", ascending=False).reset_index(drop=True)
        gt_indices = ranked.index[ranked["label"] == 1].to_numpy()
        if gt_indices.size == 0:
            continue
        rank = int(gt_indices.min()) + 1
        total += 1.0 / rank

    denom = _count_lines(test_path)
    mrr = total / denom if denom > 0 else 0.0

    print(f"MRR: {mrr:.6f}")
    print(f"MRR numerator (sum of reciprocal ranks): {total:.6f}")
    print(f"MRR denominator (test lines): {denom}")


if __name__ == "__main__":
    main()
