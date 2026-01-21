import argparse
import json
import itertools
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, recall_score, roc_auc_score
from xgboost import XGBRanker


BASE_COLUMNS = ["relation", "constant", "candidate", "label", "if_head"]

PARAM_GRID = {
    "max_depth": [4, 6, 8],
    "n_estimators": [200, 400, 600],
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train XGBoost pairwise ranker")
    parser.add_argument("--dataset", type=str, default="FB15k-237", help="dataset name")
    parser.add_argument(
        "--no_eval",
        action="store_true",
        help="disable AUC evaluation after training",
    )
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument("--num_boost_round", type=int, default=400, help="boosting rounds")
    parser.add_argument("--learning_rate", type=float, default=0.05, help="eta")
    parser.add_argument("--max_depth", type=int, default=6, help="max tree depth")
    parser.add_argument("--min_child_weight", type=float, default=1.0, help="min child weight")
    parser.add_argument("--subsample", type=float, default=0.9, help="row subsample")
    parser.add_argument("--colsample_bytree", type=float, default=0.9, help="feature subsample")
    parser.add_argument(
        "--ranker_objective",
        type=str,
        default="rank:pairwise",
        help="ranking objective",
    )
    parser.add_argument(
        "--no_grid_search",
        action="store_true",
        help="disable grid search and use provided hyperparameters",
    )
    parser.add_argument(
        "--val_ratio",
        type=float,
        default=0.1,
        help="validation ratio by query (0.1 or 0.2 recommended)",
    )
    parser.add_argument(
        "--grid_repeats",
        type=int,
        default=2,
        help="repeat count for each parameter set",
    )
    parser.add_argument(
        "--verbose_eval",
        type=int,
        default=50,
        help="print training progress every N rounds (0 to disable)",
    )
    return parser.parse_args()


def _split_queries(queries: np.ndarray, val_ratio: float, seed: int) -> tuple[set, set]:
    rng = np.random.default_rng(seed)
    shuffled = queries.copy()
    rng.shuffle(shuffled)
    split = int(len(shuffled) * (1.0 - val_ratio))
    train_q = set(shuffled[:split])
    val_q = set(shuffled[split:])
    return train_q, val_q


def _iter_grid(grid: dict) -> list[dict]:
    keys = list(grid.keys())
    if not keys:
        return [{}]
    values = [grid[k] for k in keys]
    return [dict(zip(keys, combo)) for combo in itertools.product(*values)]


def main() -> None:
    args = _parse_args()
    csv_path = Path(f"out/{args.dataset}/dependency_graph_valid.csv")
    model_path = Path(f"out/{args.dataset}/xgb_ranker.json")
    params_path = csv_path.with_name("xgb_ranker_best_params.json")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    required_cols = set(BASE_COLUMNS)
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns in CSV: {sorted(missing_cols)}")

    if "if_head" not in df.columns:
        raise ValueError("Missing 'if_head' column in CSV")

    if_head_idx = list(df.columns).index("if_head")
    feature_columns = list(df.columns)[if_head_idx:]
    if not feature_columns:
        raise ValueError("No feature columns found after 'if_head'")

    # Keep only rows with valid labels and numeric features
    df = df.copy()
    df["label"] = pd.to_numeric(df["label"], errors="coerce").fillna(0).astype(int)
    df[feature_columns] = df[feature_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    # Query is defined by (relation, constant, if_head)
    df["query"] = (
        df["relation"].astype(str)
        + "\t"
        + df["constant"].astype(str)
        + "\t"
        + df["if_head"].astype(str)
    )

    # Sort by query so group sizes align with training API
    df = df.sort_values("query").reset_index(drop=True)

    X = df[feature_columns]
    y = df["label"].values
    group_sizes = df.groupby("query").size().values

    base_params = {
        "objective": args.ranker_objective,
        "n_estimators": args.num_boost_round,
        "learning_rate": args.learning_rate,
        "max_depth": args.max_depth,
        "min_child_weight": args.min_child_weight,
        "subsample": args.subsample,
        "colsample_bytree": args.colsample_bytree,
        "random_state": args.seed,
        "eval_metric": "ndcg",
        "tree_method": "hist",
    }

    best_params = base_params.copy()
    best_auc = -1.0

    if not args.no_grid_search:
        queries = df["query"].unique()
        grid_list = _iter_grid(PARAM_GRID)
        print(f"Grid search: {len(grid_list)} parameter sets")
        for idx, grid_params in enumerate(grid_list, start=1):
            aucs = []
            for rep in range(args.grid_repeats):
                train_q, val_q = _split_queries(queries, args.val_ratio, args.seed + rep)
                train_mask = df["query"].isin(train_q)
                val_mask = df["query"].isin(val_q)
                df_train = df[train_mask]
                df_val = df[val_mask]
                if len(df_val) == 0 or len(df_train) == 0:
                    aucs.append(np.nan)
                    continue

                X_train = df_train[feature_columns]
                y_train = df_train["label"].values
                train_groups = df_train.groupby("query").size().values

                X_val = df_val[feature_columns]
                y_val = df_val["label"].values
                val_groups = df_val.groupby("query").size().values

                if len(np.unique(y_val)) < 2:
                    aucs.append(np.nan)
                    continue

                params = base_params.copy()
                params.update(grid_params)
                ranker = XGBRanker(**params)
                ranker.fit(
                    X_train,
                    y_train,
                    group=train_groups,
                    eval_set=[(X_val, y_val)],
                    eval_group=[val_groups],
                    verbose=False,
                )
                scores = ranker.predict(X_val)
                aucs.append(roc_auc_score(y_val, scores))

            mean_auc = float(np.nanmean(aucs)) if np.any(~np.isnan(aucs)) else float("-inf")
            print(
                f"[{idx}/{len(grid_list)}] params={grid_params} "
                f"mean_auc={mean_auc:.6f}"
            )
            if mean_auc > best_auc:
                best_auc = mean_auc
                best_params = base_params.copy()
                best_params.update(grid_params)

        with open(params_path, "w", encoding="utf-8") as f:
            json.dump(best_params, f, ensure_ascii=False, indent=2)
        print(f"Best params saved to: {params_path}")

    # XGBoost pairwise ranker (final training)
    ranker = XGBRanker(**best_params)

    ranker.fit(
        X,
        y,
        group=group_sizes,
        eval_set=[(X, y)],
        eval_group=[group_sizes],
        verbose=args.verbose_eval if args.verbose_eval > 0 else False,
    )

    if not args.no_eval:
        scores = ranker.predict(X)
        if len(np.unique(y)) > 1:
            overall_auc = roc_auc_score(y, scores)
            print(f"Overall AUC: {overall_auc:.6f}")
        else:
            print("Overall AUC: N/A (only one class present)")

        per_query_auc = []
        for _, group in df.assign(score=scores).groupby("query"):
            labels = group["label"].values
            if len(np.unique(labels)) < 2:
                continue
            per_query_auc.append(roc_auc_score(labels, group["score"].values))

        if per_query_auc:
            print(
                "Per-query AUC: "
                f"mean={float(np.mean(per_query_auc)):.6f}, "
                f"median={float(np.median(per_query_auc)):.6f}, "
                f"count={len(per_query_auc)}"
            )
        else:
            print("Per-query AUC: N/A (no query has both classes)")

    ranker.save_model(str(model_path))

    print(f"Model saved to: {model_path}")


if __name__ == "__main__":
    main()
