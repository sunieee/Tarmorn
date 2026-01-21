import argparse
from pathlib import Path

import pandas as pd
from xgboost import XGBRanker

from clause import Ranking, TripleSet


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate XGBoost ranker with PyClause")
    parser.add_argument("--dataset", type=str, default="FB15k-237", help="dataset name")
    parser.add_argument(
        "--csv",
        type=str,
        default="",
        help="dependency_graph.csv path (default: out/<dataset>/dependency_graph.csv)",
    )
    parser.add_argument("--valid", action="store_true", help="use valid set for evaluation")
    parser.add_argument("--test_valid_split", type=str, default="", help="split suffix")
    parser.add_argument("--topk", type=int, default=100, help="top-k candidates")
    parser.add_argument("--ranking_threads", type=int, default=-1, help="ranking threads")
    return parser.parse_args()


def _build_rankings(df: pd.DataFrame, score_col: str, topk: int) -> tuple[dict, dict]:
    head_ranking: dict[str, dict[str, list[tuple[str, float]]]] = {}
    tail_ranking: dict[str, dict[str, list[tuple[str, float]]]] = {}

    for (rel, constant, if_head), group in df.groupby(["relation", "constant", "if_head"]):
        ranked = group.sort_values(score_col, ascending=False)
        if topk > 0:
            ranked = ranked.head(topk)
        pairs = list(
            zip(
                ranked["candidate"].astype(str).tolist(),
                ranked[score_col].astype(float).tolist(),
            )
        )
        rel = str(rel)
        constant = str(constant)
        if int(if_head) == 1:
            head_ranking.setdefault(rel, {}).setdefault(constant, []).extend(pairs)
        else:
            tail_ranking.setdefault(rel, {}).setdefault(constant, []).extend(pairs)

    return head_ranking, tail_ranking


def _summarize_ranking(ranking: dict[str, dict[str, list[tuple[str, float]]]]) -> tuple[int, int, int]:
    relation_count = len(ranking)
    query_count = 0
    candidate_total = 0
    for queries in ranking.values():
        query_count += len(queries)
        for candidates in queries.values():
            candidate_total += len(candidates)
    return relation_count, query_count, candidate_total


def _summarize_df(df: pd.DataFrame) -> tuple[tuple[int, int, int], tuple[int, int, int]]:
    head_df = df[df["if_head"].astype(int) == 1]
    tail_df = df[df["if_head"].astype(int) != 1]

    def _summary(part: pd.DataFrame) -> tuple[int, int, int]:
        relation_count = part["relation"].astype(str).nunique()
        query_count = part[["relation", "constant"]].astype(str).drop_duplicates().shape[0]
        candidate_total = len(part)
        return relation_count, query_count, candidate_total

    return _summary(head_df), _summary(tail_df)


def main() -> None:
    args = _parse_args()
    dataset = args.dataset

    print("[1/9] Parsed arguments")

    csv_path = Path(args.csv) if args.csv else Path(f"out/{dataset}/dependency_graph.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    print(f"[2/9] CSV path: {csv_path}")

    model_path = Path(f"out/{dataset}/xgb_ranker.json")
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    print(f"[3/9] Model path: {model_path}")

    if args.valid:
        target = f"data/{dataset}/valid{args.test_valid_split}.txt"
    else:
        target = f"data/{dataset}/test{args.test_valid_split}.txt"
    print(f"[4/9] Target set: {target}")

    testset = TripleSet(target)
    print(f"[5/9] Loaded target triples: {len(testset.triples)}")

    print("[6/9] Reading CSV...")
    df = pd.read_csv(csv_path)
    print(f"[6/9] CSV rows: {len(df)}")
    required_cols = {"relation", "constant", "candidate", "if_head"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns in CSV: {sorted(missing_cols)}")

    if_head_idx = list(df.columns).index("if_head")
    feature_columns = list(df.columns)[if_head_idx:]
    if not feature_columns:
        raise ValueError("No feature columns found from 'if_head' onwards")

    df = df.copy()
    df[feature_columns] = df[feature_columns].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    print(f"[7/9] Feature columns: {len(feature_columns)}")

    model = XGBRanker()
    model.load_model(str(model_path))
    print("[8/9] Model loaded, predicting scores...")
    df["score"] = model.predict(df[feature_columns])

    print("[9/9] Building rankings and computing metrics...")
    head_ranking, tail_ranking = _build_rankings(df, "score", args.topk)
    head_df_stats, tail_df_stats = _summarize_df(df)
    print(
        "CSV totals (no topk) - Head: relations={0}, queries={1}, candidates={2}".format(
            *head_df_stats
        )
    )
    print(
        "CSV totals (no topk) - Tail: relations={0}, queries={1}, candidates={2}".format(
            *tail_df_stats
        )
    )
    head_rel, head_query, head_cand = _summarize_ranking(head_ranking)
    tail_rel, tail_query, tail_cand = _summarize_ranking(tail_ranking)
    print(
        "Head ranking: relations={0}, queries={1}, candidates={2}".format(
            head_rel, head_query, head_cand
        )
    )
    print(
        "Tail ranking: relations={0}, queries={1}, candidates={2}".format(
            tail_rel, tail_query, tail_cand
        )
    )

    ranking = Ranking(k=args.topk)
    ranking.convert_handler_ranking(head_ranking, tail_ranking, testset)
    ranking.compute_scores(testset.triples)

    print("*** EVALUATION RESULTS ****")
    print("Num triples: " + str(len(testset.triples)))
    print("MRR     " + "{0:.6f}".format(ranking.hits.get_mrr()))
    print("hits@1  " + "{0:.6f}".format(ranking.hits.get_hits_at_k(1)))
    print("hits@3  " + "{0:.6f}".format(ranking.hits.get_hits_at_k(3)))
    print("hits@10 " + "{0:.6f}".format(ranking.hits.get_hits_at_k(10)))


if __name__ == "__main__":
    main()