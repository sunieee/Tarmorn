import json
from c_clause import RankingHandler, Loader
from clause import Options

from clause import Ranking
from clause import TripleSet

import argparse
import os
from datetime import datetime


def _summarize_ranking(ranking: dict) -> tuple[int, int, int]:
    relation_count = len(ranking)
    query_count = 0
    candidate_total = 0
    for queries in ranking.values():
        query_count += len(queries)
        for candidates in queries.values():
            candidate_total += len(candidates)
    return relation_count, query_count, candidate_total

# *** Example Evaluation ***

## This example illustrates how to create a ranking from a rule set that has been created previously.
## The ranking is evaluated on the fly before storing it on disc.
## The example shows also at the end how to use a few lines of code to create a
## structured results table that informs about relation and direction specific MRR and hits scores.

argparser = argparse.ArgumentParser(description="Example for evaluation of a ranking")
argparser.add_argument("--dataset", type=str, default="wnrr", help="dataset to use")
argparser.add_argument("--rules", type=str, default="", help="rules to use")
argparser.add_argument("--applied_rules", type=str, default="", help="rules to use")
argparser.add_argument("--ranking_file", type=str, default="", help="rules to use")
argparser.add_argument("--ranking_dump", type=str, default="", help="dump head/tail ranking to JSON")
argparser.add_argument("--aggregation_function", type=str, default="noisyor", help="aggregation function to use")
argparser.add_argument("--disable_b", action="store_true", help="whether to disable b rules")
argparser.add_argument("--disable_combo", action="store_true", help="whether to disable combo rules")
argparser.add_argument("--disable_u_d", action="store_true", help="whether to disable u_d rules")
argparser.add_argument("--disable_u_c", action="store_true", help="whether to disable u_c rules")
argparser.add_argument("--disable_zero", action="store_true", help="whether to disable zero rules")
argparser.add_argument("--disable_u_xxc", action="store_true", help="whether to disable u_xxc rules")
argparser.add_argument("--disable_u_xxd", action="store_true", help="whether to disable u_xxd rules")
argparser.add_argument("--valid", action="store_true", help="whether to use valid set for evaluation")
argparser.add_argument("--debug", action="store_true", help="whether to disable u_xxd rules")
argparser.add_argument("--b_max_length", type=int, default=-1, help="whether to disable u_xxd rules")
argparser.add_argument("--num_unseen", type=int, default=5, help="whether to disable u_xxd rules")
argparser.add_argument("--d_weight", type=float, default=0.1, help="whether to disable u_xxd rules")
argparser.add_argument("--z_weight", type=float, default=0.01, help="whether to disable u_xxd rules")
argparser.add_argument("--test_valid_split", type=str, default="", help="whether to disable u_xxd rules")
argparser.add_argument("--positive_weight", type=float, default=0.0, help="whether to disable u_xxd rules")
argparser.add_argument("--negative_weight", type=float, default=0.0, help="whether to disable u_xxd rules")

argparser.add_argument("--loader_threads", type=int, default=os.cpu_count(), help="whether to disable u_xxd rules")
argparser.add_argument("--ranking_threads", type=int, default=-1, help="whether to disable u_xxd rules")

args = argparser.parse_args()
start_time = datetime.now()
dataset = args.dataset

train = f"data/{dataset}/train.txt"

if args.valid:
    filter_set = ""
    target = f"data/{dataset}/valid{args.test_valid_split}.txt"
else:
    filter_set = f"data/{dataset}/valid{args.test_valid_split}.txt"
    target = f"data/{dataset}/test{args.test_valid_split}.txt"

# rules = f"{get_base_dir()}/data/rules/{dataset}.txt"
rules = args.rules if args.rules else f"data/rules/{dataset}.txt"
ranking_file = args.ranking_file if args.ranking_file else f"local/ranking-{dataset}.txt"

options = Options()
options.set("ranking_handler.aggregation_function", args.aggregation_function)
options.set("loader.load_b_rules", not args.disable_b)
options.set("loader.load_zero_rules", not args.disable_zero)
options.set("loader.load_u_d_rules", not args.disable_u_d)
options.set("loader.load_u_c_rules", not args.disable_u_c)
options.set("loader.load_u_xxc_rules", not args.disable_u_xxc)
options.set("loader.load_u_xxd_rules", not args.disable_u_xxd)

options.set("loader.b_max_length", args.b_max_length)
options.set("loader.num_unseen", args.num_unseen)
options.set("loader.d_weight", args.d_weight)

# *** 关键：设置线程数 ***
options.set("ranking_handler.num_threads", args.ranking_threads)  
options.set("loader.num_threads", args.loader_threads)           # 指定4个线程用于规则加载
if args.applied_rules:
    options.set("ranking_handler.collect_rules", True)

#### Calculate a ranking
loader = Loader(options=options.get("loader"))
loader.load_data(data=train, filter=filter_set, target=target)
loader.load_rules(rules=rules)

# ComboHandler 配置现在由 Loader 管理，不再需要手动合并选项
# RankingHandler, QAHandler, PredictionHandler 都会从 Loader 获取相同的 combo 配置
ranker = RankingHandler(options=options.get("ranking_handler"))
ranker.calculate_ranking(loader=loader)
headRanking = ranker.get_ranking(direction="head", as_string=True)
tailRanking = ranker.get_ranking(direction="tail", as_string=True)

head_rel, head_query, head_cand = _summarize_ranking(headRanking)
tail_rel, tail_query, tail_cand = _summarize_ranking(tailRanking)
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

# 保存 ranking 到文件（可用于与 eval_base_ranker 对比）
if args.ranking_dump:
    dump_obj = {"head": headRanking, "tail": tailRanking}
    with open(args.ranking_dump, "w", encoding="utf-8") as f:
        json.dump(dump_obj, f, ensure_ascii=False)


# 保存applied_rules到文件
if args.applied_rules:
    headRules = ranker.get_applied_rules(direction="head")
    tailRules = ranker.get_applied_rules(direction="tail")
    output = json.dumps({"head": headRules, "tail": tailRules}, ensure_ascii=False, indent=2)
    with open(args.applied_rules, 'w', encoding='utf-8') as f:
        f.write(output)


testset = TripleSet(target)
ranking = Ranking(k=100)
# process the handler ranking which is defined on queries and not
# on triples, e.g. assign to every triple of 'testset' the corresponding query rankings
ranking.convert_handler_ranking(headRanking, tailRanking, testset)
ranking.compute_scores(testset.triples)


print("*** EVALUATION RESULTS ****")
print("Num triples: " + str(len(testset.triples)))
print("MRR     " + '{0:.6f}'.format(ranking.hits.get_mrr()))
print("hits@1  " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(1)))
print("hits@3  " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(3)))
print("hits@10 " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(10)))
print()

print("MRR " + '{0:.6f}'.format(ranking.hits.get_mrr()) + \
      ", hits@1 " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(1)) + \
      ", hits@3 " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(3)) + \
      ", hits@10 " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(10)))
# now some code to some nice overview on the different relations and directions
# the loop interates over all relations in the test set
print("relation".ljust(25) + "\t" + "MRR-h" + "\t" + "MRR-t" + "\t" + "Num triples")
for rel in testset.rels:
   rel_token = testset.index.id2to[rel]
   # store all triples that use the current relation rel in rtriples
   rtriples = list(filter(lambda x: x.rel == rel, testset.triples))

   # compute scores in head direction ...
   ranking.compute_scores(rtriples, True, False)
   (mrr_head, h1_head) = (ranking.hits.get_mrr(), ranking.hits.get_hits_at_k(1))
   # ... and in tail direction
   ranking.compute_scores(rtriples, False, True)
   (mrr_tail, h1_tail) = (ranking.hits.get_mrr(), ranking.hits.get_hits_at_k(1))
   # print the resulting scores
   print(rel_token.ljust(25) +  "\t" + '{0:.3f}'.format(mrr_head) + "\t" + '{0:.3f}'.format(mrr_tail) + "\t" + str(len(rtriples)))

# finally, write the ranking to a file, there are two ways to to this, both reults into the same ranking

# Output timing information
end_time = datetime.now()
elapsed_time = end_time - start_time
print()
print(f"Evaluation completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total runtime: {elapsed_time}")