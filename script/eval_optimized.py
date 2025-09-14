from c_clause import RankingHandler, Loader
from clause import Options

from clause import Ranking
from clause import TripleSet

import argparse

# *** 优化版本的评估脚本 ***

argparser = argparse.ArgumentParser(description="Optimized evaluation of a ranking")
argparser.add_argument("--dataset", type=str, default="wnrr", help="dataset to use")
argparser.add_argument("--rules", type=str, default="", help="rules to use")
argparser.add_argument("--ranking_file", type=str, default="", help="rules to use")
argparser.add_argument("--topk", type=int, default=50, help="top-k candidates")
argparser.add_argument("--threads", type=int, default=8, help="number of threads")

args = argparser.parse_args()
dataset = args.dataset
train = f"data/{dataset}/train.txt"
filter_set = f"data/{dataset}/valid.txt"
target = f"data/{dataset}/test.txt"

rules = args.rules if args.rules else f"data/rules/{dataset}.txt"
ranking_file = args.ranking_file if args.ranking_file else f"local/ranking-{dataset}-opt.txt"

print(f"使用优化参数: topk={args.topk}, threads={args.threads}")

options = Options()

# *** 性能优化配置 ***
# 1. 减少topk以降低计算量
options.set("ranking_handler.topk", args.topk)  # 从100降到50
options.set("ranking_handler.aggregation_function", "maxplus")

# 2. 启用早停策略
options.set("ranking_handler.disc_at_least", 5)  # 当5个候选完全区分时停止
options.set("ranking_handler.hard_stop_at", args.topk)  # 达到topk时立即停止

# 3. 限制线程数避免竞争
options.set("ranking_handler.num_threads", args.threads)  # 使用指定线程数
options.set("loader.num_threads", 4)

# 4. 关闭不必要的规则类型以减少内存和计算
options.set("loader.load_u_d_rules", False)    # 关闭U_d规则
options.set("loader.load_u_xxc_rules", False)  # 关闭U_xxc规则
options.set("loader.load_u_xxd_rules", False)  # 关闭U_xxd规则
options.set("loader.load_zero_rules", False)   # 关闭零规则

# 5. 提高规则过滤阈值
options.set("loader.b_min_support", 5)      # 最小支持度
options.set("loader.c_min_support", 5)      # 最小支持度
options.set("loader.b_min_conf", 0.001)     # 最小置信度
options.set("loader.c_min_conf", 0.001)     # 最小置信度

# 6. 其他优化
options.set("ranking_handler.adapt_topk", False)  # 关闭自适应topk
options.set("ranking_handler.collect_rules", False)  # 不收集规则信息

print("正在加载数据和规则...")

#### Calculate a ranking
loader = Loader(options=options.get("loader"))
loader.load_data(data=train, filter=filter_set, target=target)
loader.load_rules(rules=rules)

print("开始计算排名...")

ranker = RankingHandler(options=options.get("ranking_handler"))
ranker.calculate_ranking(loader=loader)

print("获取排名结果...")
headRanking = ranker.get_ranking(direction="head", as_string=True)
tailRanking = ranker.get_ranking(direction="tail", as_string=True)

testset = TripleSet(target)
ranking = Ranking(k=args.topk)

# process the handler ranking
ranking.convert_handler_ranking(headRanking, tailRanking, testset)
ranking.compute_scores(testset.triples)

print("*** 优化版评估结果 ****")
print("Num triples: " + str(len(testset.triples)))
print("MRR     " + '{0:.6f}'.format(ranking.hits.get_mrr()))
print("hits@1  " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(1)))
print("hits@3  " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(3)))
print("hits@10 " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(10)))
print()

# 写入排名文件
ranker.write_ranking(path=ranking_file, loader=loader)
print(f"排名已保存到: {ranking_file}")