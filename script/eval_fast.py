from c_clause import RankingHandler, Loader
from clause import Options

from clause import Ranking
from clause import TripleSet

import argparse

# *** 轻量级快速评估脚本 ***

argparser = argparse.ArgumentParser(description="Fast lightweight evaluation")
argparser.add_argument("--dataset", type=str, default="wnrr", help="dataset to use")
argparser.add_argument("--rules", type=str, default="", help="rules to use")
argparser.add_argument("--sample_size", type=int, default=1000, help="sample size for testing")

args = argparser.parse_args()
dataset = args.dataset
train = f"data/{dataset}/train.txt"
filter_set = f"data/{dataset}/valid.txt"
target = f"data/{dataset}/test.txt"

rules = args.rules if args.rules else f"data/rules/{dataset}.txt"

print(f"使用采样评估: sample_size={args.sample_size}")

options = Options()

# *** 极速配置 - 用于快速测试 ***
options.set("ranking_handler.topk", 20)  # 极小的topk
options.set("ranking_handler.aggregation_function", "maxplus")
options.set("ranking_handler.disc_at_least", 3)  # 很早停止
options.set("ranking_handler.hard_stop_at", 20)  # 立即停止
options.set("ranking_handler.num_threads", 4)  # 少量线程
options.set("loader.num_threads", 2)

# 关闭所有非必要规则类型
options.set("loader.load_u_d_rules", False)
options.set("loader.load_u_xxc_rules", False)
options.set("loader.load_u_xxd_rules", False)
options.set("loader.load_zero_rules", False)

# 高过滤阈值 - 只保留高质量规则
options.set("loader.b_min_support", 10)
options.set("loader.c_min_support", 10)
options.set("loader.b_min_conf", 0.01)
options.set("loader.c_min_conf", 0.01)
options.set("loader.b_max_length", 3)  # 限制规则长度
options.set("loader.c_max_length", 3)

options.set("ranking_handler.adapt_topk", False)
options.set("ranking_handler.collect_rules", False)

print("正在加载数据和规则...")

#### Calculate a ranking with sampling
loader = Loader(options=options.get("loader"))
loader.load_data(data=train, filter=filter_set, target=target)
loader.load_rules(rules=rules)

print("开始快速计算排名...")

ranker = RankingHandler(options=options.get("ranking_handler"))
ranker.calculate_ranking(loader=loader)

print("获取排名结果...")
headRanking = ranker.get_ranking(direction="head", as_string=True)
tailRanking = ranker.get_ranking(direction="tail", as_string=True)

testset = TripleSet(target)

# 如果指定了采样大小，则随机采样测试集
if args.sample_size > 0 and len(testset.triples) > args.sample_size:
    import random
    random.seed(42)  # 保证可重复性
    sampled_triples = random.sample(testset.triples, args.sample_size)
    print(f"从 {len(testset.triples)} 个测试三元组中采样 {len(sampled_triples)} 个")
else:
    sampled_triples = testset.triples

ranking = Ranking(k=20)

# process the handler ranking
ranking.convert_handler_ranking(headRanking, tailRanking, testset)
ranking.compute_scores(sampled_triples)

print("*** 快速评估结果 ****")
print("Num triples: " + str(len(sampled_triples)))
print("MRR     " + '{0:.6f}'.format(ranking.hits.get_mrr()))
print("hits@1  " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(1)))
print("hits@3  " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(3)))
print("hits@10 " + '{0:.6f}'.format(ranking.hits.get_hits_at_k(10)))
print("注意: 这是快速评估结果，用于调试和参数调优")