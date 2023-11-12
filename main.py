import functools

from download import Mongo
from download import download_all, download_new_listed
from strategy import strategyA, strategyC, strategyD, strategyE
from tools import search_all, get_code2name

if __name__ == "__main__":
    import time
    mongo = Mongo()
    code2name = get_code2name(mongo)

    # 下载最新上市的股票数据
    # download_new_listed()

    # 下载沪深股市股票日度数据
    start = time.time()
    download_all(start_date='20221128',end_date='20221130', usingThreadPool=True)
    print('总共用时{}秒'.format(time.time() - start))
    # download_all(preday=False, usingThreadPool=True, )

    # 使用策略进行筛股
    search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
               search_func=strategyA.gap_finder, strategy_name='strategyA', seach_days=75,
               file_name='news/{}_{}.txt')

    # search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
    #            search_func=functools.partial(strategyC.strong_rise_finder, using_local_minimum=True),
    #            strategy_name='strategyC',
    #            seach_days=250,
    #            file_name='news/{}_{}.txt')
    #
    # search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
    #            search_func=functools.partial(strategyD.turnover_boost_finder, window=60, upper_bound_multiply=6),
    #            strategy_name='strategyD',
    #            seach_days=250,
    #            file_name='news/{}_{}.txt')

    # search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
    #            search_func=functools.partial(strategyE.strongRiseGapFinder, least_rise_day=5, least_increase=0.8,
    #                                          max_decrease_abs=0.3, gap_day_max_decrease=0.06), strategy_name='strategyE',
    #            seach_days=75,
    #            file_name='news/{}_{}.txt')
