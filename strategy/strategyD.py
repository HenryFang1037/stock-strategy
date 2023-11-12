import warnings

warnings.filterwarnings('ignore')


def turnover_boost_finder(stock_name, stock_code, data, window=60, upper_bound_multiply=6):
    res = """
            ----------------------------------------------------------------------------------------------------------
            股票：{}, 代码：{}
            ----------------------------------------------------------------------------------------------------------
            1.在{}日成交量达到{}，涨跌幅达到{:.2%}， 成交量超过{}日均值{:.2f}的{}倍标准差值{:.2f}。
            ----------------------------------------------------------------------------------------------------------
            """
    res1 = """
            ----------------------------------------------------------------------------------------------------------
            股票：{}, 代码：{}
            ----------------------------------------------------------------------------------------------------------
            1.在{}日成交量达到{:.2f}， 成交量低于前{}日值，涨跌幅为{:.2%}，小于{:.2%}。
            ----------------------------------------------------------------------------------------------------------
            """
    turnover_std = data['成交量'].rolling(window * 2).std().iloc[-1]
    turnover_mean = data['成交量'].rolling(window).mean().iloc[-1]
    upper_bound = turnover_mean + upper_bound_multiply * turnover_std
    # small_turnover_idx = data['成交量'].iloc[-window*2:].idxmin()
    # latest_turnover_idx = data.last_valid_index()
    latest_turnover = data['成交量'].iloc[-1]
    latest_day = data['日期'].iloc[-1]
    pct = data['收盘'].iloc[-1] / data['开盘'].iloc[-1] - 1
    high_close_pct = data['最高'].iloc[-1] / data['收盘'].iloc[-1] - 1

    if latest_turnover > upper_bound and pct > 0.05 and high_close_pct < 0.005:
        return res.format(stock_name, stock_code, latest_day, latest_turnover, pct, window, turnover_mean,
                          upper_bound_multiply, turnover_std)
    # elif small_turnover_idx == latest_turnover_idx and abs(pct) <= 0.01:
    #     return res1.format(stock_name, stock_code, latest_day, latest_turnover, window, pct, 0.01)
    else:
        return None


if __name__ == '__main__':
    from download import Mongo
    from tools import search_all, get_code2name
    import functools

    mongo = Mongo()
    code2name = get_code2name(mongo)

    search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
               search_func=functools.partial(turnover_boost_finder, window=120, upper_bound_multiply=6),
               strategy_name='strategyD',
               seach_days=250,
               file_name='news/{}_{}.txt')
