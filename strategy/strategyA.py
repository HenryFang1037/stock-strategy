import warnings

import numpy as np

from tools import get_code2name

warnings.filterwarnings('ignore')


def gap_finder(stock_name, stock_code, data, least_rise_day=5, least_drop_day=3, max_small_turnover_multiply=0.33,
               least_increase=0.5, least_decrease=-0.4):
    res = """
        ----------------------------------------------------------------------------------------------------------
        股票：{}, 代码：{}
        ----------------------------------------------------------------------------------------------------------
        1.从{}开盘到{}最高点共{}日，涨幅达到{:.2%}，从最高点到{}日开盘，共{}日，跌幅达到{:.2%}。
        2.从最高点到昨日共形成{}个缺口，分别是{}。
        3.在{}日收盘价为{}，交易量缩小到最大交易量的{}倍，建议后续可买入，持有到达到{}价格。
        ----------------------------------------------------------------------------------------------------------
        """
    highest_high_idx = data['最高'].idxmax()
    highest_high = data['最高'].loc[highest_high_idx]
    highest_turnover = data['换手率'].loc[highest_high_idx]
    highest_high_day = data['日期'].loc[highest_high_idx]
    max_small_turnover = highest_turnover * max_small_turnover_multiply
    left_data = data.loc[:highest_high_idx + 1]
    lowest_open_idx = left_data['开盘'].idxmin()
    lowest_open = left_data['开盘'].loc[lowest_open_idx]
    lowest_open_day = left_data['日期'].loc[lowest_open_idx]
    max_increase = round(highest_high / lowest_open - 1, 4)
    increase_day = highest_high_idx - lowest_open_idx

    if max_increase >= least_increase and increase_day >= least_rise_day:
        right_data = data.loc[highest_high_idx:]
        right_data['gap'] = right_data['收盘'].shift(1) > right_data['最高']

        if np.any(right_data['gap']):
            gap_indexes = right_data[right_data['gap'] == True].index.values
            gap_index = gap_indexes[0]
            gap_numbers = len(gap_indexes)
            gap_days = list(right_data['日期'].loc[gap_indexes])
            gap_close_prices = list(right_data['收盘'].loc[gap_indexes])
            for i, idx in enumerate(range(gap_index + 1, right_data.index.stop), 1):
                turnover = right_data['换手率'].loc[idx]
                dropday = idx - highest_high_idx
                close_price = right_data['收盘'].loc[idx]
                pre_close_price = right_data['收盘'].loc[idx - 1]
                pct = close_price / pre_close_price - 1
                max_decrease = round(close_price / highest_high - 1, 4)
                buy_day = right_data['日期'].loc[idx]
                if max_decrease <= least_decrease and dropday >= least_drop_day and turnover <= max_small_turnover and pct > -1.0:
                    return res.format(stock_name, stock_code, lowest_open_day, highest_high_day, increase_day,
                                      max_increase, buy_day, dropday, max_decrease, gap_numbers, gap_days, buy_day,
                                      close_price,
                                      max_small_turnover_multiply,
                                      list(reversed(gap_close_prices[:i]))
                                      )


if __name__ == "__main__":
    from download import Mongo
    from tools import search_all

    mongo = Mongo()
    code2name = get_code2name(mongo)

    search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
               search_func=gap_finder, seach_days=75,
               file_name='news/{}_{}.txt')
