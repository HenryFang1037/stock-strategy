import warnings

import numpy as np

from tools import get_code2name

warnings.filterwarnings('ignore')


def strongRiseGapFinder(stock_name, stock_code, data, least_rise_day=5, least_increase=0.8,
                        max_decrease_abs=0.3, gap_day_max_decrease=0.06):
    res = """
        ----------------------------------------------------------------------------------------------------------
        股票：{}, 代码：{}
        ----------------------------------------------------------------------------------------------------------
        1.从{}日开盘到{}日共{}日，涨幅达到{:.2%}，最近{}日，ma5均大于ma10且ma10均大于ma20。
         ma5为{:.2f}，ma10为{:.2f}, ma20为{:.2f}。
        2.从{}日开盘到昨日共形成{}个缺口，分别是{}。
        3.在{}日收盘价为{}，{}，持有到达到{}价格。
        ----------------------------------------------------------------------------------------------------------
        """
    lowest_open_idx = data['开盘'].idxmin()
    lowest_open = data['开盘'].loc[lowest_open_idx]
    lowest_open_day = data['日期'].loc[lowest_open_idx]
    last_day_idx = data.last_valid_index()
    last_day_close = data['收盘'].loc[last_day_idx]
    last_day = data['日期'].loc[last_day_idx]
    total_days = last_day_idx - lowest_open_idx
    increase = last_day_close / lowest_open - 1
    if total_days < least_rise_day or increase < least_increase:
        return
    right_data = data[lowest_open_idx:]
    highest_high_idx = right_data['最高'].idxmax()
    highest_high = right_data['最高'].loc[highest_high_idx]
    decrease = last_day_close / highest_high - 1
    if abs(decrease) > max_decrease_abs:
        return
    if np.all(right_data['ma5'].iloc[-least_rise_day:] > right_data['ma10'].iloc[-least_rise_day:]) and np.all(
            right_data['ma10'].iloc[-least_rise_day:] > right_data['ma20'].iloc[-least_rise_day:]):

        gap = right_data['收盘'].iloc[-least_rise_day - 1:].shift(1) > right_data['最高'].iloc[-least_rise_day - 1:]
        if np.any(gap):
            gap_indexes = gap[gap == True].index.values
            gap_numbers = len(gap_indexes)
            if gap_numbers < 1:
                return
            gap_days = list(right_data['日期'].loc[gap_indexes])
            gap_close_prices = list(right_data['收盘'].loc[gap_indexes - 1])
            gap_indexes = [index for index in gap_indexes if np.all(right_data['收盘'].loc[index - 1] > right_data['最高'].loc[index:])]
            if len(gap_indexes) < 1:
                return
            gap_index = gap_indexes[-1]
            buy_day = right_data['日期'].loc[gap_index]
            buy_price = right_data['收盘'].loc[gap_index]
            high2low = right_data['最高'].loc[gap_index] / right_data['最低'].loc[gap_index] - 1

            expected_price = right_data['收盘'].loc[gap_index - 1]
            ma5 = right_data['ma5'].iloc[-1]
            ma10 = right_data['ma10'].iloc[-1]
            ma20 = right_data['ma20'].iloc[-1]
            if high2low > gap_day_max_decrease:
                ans = '形成缺口当日股价振幅达到{:.2%}, 超过最大限度{:.2%}， 建议观望后续价格走势'.format(high2low, gap_day_max_decrease)
            elif buy_price > ma5:
                ans = '未跌破5日收盘均价, 建议买入持有'
            elif buy_price > ma10:
                ans = '跌破5日收盘均价但未跌破10日收盘均价， 建议买入持有'
            else:
                ans = '跌破10日收盘均价，建议观望后续价格走势'

            return res.format(stock_name, stock_code, lowest_open_day, last_day, total_days,
                              increase,least_rise_day, ma5,ma10, ma20,
                              lowest_open_day, gap_numbers, list(zip(gap_days, gap_close_prices)), buy_day,
                              buy_price, ans, expected_price)


if __name__ == "__main__":
    from download import Mongo
    from tools import search_all

    mongo = Mongo()
    code2name = get_code2name(mongo)

    search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
               search_func=strongRiseGapFinder, seach_days=120, strategy_name='strategyE',
               file_name='{}_{}.txt')

    # from pymongo import MongoClient
    # import pandas as pd
    #
    #
    # def ma(dataFrame, window=5):
    #     return dataFrame['收盘'].rolling(window).mean()
    #
    #
    # def upstream(dataFrame):
    #     dataFrame['ma5'] = ma(dataFrame)
    #     dataFrame['ma10'] = ma(dataFrame, window=10)
    #     dataFrame['ma20'] = ma(dataFrame, window=20)
    #     dataFrame['ma60'] = ma(dataFrame, window=60)
    #     return dataFrame
    # client = MongoClient('localhost', 27017)
    # db = client['stock_data_sz']
    # cols = db['002417']
    # df = pd.DataFrame(cols.find({'日期': {'$gt': '2021-06-01'}}).sort('日期', 1))
    # df = upstream(df)
    #
    # res = strongRiseGapFinder('002417', '002417', df)

