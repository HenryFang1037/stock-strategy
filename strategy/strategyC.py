import math
import warnings

from scipy import stats

from tools import get_code2name, findLocalMinimum

warnings.filterwarnings('ignore')


def strong_rise_finder(stock_name, stock_code, data, using_local_minimum=False, using_upstream_compare=True,
                       using_latest_num=3, least_increase=0.3,
                       max_decrease=0.1, least_increase_day_multiply=0.7):
    res = """
            ----------------------------------------------------------------------------------------------------------
            股票：{}, 代码：{}
            ----------------------------------------------------------------------------------------------------------
            1.在{}日开盘到{}日收盘，共{}日，涨幅达到{:.2%}，高于{:.2%}。
            2.从{}最高点到{}日收盘，共{}日，跌幅为{:.2%}，不超过{:.2%}。
            3.从{}日到{}日，{}个交易日中，共{}日上涨，涨幅均值为{:.2f}，换手率均值为{:.2f}，共{}日下跌，跌幅均值为{:.2f}，换手率均值为{:.2f}。
            ----------------------------------------------------------------------------------------------------------
            """
    if using_upstream_compare is True:
        if data['ma5'].iloc[-1] <= data['ma10'].iloc[-1] or data['ma10'].iloc[-1] <= data['ma20'].iloc[-1] or \
                data['ma20'].iloc[-1] <= data['ma30'].iloc[-1]:
            return None
    if using_local_minimum is True:
        low_idx = findLocalMinimum(data['最低'].values)
        high_idx = findLocalMinimum(data['最高'].values)
        idx = low_idx & high_idx
        idxes = data.loc[idx].index.values
        if len(idxes) > using_latest_num:
            lowest_open_idx = idxes[-using_latest_num]
        elif len(idxes):
            lowest_open_idx = idxes[-1]
        else:
            lowest_open_idx = data.index.values[0]
    else:
        lowest_open_idx = data['开盘'].idxmin()
    lowest_open = data['开盘'].loc[lowest_open_idx]
    lowest_open_day = data['日期'].loc[lowest_open_idx]
    last_day_idx = data.last_valid_index()
    last_day_close = data['收盘'].loc[last_day_idx]
    last_day = data['日期'].loc[last_day_idx]
    highest_high_idx = data['最高'].idxmax()
    highest_high = data['最高'].loc[highest_high_idx]
    highest_high_day = data['日期'].loc[highest_high_idx]
    total_days = last_day_idx - lowest_open_idx
    decrease = last_day_close / highest_high - 1
    decrease_days = last_day_idx - highest_high_idx
    increase = last_day_close / lowest_open - 1

    if decrease_days <= math.ceil(
            total_days * (1 - least_increase_day_multiply)) and increase >= least_increase and abs(
        decrease) <= max_decrease:
        df = data[['日期', '开盘', '收盘', '最高', '最低', '换手率']].loc[lowest_open_idx:]
        df['涨跌幅'] = df['收盘'] - df['开盘']
        df['振幅'] = df['最高'] - df['最低']
        a = df[df['涨跌幅'] > 0]
        b = df[df['涨跌幅'] <= 0]
        increase_mean = a['涨跌幅'].mean()
        increase_turnover_mean = a['换手率'].mean()
        decrease_mean = b['涨跌幅'].mean()
        decrease_turnover_mean = b['换手率'].mean()
        pct_test = stats.ttest_ind(a['涨跌幅'], abs(b['涨跌幅']), equal_var=False)
        turnover_test = stats.ttest_ind(a['换手率'], b['换手率'], equal_var=False)
        if pct_test.pvalue < 0.05 and decrease_mean < increase_mean and turnover_test.pvalue < 0.05 and decrease_turnover_mean < increase_turnover_mean:
            return res.format(stock_name, stock_code, lowest_open_day, last_day, total_days, increase, least_increase,
                              highest_high_day, last_day, decrease_days, decrease, max_decrease, lowest_open_day,
                              last_day, total_days,
                              a.shape[0], increase_mean, increase_turnover_mean, b.shape[0], decrease_mean,
                              decrease_turnover_mean
                              )


if __name__ == "__main__":
    from download import Mongo
    from tools import search_all
    import functools

    mongo = Mongo()
    code2name = get_code2name(mongo)

    search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
               search_func=functools.partial(strong_rise_finder, using_local_minimum=True),
               seach_days=250,
               file_name='news/{}_{}.txt')
