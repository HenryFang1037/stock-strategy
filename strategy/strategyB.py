import warnings

from tools import get_code2name

warnings.filterwarnings('ignore')


def trend_finder(stock_name, stock_code, data, window=20, least_rise_day=15, least_increase=0.5):
    res = """
        ----------------------------------------------------------------------------------------------------------
        股票：{}, 代码：{}
        ----------------------------------------------------------------------------------------------------------
        1.从{}日到{}日，涨幅达到{:.2%}，{}日中有{}日收盘价高于5日均价。
        2.5日均价都高于10日均价。
        ----------------------------------------------------------------------------------------------------------
        """
    data['ma5'] = data['收盘'].rolling(5).mean()
    data['ma10'] = data['收盘'].rolling(10).mean()
    window_data = data.iloc[-window:]
    i = window_data['收盘'] >= window_data['ma5']
    j = window_data['ma5'] >= window_data['ma10']
    start_date = window_data['日期'].iloc[0]
    end_date = window_data['日期'].iloc[-1]
    pct = window_data['收盘'].iloc[-1] / window_data['收盘'].iloc[0] - 1
    if sum(i) >= least_rise_day and j.all() and pct >= least_increase:
        return res.format(stock_name, stock_code, start_date, end_date, pct, window, sum(i))


if __name__ == '__main__':
    from download import Mongo
    from tools import search_all

    mongo = Mongo()
    code2name = get_code2name(mongo)

    search_all(mongo, ["深圳股票交易所", "上海股票交易所"], code2name=code2name,
               search_func=trend_finder, seach_days=75,
               file_name='news/{}_{}.txt')
