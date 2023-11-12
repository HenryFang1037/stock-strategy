import os
import time
from datetime import datetime, timedelta

import akshare as ak
import numpy as np
import pandas as pd
from tqdm import tqdm


class Tool:
    @staticmethod
    def get_sz_stock_code():
        """
        获取深圳交易所股票代码
        :return:
        """
        return ak.stock_info_sz_name_code()

    @staticmethod
    def get_sh_stock_code():
        """
        获取上海股票交易所股票代码
        :return:
        """
        return ak.stock_info_sh_name_code()

    @staticmethod
    def get_bj_stock_code():
        """
        获取北京股票交易所股票代码
        :return:
        """
        return ak.stock_info_bj_name_code()

    @staticmethod
    def get_sz_dilisted_stocks():
        """
        获取深证股票交易所暂停和终止上市股票代码
        :return:
        """
        return ak.stock_info_sz_delist()

    @staticmethod
    def get_sh_dilisted_stocks():
        """
        获取上海股票交易所暂停和终止上市股票代码
        :return:
        """
        return ak.stock_info_sh_delist()

    @staticmethod
    def get_all_category_stock_stats():
        """
        获取各个板块各股票信息
        :return:
        """
        res = pd.DataFrame()
        cates = ak.stock_board_industry_name_em()
        for cate in cates['板块名称']:
            r = ak.stock_board_industry_cons_em(symbol=cate)
            r['cate'] = cate
            res = res.append(r)
        return res

    @staticmethod
    def get_stock_daily_history(stockCode, start_date, end_date, adjust='qfq'):
        try:
            if start_date is None:
                res = ak.stock_zh_a_hist(symbol=stockCode, period='daily', end_date=end_date,
                                         adjust=adjust)
            else:
                res = ak.stock_zh_a_hist(symbol=stockCode, period='daily', start_date=start_date, end_date=end_date,
                                         adjust=adjust)
            return stockCode, res
        except Exception as e:
            # print("Raise exception {}".format(e))
            return stockCode, None

    @staticmethod
    def get_stock_individual_fund_flow(indicator='今日'):
        return ak.stock_individual_fund_flow_rank(indicator=indicator)

    @staticmethod
    def get_stock_sector_fund_flow_rank(indicator='今日'):
        return ak.stock_sector_fund_flow_rank(indicator=indicator)

    @staticmethod
    def get_stock_individual_info(symbol):
        return ak.stock_individual_info_em(symbol=symbol)


def get_code2name(mongo):
    code2name = {}
    stock_code = mongo.stock_code
    sz_stock_code = stock_code.sz
    sh_stock_code = stock_code.sh

    df = pd.DataFrame(sz_stock_code.find({}, {'_id': 0, 'A股代码': 1, 'A股简称': 1}))
    df = df.apply(lambda row: {row[0]: row[1]}, axis=1).to_list()
    for item in df:
        code2name.update(item)
    df1 = pd.DataFrame(sh_stock_code.find({}, {'_id': 0, '证券代码': 1, '证券简称': 1}))
    df1 = df1.apply(lambda row: {row[0]: row[1]}, axis=1).to_list()
    for item in df1:
        code2name.update(item)
    return code2name


def findLocalMinimum(numpyArray):
    return np.r_[True, numpyArray[1:] < numpyArray[:-1]] & np.r_[numpyArray[:-1] < numpyArray[1:], True]


def findLocalMaximum(numpyArray):
    return np.r_[True, numpyArray[1:] > numpyArray[:-1]] & np.r_[numpyArray[:-1] > numpyArray[1:], True]


def ma(dataFrame, window=5):
    return dataFrame['收盘'].rolling(window).mean()


def upstream(dataFrame):
    dataFrame['ma5'] = ma(dataFrame)
    dataFrame['ma10'] = ma(dataFrame, window=10)
    dataFrame['ma20'] = ma(dataFrame, window=20)
    dataFrame['ma30'] = ma(dataFrame, window=30)
    return dataFrame


def search_all(mongo, exchange_names, code2name, search_func, strategy_name, seach_days, file_name='news/{}_{}.txt'):
    today = datetime.strftime(datetime.now(), '%Y-%m-%d')
    start_date = datetime.strftime(datetime.now() - timedelta(days=seach_days), "%Y-%m-%d")
    file = os.path.join(os.path.abspath(os.path.curdir), file_name.format(today, strategy_name))
    for exchange_name in exchange_names:
        if exchange_name == '深圳股票交易所':
            stocks_data = mongo.stock_data_sz
            stock_codes = stocks_data.list_collection_names()
        elif exchange_name == '上海股票交易所':
            stocks_data = mongo.stock_data_sh
            stock_codes = stocks_data.list_collection_names()
        else:
            raise Exception('exchange_name为深圳股票交易所或上海股票交易所。交易所名称错误！ ')

        start = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

        print("{}: 使用{}策略对{}市场{}只股票开始分析！".format(start, strategy_name, exchange_name, len(stock_codes)))

        for code in tqdm(stock_codes):
            col = stocks_data[code]
            data = pd.DataFrame(col.find({'日期': {'$gt': start_date}}).sort('日期', 1))
            if data.empty is True:
                continue
            data = upstream(data)
            res = search_func(code2name[code], code, data)

            if res is not None:
                with open(file, 'a') as f:
                    f.writelines(res)
                    f.newlines
        end = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        print("{}: 使用{}策略对{}市场{}只股票分析完成！".format(end, strategy_name, exchange_name, len(stock_codes)))
        time.sleep(1)


if __name__ == '__main__':
    # df = ak.stock_board_industry_name_em()
    # print(df['板块名称'])

    # Tool.get_stock_sector_fund_flow_rank()
    # import time
    # start = time.time()
    # print(Tool.get_all_category_stock_stats())
    # print("总共用时{}秒".format(time.time() - start))

    res = ak.stock_individual_info_em(symbol="002962")
    print(res)