import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from datetime import timedelta

from pymongo import MongoClient
from tqdm import tqdm

from tools import Tool


class Mongo:
    def __init__(self):
        self.client = MongoClient('localhost', 27017, maxPoolSize=None)
        self.stock_code = self.client['stock_code']
        self.stock_cate = self.client['stock_cate']
        self.stock_index = self.client['stock_index']
        self.stock_data_sz = self.client['stock_data_sz']
        self.stock_data_sh = self.client['stock_data_sh']
        self.stock_fund_flow = self.client['stock_fund_flow']


def downloader(mongo, exchange_name, start_date=None, end_date=None, usingThreadPool=True, download_new_listed=False):
    def save(code, res):
        res = res.to_dict('records')
        col = stocks_data[code]
        for data in res:
            col.update_one({'日期': data['日期']}, {'$set': data}, upsert=True)

    def download(codes, usingThreadPool=usingThreadPool):
        retry_codes = []
        if usingThreadPool is False:
            for code in tqdm(codes):
                code, res = Tool.get_stock_daily_history(stockCode=code, start_date=start_date, end_date=end_date)
                if res is not None:
                    pass
                    # save(code, res)
                else:
                    retry_codes.append(code)
        else:
            with ThreadPoolExecutor(max_workers=25) as pool:
                futures = [pool.submit(Tool.get_stock_daily_history, code, start_date, end_date) for code in
                           codes]
                for future in as_completed(futures):
                    code, res = future.result()
                    if res is not None:
                        pass
                        # save(code, res)
                    else:
                        retry_codes.append(code)
        return retry_codes

    if download_new_listed is False:
        if exchange_name == '深圳股票交易所':
            stocks_data = mongo.stock_data_sz
            codes = stocks_data.list_collection_names()
        elif exchange_name == '上海股票交易所':
            stocks_data = mongo.stock_data_sh
            codes = stocks_data.list_collection_names()
        else:
            raise Exception('exchange_name为深圳股票交易所或上海股票交易所。交易所名称错误！ ')
    else:
        codes = []
        stock_code = mongo.stock_code

        if exchange_name == '深圳股票交易所':
            sz_stock_code = stock_code.sz
            stocks_data = mongo.stock_data_sz
            stock_codes = stocks_data.list_collection_names()
            sz_stock_codes_all = Tool.get_sz_stock_code()
            if sz_stock_codes_all.empty is False:
                new_codes = sz_stock_codes_all['A股代码'].to_list()
                codes = list(set(new_codes) - set(stock_codes))
                if len(codes) != 0:
                    for record in sz_stock_codes_all.to_dict('records'):
                        if record['A股代码'] in codes:
                            record['A股上市日期']  = str(record['A股上市日期'])
                            sz_stock_code.insert_one(record)
        elif exchange_name == '上海股票交易所':
            sh_stock_code = stock_code.sh
            stocks_data = mongo.stock_data_sh
            stock_codes = stocks_data.list_collection_names()
            sh_stock_codes_all = Tool.get_sh_stock_code()
            if sh_stock_codes_all.empty is False:
                new_codes = sh_stock_codes_all['证券代码'].to_list()
                codes = list(set(new_codes) - set(stock_codes))
                if len(codes) != 0:
                    for record in sh_stock_codes_all.to_dict('records'):
                        if record['证券代码'] in codes:
                            record['上市日期'] = str(record['上市日期'])
                            sh_stock_code.insert_one(record)
        else:
            raise Exception('exchange_name为深圳股票交易所或上海股票交易所。交易所名称错误！ ')

    if len(codes) == 0:
        print("{}：{}近期没有新上市的股票！".format(datetime.strftime
                                        (datetime.now(), '%Y-%m-%d %H:%M:%S'), exchange_name))
        return

    print("{}：开始下载{}数据".format(datetime.strftime
                               (datetime.now(), '%Y-%m-%d %H:%M:%S'), exchange_name))

    while len(codes):
        codes = download(codes, usingThreadPool)
        time.sleep(5)

    print("{}：{}股票数据下载完成".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), exchange_name))


def download_new_listed(end_date=None, usingThreadPool=True):
    mongo = Mongo()
    today = datetime.now()
    isoweekday = today.isoweekday()
    if end_date is None:
        if isoweekday == 6:
            end_date = datetime.strftime(today - timedelta(days=1), "%Y%m%d")
            print('{}是周{}，非交易日'.format(datetime.strftime(today, "%Y-%m-%d"), isoweekday))
        elif isoweekday == 7:
            end_date = datetime.strftime(today - timedelta(days=2), "%Y%m%d")
            print('{}是周{}，非交易日'.format(datetime.strftime(today, "%Y-%m-%d"), isoweekday))
        elif isoweekday == 1:
            end_date = datetime.strftime(today, "%Y%m%d")
        else:
            end_date = datetime.strftime(today, "%Y%m%d")
    print("{}：开始为您下载近期上市的沪深股票数据".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')))

    downloader(mongo, exchange_name='深圳股票交易所', start_date=None, end_date=end_date, usingThreadPool=usingThreadPool,
               download_new_listed=True)
    downloader(mongo, exchange_name='上海股票交易所', start_date=None, end_date=end_date, usingThreadPool=usingThreadPool,
               download_new_listed=True)

    print("{}：近期上市的沪深股票数据下载完成".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')))


def download_all(preday=False, usingThreadPool=False, start_date=None, end_date=None):
    mongo = Mongo()
    today = datetime.now()
    isoweekday = today.isoweekday()
    if end_date is None and start_date is None:
        if isoweekday == 6:
            end_date = datetime.strftime(today - timedelta(days=1), "%Y%m%d")
            start_date = datetime.strftime(today - timedelta(days=2), "%Y%m%d")
            print('{}是周{}，非交易日'.format(datetime.strftime(today, "%Y-%m-%d"), isoweekday))
        elif isoweekday == 7:
            end_date = datetime.strftime(today - timedelta(days=2), "%Y%m%d")
            start_date = datetime.strftime(today - timedelta(days=3), "%Y%m%d")
            print('{}是周{}，非交易日'.format(datetime.strftime(today, "%Y-%m-%d"), isoweekday))
        elif isoweekday == 1:
            end_date = datetime.strftime(today, "%Y%m%d")
            start_date = datetime.strftime(today - timedelta(days=3), "%Y%m%d")
        else:
            end_date = datetime.strftime(today, "%Y%m%d")
            start_date = datetime.strftime(today - timedelta(days=1), "%Y%m%d")
        if preday is True:
            start_date = start_date
            end_date = end_date
        else:
            start_date = end_date
            end_date = end_date
        print(
            "{}：开始为您下载{}日到{}日的数据".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), start_date, end_date))
        downloader(mongo, exchange_name='深圳股票交易所', start_date=start_date, end_date=end_date,
                   usingThreadPool=usingThreadPool)
        downloader(mongo, exchange_name='上海股票交易所', start_date=start_date, end_date=end_date,
                   usingThreadPool=usingThreadPool)

    else:
        print("{}：开始为您下载{}日的数据".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S'), end_date))
        downloader(mongo, exchange_name='深圳股票交易所', start_date=start_date, end_date=end_date,
                   usingThreadPool=usingThreadPool)
        downloader(mongo, exchange_name='上海股票交易所', start_date=start_date, end_date=end_date,
                   usingThreadPool=usingThreadPool)

    print("{}：开始下载沪深两市个股资金流向统计数据！".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')))
    try:
        today = datetime.strftime(datetime.now(), '%Y-%m-%d')
        individual_stock_fund_flow = Tool.get_stock_individual_fund_flow()
        individual_stock_fund_flow['日期'] = today
        records = individual_stock_fund_flow.to_dict('records')
        col = mongo.stock_fund_flow.rank
        for record in records:
            col.insert_one(record)
    except Exception as e:
        print(e)
    print("{}：沪深两市个股资金流向统计数据下载完成！".format(datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')))


if __name__ == '__main__':
    # download_all(preday=True, usingThreadPool=True, end_date='20220719', start_date='20220713')
    # download_new_listed()
    import asyncio
    from concurrent.futures import Future
    today = datetime.now()
    end_date = datetime.strftime(today - timedelta(days=1), "%Y%m%d")
    start_date = datetime.strftime(today - timedelta(days=2), "%Y%m%d")
    mongo = Mongo()
    # sh_stock_code = stock_code.sh
    stocks_data = mongo.stock_data_sh
    stock_codes = stocks_data.list_collection_names()
    print(stock_codes)
    # print(stock_codes)
    # async def download(stock_codes, start_date, end_date):
    #     loop = asyncio.get_running_loop()
    #     task_list = [loop.run_in_executor(None, Tool.get_stock_daily_history(stockCode=code, start_date=start_date, end_date=end_date))
    #                  for code in stock_codes[:10]]
    #
    #     done, pending = await asyncio.wait(task_list)
    #     print(done)
    #
    # asyncio.run(download(stock_codes, start_date, end_date))
    # sh_stock_codes_all = Tool.get_sh_stock_code()
    # col = mongo.stock_code.sh
    # sh_stock_codes_all['上市日期'] = sh_stock_codes_all['上市日期'].apply(str)
    # for record in sh_stock_codes_all.to_dict('records'):
    #     col.update_one({'证券代码': record['证券代码']}, {'$set': record}, upsert=True)
    #
    # sz_stock_codes_all = Tool.get_sz_stock_code()
    # sz_stock_codes_all['A股上市日期'] = sz_stock_codes_all['A股上市日期'].apply(str)
    # col = mongo.stock_code.sz
    #
    # for record in sz_stock_codes_all.to_dict('records'):
    #     col.update_one({'A股代码': record['A股代码']}, {'$set': record}, upsert=True)


