import aiohttp
import asyncio
import pandas as pd
import requests
import akshare as ak


def code_id_map_em() -> dict:
    """
    东方财富-股票和市场代码
    http://quote.eastmoney.com/center/gridlist.html#hs_a_board
    :return: 股票和市场代码
    :rtype: dict
    """
    url = "http://80.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "50000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:1 t:2,m:1 t:23",
        "fields": "f12",
        "_": "1623833739532",
    }

    r = requests.get(url, params=params)
    data_json =  r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df = pd.DataFrame(data_json["data"]["diff"])
    temp_df["market_id"] = 1
    temp_df.columns = ["sh_code", "sh_id"]
    code_id_dict = dict(zip(temp_df["sh_code"], temp_df["sh_id"]))
    params = {
        "pn": "1",
        "pz": "5000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:6,m:0 t:80",
        "fields": "f12",
        "_": "1623833739532",
    }

    r = requests.get(url, params=params)
    data_json =  r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["sz_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["sz_id"])))
    params = {
        "pn": "1",
        "pz": "5000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:0 t:81 s:2048",
        "fields": "f12",
        "_": "1623833739532",
    }

    r = requests.get(url, params=params)
    data_json =  r.json()
    if not data_json["data"]["diff"]:
        return dict()
    temp_df_sz = pd.DataFrame(data_json["data"]["diff"])
    temp_df_sz["bj_id"] = 0
    code_id_dict.update(dict(zip(temp_df_sz["f12"], temp_df_sz["bj_id"])))
    return code_id_dict


async def stock_zh_a_hist(
        symbol: str = "000001",
        period: str = "daily",
        start_date: str = "19700101",
        end_date: str = "20500101",
        adjust: str = "",
        code_id_dict: dict = {},
) -> pd.DataFrame:
    """
    东方财富网-行情首页-沪深京 A 股-每日行情
    http://quote.eastmoney.com/concept/sh603777.html?from=classic
    :param symbol: 股票代码
    :type symbol: str
    :param period: choice of {'daily', 'weekly', 'monthly'}
    :type period: str
    :param start_date: 开始日期
    :type start_date: str
    :param end_date: 结束日期
    :type end_date: str
    :param adjust: choice of {"qfq": "前复权", "hfq": "后复权", "": "不复权"}
    :type adjust: str
    :return: 每日行情
    :rtype: pandas.DataFrame
    """
    adjust_dict = {"qfq": "1", "hfq": "2", "": "0"}
    period_dict = {"daily": "101", "weekly": "102", "monthly": "103"}
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": period_dict[period],
        "fqt": adjust_dict[adjust],
        "secid": f"{code_id_dict[symbol]}.{symbol}",
        "beg": start_date,
        "end": end_date,
        "_": "1623766962675",
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_json = await r.json()
        if not (data_json["data"] and data_json["data"]["klines"]):
            return pd.DataFrame()
        temp_df = pd.DataFrame(
            [item.split(",") for item in data_json["data"]["klines"]]
        )
        temp_df.columns = [
            "日期",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
        temp_df.index = pd.to_datetime(temp_df["日期"])
        temp_df.reset_index(inplace=True, drop=True)

        temp_df["开盘"] = pd.to_numeric(temp_df["开盘"])
        temp_df["收盘"] = pd.to_numeric(temp_df["收盘"])
        temp_df["最高"] = pd.to_numeric(temp_df["最高"])
        temp_df["最低"] = pd.to_numeric(temp_df["最低"])
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"])
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"])
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"])
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])

        return temp_df


async def stock_board_industry_name_em() -> pd.DataFrame:
    """
    东方财富网-沪深板块-行业板块-名称
    http://quote.eastmoney.com/center/boardlist.html#industry_board
    :return: 行业板块-名称
    :rtype: pandas.DataFrame
    """
    url = "http://17.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "2000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": "m:90 t:2 f:!50",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222",
        "_": "1626075887768",
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_json = await r.json()
        temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_df.reset_index(inplace=True)
        temp_df["index"] = temp_df.index + 1
        temp_df.columns = [
            "排名",
            "-",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "-",
            "_",
            "-",
            "换手率",
            "-",
            "-",
            "-",
            "板块代码",
            "-",
            "板块名称",
            "-",
            "-",
            "-",
            "-",
            "总市值",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            "上涨家数",
            "下跌家数",
            "-",
            "-",
            "-",
            "领涨股票",
            "-",
            "-",
            "领涨股票-涨跌幅",
            "-",
            "-",
            "-",
            "-",
            "-",
        ]
        temp_df = temp_df[
            [
                "排名",
                "板块名称",
                "板块代码",
                "最新价",
                "涨跌额",
                "涨跌幅",
                "总市值",
                "换手率",
                "上涨家数",
                "下跌家数",
                "领涨股票",
                "领涨股票-涨跌幅",
            ]
        ]
        temp_df["最新价"] = pd.to_numeric(temp_df["最新价"])
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"])
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"])
        temp_df["总市值"] = pd.to_numeric(temp_df["总市值"], errors="coerce")
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"])
        temp_df["上涨家数"] = pd.to_numeric(temp_df["上涨家数"])
        temp_df["下跌家数"] = pd.to_numeric(temp_df["下跌家数"])
        temp_df["领涨股票-涨跌幅"] = pd.to_numeric(temp_df["领涨股票-涨跌幅"])
        return temp_df

async def stock_board_industry_cons_em(symbol: str = "小金属") -> pd.DataFrame:
    """
    东方财富网-沪深板块-行业板块-板块成份
    https://data.eastmoney.com/bkzj/BK1027.html
    :param symbol: 板块名称
    :type symbol: str
    :return: 板块成份
    :rtype: pandas.DataFrame
    """
    stock_board_concept_em_map = await stock_board_industry_name_em()
    stock_board_code = stock_board_concept_em_map[
        stock_board_concept_em_map["板块名称"] == symbol
    ]["板块代码"].values[0]
    url = "http://29.push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": "1",
        "pz": "2000",
        "po": "1",
        "np": "1",
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": "2",
        "invt": "2",
        "fid": "f3",
        "fs": f"b:{stock_board_code} f:!50",
        "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152,f45",
        "_": "1626081702127",
    }
    async with aiohttp.ClientSession() as session:
        r = await session.get(url, params=params)
        data_json = await r.json()
        temp_df = pd.DataFrame(data_json["data"]["diff"])
        temp_df.reset_index(inplace=True)
        temp_df["index"] = range(1, len(temp_df) + 1)
        temp_df.columns = [
            "序号",
            "_",
            "最新价",
            "涨跌幅",
            "涨跌额",
            "成交量",
            "成交额",
            "振幅",
            "换手率",
            "市盈率-动态",
            "_",
            "_",
            "代码",
            "_",
            "名称",
            "最高",
            "最低",
            "今开",
            "昨收",
            "_",
            "_",
            "_",
            "市净率",
            "_",
            "_",
            "_",
            "_",
            "_",
            "_",
            "_",
            "_",
            "_",
            "_",
        ]
        temp_df = temp_df[
            [
                "序号",
                "代码",
                "名称",
                "最新价",
                "涨跌幅",
                "涨跌额",
                "成交量",
                "成交额",
                "振幅",
                "最高",
                "最低",
                "今开",
                "昨收",
                "换手率",
                "市盈率-动态",
                "市净率",
            ]
        ]
        temp_df["最新价"] = pd.to_numeric(temp_df["最新价"], errors="coerce")
        temp_df["涨跌幅"] = pd.to_numeric(temp_df["涨跌幅"], errors="coerce")
        temp_df["涨跌额"] = pd.to_numeric(temp_df["涨跌额"], errors="coerce")
        temp_df["成交量"] = pd.to_numeric(temp_df["成交量"], errors="coerce")
        temp_df["成交额"] = pd.to_numeric(temp_df["成交额"], errors="coerce")
        temp_df["振幅"] = pd.to_numeric(temp_df["振幅"], errors="coerce")
        temp_df["最高"] = pd.to_numeric(temp_df["最高"], errors="coerce")
        temp_df["最低"] = pd.to_numeric(temp_df["最低"], errors="coerce")
        temp_df["今开"] = pd.to_numeric(temp_df["今开"], errors="coerce")
        temp_df["昨收"] = pd.to_numeric(temp_df["昨收"], errors="coerce")
        temp_df["换手率"] = pd.to_numeric(temp_df["换手率"], errors="coerce")
        temp_df["市盈率-动态"] = pd.to_numeric(temp_df["市盈率-动态"], errors="coerce")
        temp_df["市净率"] = pd.to_numeric(temp_df["市净率"], errors="coerce")
        temp_df["板块"] = symbol
        return temp_df

async def get_all_category_stock_stats():
        """
        获取各个板块各股票信息
        :return:
        """
        tasks = []
        res = pd.DataFrame()
        cates = await stock_board_industry_name_em()
        # return cates
        for cate in cates['板块名称']:
            cor = stock_board_industry_cons_em(symbol=cate)
            task = asyncio.ensure_future(cor)
            tasks.append(task)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
        #     r = await stock_board_industry_cons_em(symbol=cate)
        #     r['cate'] = cate
        #     res = res.append(r)
        # print( res )
if __name__ == '__main__':
    from download import Mongo
    mongo = Mongo()
    def callback(task):
        print(task.result())
        # pass
    import time
    tasks = []
    start = time.time()
    end_date = '20221129'
    start_date = '20221128'
    exchange_name = '深圳股票交易所'
    stocks_data = mongo.stock_data_sz
    codes = stocks_data.list_collection_names()
    code_id_dict =  code_id_map_em()
    async def main(codes):
        for cate in codes:
            cor = stock_zh_a_hist(symbol=cate, start_date=start_date, end_date=end_date, code_id_dict=code_id_dict)
            task = asyncio.ensure_future(cor)
            task.add_done_callback(callback)
            tasks.append(task)

    exchange_name = '上海股票交易所'
    stocks_data = mongo.stock_data_sh
    codes = stocks_data.list_collection_names()
    for cate in codes:
        cor = stock_zh_a_hist(symbol=cate, start_date=start_date, end_date=end_date, code_id_dict=code_id_dict)
        task = asyncio.ensure_future(cor)
        task.add_done_callback(callback)
        tasks.append(task)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    print("总共用时{}秒".format(time.time() - start))
