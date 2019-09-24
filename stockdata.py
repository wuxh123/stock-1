#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  stockdata.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-18 16:07:49
#  Last Modified:  2019-09-24 08:27:39
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import sys
import pickle
import time
import datetime
import zlib
import redis
import pandas as pd
import tushare as ts


class stockdata:
    def __init__(self):
        with open('tk.pkl', 'rb') as f:
            tk = pickle.load(f)
            ts.set_token(tk)
        self.pro = ts.pro_api()
        # 原始数据存数据库
        # self.original = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=0)
        self.original = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=0)
        # 原始数据 展开 加快处理速度
        # self.expand = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=1)
        self.expand = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=1)
        # 数据处理标志 临时数据 存数据库
        # self.temp = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=2)
        self.temp = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=2)
        # 训练用数据存数据库
        # self.train_data = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=3)
        self.train_data = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=3)

    def get_today_date(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    def download_stock_basic(self):
        df = self.pro.stock_basic(list_status="L")
        df = df.reset_index(drop=True)
        self.original.set('stock_basic', zlib.compress(pickle.dumps(df), 5))
        print("save: ", 'stock_basic ok')

    def get_stock_basics(self):
        df = pickle.loads(zlib.decompress(self.original.get('stock_basic')))
        return df

    def download_trade_cal_list(self):
        todaydate = self.get_today_date()
        df = self.pro.query('trade_cal', start_date='20000101', end_date=todaydate)
        df = df[df.is_open == 1]
        df = df['cal_date']
        df = df.reset_index(drop=True)
        self.original.set('trade_cal', zlib.compress(pickle.dumps(df), 5))
        print("save: ", 'trade_cal ok')
        return df

    def get_trade_cal_list(self, st_date='20130101'):
        cal = pickle.loads(zlib.decompress(self.original.get('trade_cal')))
        cal = cal[cal >= st_date]
        cal = cal.reset_index(drop=True)
        return cal

    # use local redis db
    def get_stock_list_date_n(self, code, date):
        da = self.get_stock_basics()
        da = da[da.ts_code == code]
        if da.empty is True:
            return pd.DataFrame()

        da = da['list_date'].iat[0]
        dl = self.get_trade_cal_list(da)
        dl = dl[(dl > da) & ((dl <= date))]
        dl = dl.sort_values(ascending=False)
        dl = dl.reset_index(drop=True)
        cnt = dl.shape[0]
        if cnt < 41:
            return pd.DataFrame()

        df = pd.DataFrame()
        for i in range(cnt):
            dt = dl.iat[i]
            dff = self.get_date_stock_num(dt, code)
            if dff.empty is False:
                df = df.append(dff, ignore_index=True)
                df = df[dff.index]
                # 调整列顺序，上一行操作后会自动按字母顺序将列排序
                if df.shape[0] == 41:
                    df = df.sort_values("trade_date", ascending=True)
                    df = df.reset_index(drop=True)
                    return df

        return pd.DataFrame()

    #                                          date name
    def check_exists_and_save(self, rds, func, key, field):
        re = rds.hexists(key, field)
        ret = False
        if re == 0:
            data = func(trade_date=key)
            ret = True
            if data.empty is False:
                rds.hset(key, field, zlib.compress(pickle.dumps(data), 5))
                print("save: ", key, field, " ok")
            else:
                print("save: ", key, field, " error")
        return ret

    # 000001.SH 399001.SZ 399006.SZ
    def download_index_daily(self, code, date):
        re = self.original.hexists(date, code)
        if re == 0:
            data = self.pro.index_daily(ts_code=code, trade_date=date)
            time.sleep(0.6)
            if data.empty is False:
                self.original.hset(date, code, zlib.compress(pickle.dumps(data), 5))
                print("save: ", date, code, " ok")
            else:
                print("save: ", date, code, " error")

    def download_index_daily_all(self, date):
        self.download_index_daily('000001.SH', date)
        self.download_index_daily('399001.SZ', date)
        if date >= "20100601":
            self.download_index_daily('399006.SZ', date)

    def get_index_daily_all(self, date):
        d1 = pickle.loads(zlib.decompress(self.original.hget(date, '000001.SH')))
        d2 = pickle.loads(zlib.decompress(self.original.hget(date, '399001.SZ')))
        d3 = pickle.loads(zlib.decompress(self.original.hget(date, '399006.SZ')))
        d1 = d1.append([d2, d3])
        d1 = d1.reset_index(drop=True)
        # print(d1)
        return d1

    def download_top_list(self, date):
        self.check_exists_and_save(self.original, self.pro.top_list, date, 'top_list')

    def get_top_list(self, date):
        if self.original.hexists(date, 'top_list'):
            return pickle.loads(zlib.decompress(self.original.hget(date, 'top_list')))
        return pd.DataFrame()

    def download_top_inst(self, date):
        self.check_exists_and_save(self.original, self.pro.top_inst, date, 'top_inst')

    def get_top_inst(self, date):
        if self.original.hexists(date, 'top_inst'):
            return pickle.loads(zlib.decompress(self.original.hget(date, 'top_inst')))
        return pd.DataFrame()

    def download_stk_limit(self, date):
        self.check_exists_and_save(self.original, self.pro.stk_limit, date, 'stk_limit')

    def get_stk_limit(self, date):
        if self.original.hexists(date, 'stk_limit'):
            return pickle.loads(zlib.decompress(self.original.hget(date, 'stk_limit')))
        return pd.DataFrame()

    def download_daily(self, date):
        self.check_exists_and_save(self.original, self.pro.daily, date, 'daily')

    def get_daily(self, date):
        if self.original.hexists(date, 'daily'):
            return pickle.loads(zlib.decompress(self.original.hget(date, 'daily')))
        return pd.DataFrame()

    def download_hk_hold(self, date):
        b = self.check_exists_and_save(self.original, self.pro.hk_hold, date, 'hk_hold')
        if b:
            time.sleep(41)

    def download_block_trade(self, date):
        if date < "20160108":
            return
        b = self.check_exists_and_save(self.original, self.pro.block_trade, date, 'block_trade')
        if b:
            time.sleep(0.8)

    def get_block_trade(self, date):
        if self.original.hexists(date, "block_trade"):
            return pickle.loads(zlib.decompress(self.original.hget(date, 'block_trade')))
        return pd.DataFrame()

    def download_stk_holdertrade(self, date):
        self.check_exists_and_save(self.original, self.pro.stk_holdertrade, date, 'stk_holdertrade')

    def check_all_download_data(self):
        ds_date = self.get_trade_cal_list()
        for d in ds_date:
            dk = self.original.hkeys(d)
            for f in dk:
                fs = f.decode()
                data = pickle.loads(zlib.decompress(self.original.hget(d, fs)))
                if data.empty is True:
                    print("del empty: ", d, fs)
                    self.original.hdel(d, fs)

    # expand daily
    def expand_date_daily(self, date):
        if date < "20151001":
            return

        if self.temp.hexists(date, "expand"):
            return

        df = self.get_daily(date)
        if df.empty is True:
            return

        for i in df.index:
            d = df.loc[i]
            c = d['ts_code']
            if self.expand.hexists(date, c) is False:
                self.expand.hset(date, c, zlib.compress(pickle.dumps(d), 5))

        self.temp.hset(date, "expand", "ok")
        print("expand_date_daily: ", date, " ok")

    # ********************************************************************
    # 下载数据
    # 下载时间短的
    def download_all_data(self):
        ds_date = self.get_trade_cal_list("20080101")
        print("start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])
        for d in ds_date:
            self.download_stk_limit(d)
            self.download_index_daily_all(d)
            self.download_top_list(d)
            self.download_top_inst(d)
            self.download_daily(d)
            self.download_block_trade(d)
            self.expand_date_daily(d)

    # 时间长的单独下载
    def download_all_data2_save(self):
        ds_date = self.get_trade_cal_list()
        print("start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])
        for d in ds_date:
            self.download_hk_hold(d)
            # self.download_stk_holdertrade(d)

    # ********************************************************************
    def get_date_up_limit_ts_code_df(self, date):
        dfd = self.get_daily(date)
        dfs = self.get_stk_limit(date)
        df = pd.merge(dfd, dfs, on='ts_code')
        df = df[(df.close == df.up_limit) & (df.pct_chg > 6.0) & (df.pct_chg < 12.0)]
        df = df['ts_code']
        df = df.reset_index(drop=True)
        return df

    def handle_date_training_data_save(self, date):
        if self.temp.hexists(date, "train_data"):
            return

        ds_date = self.get_trade_cal_list(date)
        if ds_date.shape[0] < 2:
            print("skip: ", date)
            return

        next_day = ds_date[1]
        dr = self.original.hexists(next_day, 'daily')
        if dr is False:
            print("skip: ", date, next_day)
            return

        df = self.get_date_up_limit_ts_code_df(date)

        for i in range(df.shape[0]):
            c = df.iat[i]
            ret = self.train_data.hexists(date, c)
            if ret is False:
                dnf = self.get_stock_list_date_n(c, next_day)
                if dnf.shape[0] == 41:
                    self.train_data.hset(date, c, zlib.compress(pickle.dumps(dnf), 5))
                    print("handle_date_training_data_save: ", date, c)
        self.temp.hset(date, "train_data", "ok")

    def get_date_stock_num(self, date, code):
        if self.expand.hexists(date, code) is False:
            return pd.DataFrame()
        return pickle.loads(zlib.decompress(self.expand.hget(date, code)))

    def handle_training_data_all_save(self):
        cal = self.get_trade_cal_list('20160101')
        for d in cal:
            self.handle_date_training_data_save(d)

    #
    def get_train_data_df(self, date, code):
        if self.train_data.hexists(date, code) is False:
            return pd.DataFrame()
        return pickle.loads(zlib.decompress(self.train_data.hget(date, code)))

    def get_date_up_limit_num(self, date):
        c = self.train_data.hkeys(date)
        return len(c)

    def get_all_train_data_list(self):
        return pickle.loads(zlib.decompress(self.temp.get("all_train_data")))

    def save_all_train_data_list(self):
        print("save_all_train_data_list: ")
        ldf = []
        keys = self.train_data.keys("20*")
        keys.sort()
        for k in keys:
            hkeys = self.train_data.hkeys(k)
            hkeys.sort()
            for hk in hkeys:
                df = pickle.loads(zlib.decompress(self.train_data.hget(k, hk)))
                ldf.append(df)
        self.temp.set("all_train_data", zlib.compress(pickle.dumps(ldf), 5))

    def gen_latest_data_for_predictor(self):
        dlist = self.get_trade_cal_list()
        dlist = dlist[-2:]
        d = dlist.iat[1]
        td = self.get_today_date()

        h = datetime.datetime.now().strftime("%H")

        if td == d and h < "18":
            d = dlist.iat[0]
            print("use previous trade day data: ", d)

        if self.temp.exists("predictor_date"):
            if self.temp.get("predictor_date").decode() == d:
                return

        df = self.get_date_up_limit_ts_code_df(d)

        ldf = []
        for i in range(df.shape[0]):
            c = df.iat[i]
            dnf = self.get_stock_list_date_n(c, d)
            if dnf.empty is False:
                dnf = dnf.tail(40)
                dnf = dnf.reset_index(drop=True)
                if dnf.shape[0] >= 40:
                    ldf.append(dnf)
                    print("gen for predictor:  ", d, c)

        self.temp.set("predictor", zlib.compress(pickle.dumps(ldf), 5))
        self.temp.set("predictor_date", d)

    def get_latest_data_for_predictor(self):
        ldf = []
        if self.temp.exists("predictor") is False:
            return ldf
        return pickle.loads(zlib.decompress(self.temp.get("predictor")))


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    A = stockdata()
    if len(sys.argv) > 1:
        # check data del empty
        if sys.argv[1] == 'c':
            A.check_all_download_data()
        # download index_daily top stk daily
        elif sys.argv[1] == 'd1':
            A.download_stock_basic()
            A.download_trade_cal_list()
            A.download_all_data()
            A.gen_latest_data_for_predictor()
        # download other
        elif sys.argv[1] == 'd2':
            A.download_all_data2_save()
        # handle training data
        elif sys.argv[1] == 'h':
            A.handle_training_data_all_save()
            A.save_all_train_data_list()
    else:
        d = "Test: ................."
        # A.gen_latest_data_for_predictor()
        # d = A.get_latest_data_for_predictor()
        # d = A.get_trade_cal_list()
        # d = A.get_date_stock_num('20190920', '600818.SH')
        # A.handle_date_training_data_save('20170103')
        # a = A.get_stock_list_date_n('600818.SH', '20190918')
        a = A.get_train_data_df("20170822", "002600.SZ")
        # A.expand_date_daily("20190916")
        # a = pickle.loads(zlib.decompress(A.expand.hget("20180919", "600818.SH")))
        print(a)
        # a = A.get_date_up_limit_num('20190919')
        # print(d, d.shape[0])
        # A.get_index_daily_all('20170105')
        # d = A.get_stock_basics()
        # print(d)
    print("Time taken:", datetime.datetime.now() - startTime)
