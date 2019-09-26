#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  stockdata.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-18 16:07:49
#  Last Modified:  2019-09-26 16:03:48
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

    def get_today_date(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    def download_stock_basic(self):
        df = self.pro.stock_basic(list_status="L")
        df = df.reset_index(drop=True)
        self.original.set('stock_basic', zlib.compress(pickle.dumps(df), 5))
        print("save: ", 'stock_basic ok')

    def get_stock_basics(self):
        return pickle.loads(zlib.decompress(self.original.get('stock_basic')))

    def download_trade_cal_list(self):
        todaydate = self.get_today_date()
        df = self.pro.query('trade_cal', start_date='20000101', end_date=todaydate)
        df = df[df.is_open == 1]
        df = df['cal_date']
        df = df.reset_index(drop=True)
        self.original.set('trade_cal', zlib.compress(pickle.dumps(df), 5))
        print("save: ", 'trade_cal ok')

    def get_trade_cal_list(self, st_date='20000101'):
        cal = pickle.loads(zlib.decompress(self.original.get('trade_cal')))
        cal = cal[cal >= st_date]
        cal = cal.reset_index(drop=True)
        return cal

    # ********************************
    #                                          date name
    def check_exists_and_save(self, rds, func, key, field):
        if rds.hexists(key, field) == 0:
            data = func(trade_date=key)
            if data.empty is False:
                rds.hset(key, field, zlib.compress(pickle.dumps(data), 5))
                print("save: ", key, field, " ok")
                return True
            print("save: ", key, field, " error")
        return False

    def download_top_list(self, date):
        self.check_exists_and_save(self.original, self.pro.top_list, date, 'top_list')

    def get_top_list(self, date):
        if self.original.hexists(date, 'top_list') == 1:
            return pickle.loads(zlib.decompress(self.original.hget(date, 'top_list')))
        return pd.DataFrame()

    def download_top_inst(self, date):
        self.check_exists_and_save(self.original, self.pro.top_inst, date, 'top_inst')

    def get_top_inst(self, date):
        if self.original.hexists(date, 'top_inst') == 1:
            return pickle.loads(zlib.decompress(self.original.hget(date, 'top_inst')))
        return pd.DataFrame()

    def download_stk_limit(self, date):
        self.check_exists_and_save(self.original, self.pro.stk_limit, date, 'stk_limit')

    def get_stk_limit(self, date):
        if self.original.hexists(date, 'stk_limit') == 1:
            return pickle.loads(zlib.decompress(self.original.hget(date, 'stk_limit')))
        return pd.DataFrame()

    def download_daily(self, date):
        self.check_exists_and_save(self.original, self.pro.daily, date, 'daily')

    def get_daily(self, date):
        if self.original.hexists(date, 'daily') == 1:
            return pickle.loads(zlib.decompress(self.original.hget(date, 'daily')))
        return pd.DataFrame()

    # 000001.SH 399001.SZ 399006.SZ
    def download_index_daily_sh(self):
        df = self.pro.index_daily(ts_code='000001.SH', start_date='20000101')
        if df.empty is False:
            self.original.set('sh', zlib.compress(pickle.dumps(df), 5))
            print("save: sh: ok")

    def download_index_daily_sz(self):
        df = self.pro.index_daily(ts_code='399001.SZ', start_date='20000101')
        if df.empty is False:
            self.original.set('sz', zlib.compress(pickle.dumps(df), 5))
            print("save: sz: ok")

    def download_index_daily_cyb(self):
        df = self.pro.index_daily(ts_code='399006.SZ', start_date='20000101')
        if df.empty is False:
            self.original.set('cyb', zlib.compress(pickle.dumps(df), 5))
            print("save: cyb: ok")

    def download_index_daily_all(self):
        self.download_index_daily_sh()
        self.download_index_daily_sz()
        self.download_index_daily_cyb()

    def get_index_daily_sh(self):
        return pickle.loads(zlib.decompress(self.original.get('sh')))

    def get_index_daily_sz(self):
        return pickle.loads(zlib.decompress(self.original.get('sz')))

    def get_index_daily_cyb(self):
        return pickle.loads(zlib.decompress(self.original.get('cyb')))

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
        if self.original.hexists(date, "block_trade") == 1:
            return pickle.loads(zlib.decompress(self.original.hget(date, 'block_trade')))
        return pd.DataFrame()

    def download_stk_holdertrade(self, date):
        self.check_exists_and_save(self.original, self.pro.stk_holdertrade, date, 'stk_holdertrade')

    def calc_date_up_limit_ts_code_df(self, date):
        dfd = self.get_daily(date)
        if dfd.empty:
            return pd.DataFrame()
        dfs = self.get_stk_limit(date)
        df = pd.merge(dfd, dfs, on='ts_code')
        df = df[(df.close == df.up_limit) & (df.pct_chg > 6.0) & (df.pct_chg < 12.0)]
        df = df['ts_code']
        df = df.reset_index(drop=True)
        return df

    def save_date_up_limit_ts_code_df(self, date):
        if self.original.hexists(date, "up_limit_list") == 0:
            data = self.calc_date_up_limit_ts_code_df(date)
            if data.empty is False:
                self.original.hset(date, 'up_limit_list', zlib.compress(pickle.dumps(data), 5))
                print("save: ", date, 'up_limit_list', " ok")

    def get_date_up_limit_ts_code_df(self, date):
        if self.original.hexists(date, "up_limit_list") == 1:
            return pickle.loads(zlib.decompress(self.original.hget(date, 'up_limit_list')))
        return pd.DataFrame()

    def download_date_up_limit_history_data(self, date):
        ld = self.original.get('latest_date')
        if date <= ld.decode():
            return

        if self.original.hexists(date, 'daily') == 0:
            return

        td = self.get_today_date()
        df = self.get_date_up_limit_ts_code_df(date).values.tolist()
        for c in df:
            data = self.pro.daily(ts_code=c, start_date='20000101', end_date=td)
            if data.empty is False:
                self.original.set(c, zlib.compress(pickle.dumps(data), 5))
                print("save: ", date, c, 'up_limit_list daily', " ok")

    def get_all_code(self):
        ll = self.original.keys("*.*")
        return ll

    def get_data_by_code(self, code):
        if self.original.exists(code) == 1:
            return pickle.loads(zlib.decompress(self.original.get(code)))
        return pd.DataFrame()

    def get_date_up_limit_data_df_list(self, date):
        dl = self.get_date_up_limit_ts_code_df(date).values.tolist()
        lc = []
        if len(dl) == 0:
            return lc

        for c in dl:
            pass

        return lc

    '''
    def download_all_date_up_limit_history_data(self):
        ds_date = self.get_trade_cal_list('20090101')
        ds_date = ds_date.sort_values(ascending=False)
        ds_date = ds_date.reset_index(drop=True)
        dl = ds_date.values.tolist()
        for d in dl:
            self.download_date_up_limit_history_data(d)
    '''

    # use local redis db
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

    # ********************************************************************
    # 下载时间短的
    def download_all_data(self):
        ds_date = self.get_trade_cal_list("20080101")
        end_date = ds_date[ds_date.shape[0] - 1]
        print("start_date: ", ds_date[0], "end_date: ", end_date)
        for d in ds_date:
            self.download_stk_limit(d)
            self.download_daily(d)
            self.download_top_list(d)
            self.download_top_inst(d)
            self.download_block_trade(d)
            self.save_date_up_limit_ts_code_df(d)
            self.download_date_up_limit_history_data(d)

        if self.original.hexists(end_date, 'daily') == 1:
            self.original.set("latest_date", end_date)

    # 时间长的单独下载
    def download_all_data2_save(self):
        ds_date = self.get_trade_cal_list()
        print("start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])
        for d in ds_date:
            self.download_hk_hold(d)
            # self.download_stk_holdertrade(d)

    # ********************************************************************


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    A = stockdata()
    if len(sys.argv) > 1:
        if sys.argv[1] == 'c':
            A.check_all_download_data()
        elif sys.argv[1] == 'd1':
            A.download_stock_basic()
            A.download_trade_cal_list()
            A.download_index_daily_all()
            A.download_all_data()
        elif sys.argv[1] == 'd2':
            A.download_all_data2_save()
    else:
        d = "Test: ................."
        # d = A.get_trade_cal_list()
        # a = A.get_data_by_code('600818.SH')
        # a = A.get_date_up_limit_ts_code_df('20190925')
        # A.get_date_up_limit_data_df_list('20190924')
        A.get_all_code()
        # A.download_all_date_up_limit_history_data()
        # A.download_date_up_limit_history_data('20190925')
        # print(a)
    print("Time taken:", datetime.datetime.now() - startTime)
