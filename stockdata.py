#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  stockdata.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-18 16:07:49
#  Last Modified:  2019-10-12 17:26:39
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
        self.hdf5 = "data.h5"
        # 原始数据存数据库
        # self.original = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=0)
        self.original = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=0)
        self.temp = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=1)
        self.train = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=2)
        self.test = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=3)

    def get_today_date(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    def download_stock_basic(self):
        df = self.pro.stock_basic(list_status="L")
        df = df.reset_index(drop=True)
        self.original.set('stock_basic', zlib.compress(pickle.dumps(df), 5))
        print("save:", 'stock_basic ok')

    def get_stock_basics(self):
        return pickle.loads(zlib.decompress(self.original.get('stock_basic')))

    def download_trade_cal_list(self):
        td = self.get_today_date()
        df = self.pro.query('trade_cal', is_open=1, end_date=td)
        df = df['cal_date']
        df = df.reset_index(drop=True)
        self.original.set('trade_cal', zlib.compress(pickle.dumps(df), 5))
        print("save:", 'trade_cal ok')

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

    def get_index_daily_by_code(self, code):
        if code[0] == '0':
            return self.get_index_daily_sz()
        elif code[0] == '3':
            return self.get_index_daily_cyb()
        return self.get_index_daily_sh()

    def download_hk_hold(self, date):
        b = self.check_exists_and_save(self.original, self.pro.hk_hold, date, 'hk_hold')
        if b:
            time.sleep(41)

    def download_block_trade(self, date):
        if date < "20160108":
            return
        if date == self.get_today_date():
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

    def download_data_by_code(self, code):
        td = self.get_today_date()
        data = self.pro.daily(ts_code=code, start_date='20000101', end_date=td)
        if data.empty is False:
            self.original.set(code, zlib.compress(pickle.dumps(data), 5))
            print("save: ", td, code, 'download_data_by_code', " ok")

    def update_all_code_data(self):
        lds = self.original.get('latest_date').decode()
        cal = self.get_trade_cal_list(st_date=lds)
        cal = cal[cal > lds].tolist()

        ac = self.get_stock_basics()
        ac = ac[ac.market != '科创板']['ts_code'].tolist()
        ac.sort()

        for d in cal:
            di = self.get_daily(d)
            if di.empty:
                continue
            for c in ac:
                print("update: ", d, c)
                dc = self.get_data_by_code(c)
                da = di[di.ts_code == c]
                if da.empty:
                    continue
                da = da.append(dc, ignore_index=True)
                self.original.set(c, zlib.compress(pickle.dumps(da), 5))

    def get_all_code(self):
        return self.original.keys("*.*")

    def get_data_by_code(self, code):
        if self.original.exists(code) == 1:
            return pickle.loads(zlib.decompress(self.original.get(code)))
        return self.pro.daily(ts_code=code)

    def get_date_up_limit_data_df_list(self, date):
        return self.get_date_up_limit_ts_code_df(date).values.tolist()

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

        self.update_all_code_data()
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
    def init_all_data_to_h5(self):
        df = self.pro.stock_basic(list_status="L").reset_index(drop=True)
        ac = df['ts_code'].tolist()
        ac.sort()

        s = pd.HDFStore(self.hdf5, 'a', complevel=9, complib='blosc')
        skey = s.keys()
        skey.sort()

        for c in ac:
            h5key = '/' + c[-2:] + c[:-3]
            if h5key in skey:
                continue
            df = self.pro.daily(ts_code=c)
            # df = self.pro.daily(ts_code=c, end_date='20191008')
            time.sleep(0.3)
            if df.empty:
                continue
            df.sort_values(by='trade_date', ascending=True, inplace=True)
            s.put(h5key, df, format='t', append=True, data_columns=True)
            print("save:", h5key, c, "to", self.hdf5)
        s.close()

    def update_all_data_h5(self):
        ac = self.pro.stock_basic(list_status="L").reset_index(drop=True)['ts_code'].tolist()
        ac.sort()

        c = ac[0]
        ac1_h5_key = c[-2:] + c[:-3]

        s = pd.HDFStore(self.hdf5, 'a', complevel=5, complib='lzo')
        ld = s[ac1_h5_key].tail(1)['trade_date'].reset_index(drop=True).loc[0]

        td = self.get_today_date()
        cal = self.pro.query('trade_cal', is_open=1, start_date=ld, end_date=td)
        cal = cal[cal.cal_date > ld]
        cal = cal['cal_date'].reset_index(drop=True)
        cal.sort_values(ascending=True, inplace=True)
        cal = cal.tolist()

        for d in cal:
            dfd = self.pro.daily(trade_date=d)
            dfd.sort_values(by="ts_code", ascending=True, inplace=True)
            code_list = dfd['ts_code'].tolist()
            for c in code_list:
                h5key = c[-2:] + c[:-3]
                dfc = dfd[dfd.ts_code == c]
                s.put(h5key, dfc, format='t', append=True, data_columns=True)
                print("update:", d, h5key, c)
        s.close()

    def update_index_daily(self):
        with pd.HDFStore('data.h5', 'a') as s:
            df = self.pro.index_daily(ts_code='000001.SH')[::-1].reset_index(drop=True)
            s.put('SH', df, format='t', append=False, data_columns=True)
            df = self.pro.index_daily(ts_code='399001.SZ')[::-1].reset_index(drop=True)
            s.put('SZ', df, format='t', append=False, data_columns=True)
            df = self.pro.index_daily(ts_code='399006.SZ')[::-1].reset_index(drop=True)
            s.put('CYB', df, format='t', append=False, data_columns=True)

    def test2(self):
        df = self.pro.index_daily(ts_code='000001.SH')[::-1].reset_index(drop=True)
        # df.sort_values(by='trade_date', ascending=True, inplace=True)
        # df = df.reset_index(drop=True)
        print(df)

    def test3(self):
        with pd.HDFStore('data.h5', 'a') as s:
            # df = self.pro.index_daily(ts_code='000001.SH')
            # df.sort_values(by='trade_date', ascending=True, inplace=True)
            # df = df.reset_index(drop=True)
            # s.put('SH', df, format='t', append=False, data_columns=True)
            df = s['CYB']
            df = df[df.trade_date > '20190901']
            # df.to_excel('a.xlsx', encoding='utf-8', index=False)
            # s['SH'] = df
            # df = s['SZ000005']
            # df.sort_values(by='trade_date', ascending=True, inplace=True)
            # dfd = self.pro.daily(trade_date='20191008')
            # dfd = dfd[dfd.ts_code == '000005.SZ']
            print(df)
            print(type(df))


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
        elif sys.argv[1] == 'hi':
            A.init_all_data_to_h5()
        elif sys.argv[1] == 'hu':
            A.update_all_data_h5()
    else:
        d = "Test: ................."
        # d = A.get_trade_cal_list()
        # A.download_data_by_code('000058.SZ')
        # A.download_data_by_code('002464.SZ')
        # A.update_all_code_data()
        # d = A.get_data_by_code('600818.SH')
        # A.init_all_data_to_h5()
        # A.update_all_data_h5()
        # A.update_index_daily()
        # A.test2()
        A.test3()
        # d = A.get_all_code()
        # d = A.get_date_up_limit_ts_code_df('20190925')
        # A.get_date_up_limit_data_df_list('20190924')
        # A.get_all_code()
        # A.download_all_date_up_limit_history_data()
        print(d)
    print("Time taken:", datetime.datetime.now() - startTime)
