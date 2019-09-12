#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  stockdata.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-18 16:07:49
#  Last Modified:  2019-09-12 14:23:27
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
        # 起始日期
        self.st_date = "20140101"
        # ts 接口
        self.pro = ts.pro_api()
        # 原始数据存数据库0
        self.r0 = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=0)
        # 加工后的数据存数据库1
        self.r1 = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=1)

    def get_today_date(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    def get_trade_cal_list(self):
        todaydate = self.get_today_date()
        df = self.pro.query('trade_cal', start_date=self.st_date, end_date=todaydate)
        df = df[df.is_open == 1]
        df = df['cal_date']
        df = df.reset_index(drop=True)
        return df

    #                                          date name
    def check_exists_and_save(self, rds, func, key, field):
        re = rds.hexists(key, field)
        if re == 0:
            print("downloading: ", key, field)
            data = func(trade_date=key)
            if data.empty is False:
                rds.hset(key, field, zlib.compress(pickle.dumps(data), 5))

    # 000001.SH 399001.SZ 399006.SZ
    def get_index_daily_save(self, code, date):
        re = self.r0.hexists(date, code)
        if re == 0:
            print("downloading: ", date, code)
            data = self.pro.index_daily(ts_code=code, trade_date=date)
            if data.empty is False:
                self.r0.hset(date, code, zlib.compress(pickle.dumps(data), 5))
                time.sleep(0.6)

    def get_index_daily_save_all(self, date):
        self.get_index_daily_save('000001.SH', date)
        self.get_index_daily_save('399001.SZ', date)
        self.get_index_daily_save('399006.SZ', date)

    def get_top_list_save(self, date):
        self.check_exists_and_save(self.r0, self.pro.top_list, date, 'top_list')

    def get_top_inst_save(self, date):
        self.check_exists_and_save(self.r0, self.pro.top_inst, date, 'top_inst')

    def get_stk_limit_save(self, date):
        self.check_exists_and_save(self.r0, self.pro.stk_limit, date, 'stk_limit')

    def get_daily_save(self, date):
        self.check_exists_and_save(self.r0, self.pro.daily, date, 'daily')

    def get_hk_hold_save(self, date):
        self.check_exists_and_save(self.r0, self.pro.hk_hold, date, 'hk_hold')
        time.sleep(31)

    def get_block_trade_save(self, date):
        self.check_exists_and_save(self.r0, self.pro.block_trade, date, 'block_trade')

    def get_stk_holdertrade_save(self, date):
        self.check_exists_and_save(self.r0, self.pro.stk_holdertrade, date, 'stk_holdertrade')

    def test(self, date):
        data = self.pro.hk_hold(trade_date=date)
        print(data)

    def check_all_data(self):
        ds_date = self.get_trade_list()

        for d in ds_date:
            dk = self.r0.hkeys(d)
            for f in dk:
                fs = f.decode()
                data = pickle.loads(zlib.decompress(self.r0.hget(d, fs)))
                if data.empty is True:
                    print("del empty: ", d, fs)
                    self.r0.hdel(d, fs)

    # ********************************************************************

    def get_stock_number_start_end_date(self, code, st, end):
        return self.pro.daily(ts_code=code, start_date=st, end_date=end)

    def get_stock_number_date_last_ndays(self, code, date, ndays):
        end = datetime.datetime.now()
        st = end - datetime.timedelta(days=ndays * 2)
        st = st.strftime("%Y%m%d")
        end = end.strftime("%Y%m%d")
        df = self.get_stock_number_start_end_date(code, st, end)
        if df.shape[0] <= ndays:
            return df
        return df[0:ndays]

    def get_stock_basics(self):
        return self.pro.get_stock_basics()

    def get_date_limitup(self, date):
        dfd = self.pro.daily(trade_date=date)
        dfd = dfd[['ts_code', 'close', 'pct_chg']]
        dfs = self.pro.stk_limit(trade_date=date)
        dfs = dfs[['ts_code', 'up_limit']]
        df = pd.merge(dfd, dfs, on='ts_code')
        df = df[(df.close == df.up_limit) & (df.pct_chg > 6.0) & (df.pct_chg < 12.0)]
        df = df['ts_code']
        df = df.reset_index(drop=True)
        return df

    def get_one_day_data_save(self, date):
        df_tscode = self.get_date_limitup(date)
        for i in df_tscode.index:
            c = df_tscode.loc[i]
            re = self.r.hexists(date, c)
            if (re == 0):
                print("downloading: ", date, c)
                df = self.get_stock_number_date_last_ndays(c, date, 40)
                self.r.hset(date, c, zlib.compress(pickle.dumps(df), 5))
                time.sleep(0.15)

    def get_all_data_save(self):
        ds_date = self.get_trade_cal_list()
        print(" start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])

        for d in ds_date:
            self.get_index_daily_save_all(d)
            self.get_top_list_save(d)
            self.get_top_inst_save(d)
            self.get_stk_limit_save(d)
            self.get_daily_save(d)

    def get_all_data2_save(self):
        ds_date = self.get_trade_cal_list()
        print(" start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])

        for d in ds_date:
            self.get_hk_hold_save(d)
            self.get_block_trade_save(d)
            # self.get_stk_holdertrade_save(d)


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    A = stockdata()
    if len(sys.argv) > 1:
        # check data del empty
        if sys.argv[1] == 'c':
            A.check_all_data()
        # download index_daily top stk daily
        elif sys.argv[1] == 'd1':
            A.get_all_data_save()
        # download other
        elif sys.argv[1] == 'd2':
            pass
    else:
        d = A.get_trade_cal_list()
        print(d)

    print("Time taken:", datetime.datetime.now() - startTime)
