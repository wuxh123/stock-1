#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  stockdata.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-18 16:07:49
#  Last Modified:  2019-09-10 11:30:44
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

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
        self.redis = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379)

    def get_index_daily_start_end_date(self, code, st, end):
        return self.pro.index_daily(ts_code=code, start_date=st, end_date=end)

    def get_stock_number_start_end_date(self, code, st, end):
        return self.pro.daily(ts_code=code, start_date=st, end_date=end)

    def get_stock_number_date_last_ndays(self, code, date, ndays):
        end = datetime.datetime.strptime(date, "%Y%m%d")
        st = end - datetime.timedelta(days=ndays * 2)
        st = st.strftime("%Y%m%d")
        end = end.strftime("%Y%m%d")
        df = self.get_stock_number_start_end_date(code, st, end)
        if df.shape[0] <= ndays:
            return df

        return df[0:ndays]

    def get_stock_basics(self):
        return self.pro.get_stock_basics()

    def get_today_date(self):
        return datetime.datetime.now().strftime("%Y%m%d")

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

    def get_date_limitdown(self, date):
        dfd = self.pro.daily(trade_date=date)
        dfd = dfd[['ts_code', 'close', 'pct_chg']]
        dfs = self.pro.stk_limit(trade_date=date)
        dfs = dfs[['ts_code', 'down_limit']]
        df = pd.merge(dfd, dfs, on='ts_code')
        df = df[(df.close == df.down_limit) & (df.pct_chg < -6.0) & (df.pct_chg > -12.0)]
        return df

    def get_date_stock_info(self, date):
        return self.pro.daily(trade_date=date)

    def get_top_list_save(self, date):
        df = self.pro.top_list(trade_date=date)
        re = self.redis.hexists(date, 'top_list')
        if re == 0:
            print("downloading: top_list", date)
            self.redis.hset(date, 'top_list', zlib.compress(pickle.dumps(df), 5))
        return df

    def get_trade_cal(self, st):
        todaydate = datetime.datetime.now().strftime("%Y%m%d")
        df = self.pro.query('trade_cal', start_date=st, end_date=todaydate)
        df = df[df.is_open == 1]
        df = df.reset_index(drop=True)
        return df

    def get_one_day_data_save(self, date):
        df_tscode = self.get_date_limitup(date)
        df_tscode = df_tscode['ts_code']
        df_tscode = df_tscode.reset_index(drop=True)

        for i in df_tscode.index:
            c = df_tscode.loc[i]
            re = self.redis.hexists(date, c)

            if (re == 0):
                print("downloading: ", date, c)
                df = self.get_stock_number_date_last_ndays(c, date, 40)
                self.redis.hset(date, c, zlib.compress(pickle.dumps(df), 5))
                time.sleep(0.1)

    def get_all_data_save(self):
        ds = "20170101"

        rd = self.redis.keys("20*")
        if rd:
            rd.sort()
            ds = rd[-1].decode()

        ds_date = self.get_trade_cal(ds)
        ds_date = ds_date['cal_date']
        ds_date = ds_date.reset_index(drop=True)

        print("start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])

        '''
        for i in ds_date.index:
            d = ds_date.loc[i]
            self.get_one_day_data_save(d)
            # self.redis.save()
        '''


A = stockdata()
# df = A.get_stock_number_date_last_ndays('600818.sh', '20150101', 40)
# df = A.get_index_daily_start_end_date('000001.SH', '20190501', '20190601')
startTime = datetime.datetime.now()
# A.get_one_day_data_save('20190903')
A.get_all_data_save()
# d = A.get_date_limitup('20190909')
# d = A.get_top_list_save('20190909')
# print(d)
# d = A.get_trade_cal("20190901")
# print(d[['ts_code', 'pre_close', 'close']])
# print(d.shape)
print("Time taken:", datetime.datetime.now() - startTime)
'''
print(df.shape)
print(df.size)
print(df[0:3])
'''

'''
# a = A.GetData('600818.sh', 20)
a = A.GetDateLimitUp('20190903')
r.hset('20190903', 'limitup', zlib.compress(pickle.dumps(a), 5))
df = pickle.loads(zlib.decompress(r.hget('20190903', 'limitup')))
# print(df)

d1 = r.hget('20190901', 'limitup')
if (d1 is None):
    print('none data')
else:
    print(d1)
'''
# /usr/local/var/db/redis/dump.rdb
# a.to_excel("a.xlsx", sheet_name="Sheet1", engine='xlsxwriter')
# a = A.GetDateLimitUp('20190830')
# a = A.GetDateLimitDown('20190830')
# print(a)
