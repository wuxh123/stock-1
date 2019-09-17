#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  stockdata.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-18 16:07:49
#  Last Modified:  2019-09-17 13:21:13
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
        # ts 接口
        self.pro = ts.pro_api()
        # 原始数据存数据库0
        self.r0 = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=0)
        # self.r0 = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=0)
        # 数据处理标志存数据库1
        self.r1 = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=1)
        # self.r1 = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=1)
        # 训练用数据存数据库2
        self.r2 = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=2)
        # self.r2 = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=2)

    def get_today_date(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    def download_stock_basic(self):
        df = self.pro.stock_basic(list_status="L")
        df = df.reset_index(drop=True)
        self.r0.set('stock_basic', zlib.compress(pickle.dumps(df), 5))
        print("save: ", 'stock_basic ok')

    def get_stock_basics(self):
        df = pickle.loads(zlib.decompress(self.r0.get('stock_basic')))
        return df

    def download_trade_cal_list(self):
        todaydate = self.get_today_date()
        df = self.pro.query('trade_cal', start_date='20130101', end_date=todaydate)
        df = df[df.is_open == 1]
        df = df['cal_date']
        df = df.reset_index(drop=True)
        self.r0.set('trade_cal', zlib.compress(pickle.dumps(df), 5))
        print("save: ", 'trade_cal ok')
        return df

    def get_trade_cal_list(self, st_date='20130101'):
        cal = pickle.loads(zlib.decompress(self.r0.get('trade_cal')))
        cal = cal[cal >= st_date]
        cal = cal.reset_index(drop=True)
        return cal

    # 获取某代码到某天上市数据
    def get_stock_list_date_n(self, code, date):
        da = self.get_stock_basics()
        da = da[da.ts_code == code]
        if da.empty is True:
            return 0
        da = da['list_date'].iat[0]
        df = self.pro.daily(ts_code=code, start_date=da, end_date=date)
        df = df.head(41)
        df = df.sort_values("trade_date", ascending=True)
        df = df.reset_index(drop=True)
        return df

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
        re = self.r0.hexists(date, code)
        if re == 0:
            data = self.pro.index_daily(ts_code=code, trade_date=date)
            time.sleep(0.6)
            if data.empty is False:
                self.r0.hset(date, code, zlib.compress(pickle.dumps(data), 5))
                print("save: ", date, code, " ok")
            else:
                print("save: ", date, code, " error")

    def download_index_daily_all(self, date):
        self.download_index_daily('000001.SH', date)
        self.download_index_daily('399001.SZ', date)
        self.download_index_daily('399006.SZ', date)

    def get_index_daily_all(self, date):
        d1 = pickle.loads(zlib.decompress(self.r0.hget(date, '000001.SH')))
        d2 = pickle.loads(zlib.decompress(self.r0.hget(date, '399001.SZ')))
        d3 = pickle.loads(zlib.decompress(self.r0.hget(date, '399006.SZ')))
        d1 = d1.append([d2, d3])
        d1 = d1.reset_index(drop=True)
        print(d1)
        return d1

    def download_top_list(self, date):
        self.check_exists_and_save(self.r0, self.pro.top_list, date, 'top_list')

    def get_top_list(self, date):
        df = pickle.loads(zlib.decompress(self.r0.hget(date, 'top_list')))
        return df

    def download_top_inst(self, date):
        self.check_exists_and_save(self.r0, self.pro.top_inst, date, 'top_inst')

    def get_top_inst(self, date):
        df = pickle.loads(zlib.decompress(self.r0.hget(date, 'top_inst')))
        return df

    def download_stk_limit(self, date):
        self.check_exists_and_save(self.r0, self.pro.stk_limit, date, 'stk_limit')

    def get_stk_limit(self, date):
        df = pickle.loads(zlib.decompress(self.r0.hget(date, 'stk_limit')))
        return df

    def download_daily(self, date):
        self.check_exists_and_save(self.r0, self.pro.daily, date, 'daily')

    def get_daily(self, date):
        df = pickle.loads(zlib.decompress(self.r0.hget(date, 'daily')))
        return df

    def download_hk_hold(self, date):
        b = self.check_exists_and_save(self.r0, self.pro.hk_hold, date, 'hk_hold')
        if b:
            time.sleep(41)

    def download_block_trade(self, date):
        b = self.check_exists_and_save(self.r0, self.pro.block_trade, date, 'block_trade')
        if b:
            time.sleep(0.8)

    def get_block_trade(self, date):
        df = pickle.loads(zlib.decompress(self.r0.hget(date, 'block_trade')))
        return df

    def download_stk_holdertrade(self, date):
        self.check_exists_and_save(self.r0, self.pro.stk_holdertrade, date, 'stk_holdertrade')

    def check_all_download_data(self):
        ds_date = self.get_trade_cal_list()
        for d in ds_date:
            dk = self.r0.hkeys(d)
            for f in dk:
                fs = f.decode()
                data = pickle.loads(zlib.decompress(self.r0.hget(d, fs)))
                if data.empty is True:
                    print("del empty: ", d, fs)
                    self.r0.hdel(d, fs)

    # ********************************************************************
    # 下载数据
    # 下载时间短的
    def download_all_data(self):
        ds_date = self.get_trade_cal_list()
        print("start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])
        for d in ds_date:
            self.download_index_daily_all(d)
            self.download_top_list(d)
            self.download_top_inst(d)
            self.download_stk_limit(d)
            self.download_daily(d)
            self.download_block_trade(d)

    # 时间长的单独下载
    def get_all_data2_save(self):
        ds_date = self.get_trade_cal_list()
        print("start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])
        for d in ds_date:
            self.download_hk_hold(d)
            # self.download_stk_holdertrade(d)

    # ********************************************************************
    # 处理数据 从数据库0 读取数据 处理好后 存到数据库2 ,db1 保存标志
    def handle_date_trainning_data_save(self, date):
        if self.r1.hexists(date, "train_data"):
            return

        ds_date = self.get_trade_cal_list(date)
        if ds_date.shape[0] < 2:
            print("skip: ", date)
            return

        next_day = ds_date[1]
        dr = self.r0.hexists(next_day, 'daily')

        if dr is True:
            dfd = self.get_daily(next_day)
            dfs = self.get_stk_limit(next_day)
            df = pd.merge(dfd, dfs, on='ts_code')
            df = df[(df.close == df.up_limit) & (df.pct_chg > 6.0) & (df.pct_chg < 12.0)]
            df = df['ts_code']
            df = df.reset_index(drop=True)

            for i in range(df.shape[0]):
                c = df.iat[i]
                ret = self.r2.hexists(date, c)
                if ret is False:
                    dnf = self.get_stock_list_date_n(c, next_day)
                    if dnf.shape[0] == 41:
                        self.r2.hset(date, c, zlib.compress(pickle.dumps(dnf), 5))
                        print("handle_uplimit_last_40days_data_save: ", date, c)
        self.r1.hset(date, "train_data", "ok")

    # 处理数据
    # 从 数据库0 查询 获取某天某代码
    def get_date_stock_num(self, date, code):
        ret = self.r0.hexists(date, 'daily')
        if ret == 0:
            return None
        df = self.get_daily(date)
        df = df[df.ts_code == code]
        if df.empty is False:
            return df
        return None

    # data for trainning save in db1
    def handle_trainning_data_all_save(self):
        cal = self.get_trade_cal_list('20170101')
        for d in cal:
            self.handle_date_trainning_data_save(d)


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
        # download other
        elif sys.argv[1] == 'd2':
            A.get_all_data2_save()
        elif sys.argv[1] == 'h':
            A.handle_trainning_data_all_save()
    else:
        pass
        d = "ssss"
        # d = A.get_trade_cal_list()
        # A.download_trade_cal_list()
        # A.handle_trainning_data_all_save()
        # A.get_date_stock_num('20190911', '600818.SH')
        A.handle_date_trainning_data_save('20160917')
        # print(d)
        # d = A.get_stock_list_date_n('300425.SZ', '20150615')
        # d = A.get_stock_list_date_n('002120.SZ', '20160919')
        print(d)
        # A.get_index_daily_all('20160105')
        # d = A.get_stock_basics()
    print("Time taken:", datetime.datetime.now() - startTime)


# import zlib, pickle
# redis.hset(key_name, field, df.to_msgpack(compress='zlib'))
# pd.read_msgpack(redis.hget(key_name, field))

# import zlib, pickle
# redis.hset(key_name, field, zlib.compress(pickle.dumps(df), 5))
# df = pickle.loads(zlib.decompress(redis.hget(key_name, field)))
#
# n= n.drop(index=(n.loc[n.ts_code == '002232.SZ'].index))
