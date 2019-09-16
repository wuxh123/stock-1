#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  stockdata.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-18 16:07:49
#  Last Modified:  2019-09-16 11:31:13
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
        # self.r0 = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=0)
        self.r0 = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=0)
        # 加工后的数据存数据库1
        # self.r1 = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=1)
        self.r1 = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=1)
        # 训练用数据存数据库2
        # self.r2 = redis.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=2)
        self.r2 = redis.Redis(host='127.0.0.1', password='zt@123456', port=6379, db=2)

    def get_today_date(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    def get_stock_basics(self):
        return self.pro.get_stock_basics()

    def get_trade_cal_list(self):
        df = pickle.loads(zlib.decompress(self.r0.get('trade_cal')))
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

    def download_top_list(self, date):
        self.check_exists_and_save(self.r0, self.pro.top_list, date, 'top_list')

    def download_top_inst(self, date):
        self.check_exists_and_save(self.r0, self.pro.top_inst, date, 'top_inst')

    def download_stk_limit(self, date):
        self.check_exists_and_save(self.r0, self.pro.stk_limit, date, 'stk_limit')

    def download_daily(self, date):
        self.check_exists_and_save(self.r0, self.pro.daily, date, 'daily')

    def download_hk_hold(self, date):
        b = self.check_exists_and_save(self.r0, self.pro.hk_hold, date, 'hk_hold')
        if b:
            time.sleep(41)

    def download_block_trade(self, date):
        b = self.check_exists_and_save(self.r0, self.pro.block_trade, date, 'block_trade')
        if b:
            time.sleep(0.8)

    def download_stk_holdertrade(self, date):
        self.check_exists_and_save(self.r0, self.pro.stk_holdertrade, date, 'stk_holdertrade')

    def test(self, date):
        data = self.pro.hk_hold(trade_date=date)
        print(data)

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
    # 处理数据 从数据库0 读取数据 处理好后 存到数据库1
    def handle_date_limitup_save(self, date):
        ue = self.r1.hexists(date, 'up_limit')
        dr = self.r0.hexists(date, 'daily')
        ds = self.r0.hexists(date, 'stk_limit')
        if ue == 0 and dr == 1 and ds == 1:
            dfd = pickle.loads(zlib.decompress(self.r0.hget(date, 'daily')))
            dfd = dfd[['ts_code', 'close', 'pct_chg']]
            dfs = pickle.loads(zlib.decompress(self.r0.hget(date, 'stk_limit')))
            dfs = dfs[['ts_code', 'up_limit']]
            df = pd.merge(dfd, dfs, on='ts_code')
            df = df[(df.close == df.up_limit) & (df.pct_chg > 6.0) & (df.pct_chg < 12.0)]
            df = df['ts_code']
            df = df.reset_index(drop=True)

            if df.empty is False:
                self.r1.hset(date, 'up_limit', zlib.compress(pickle.dumps(df), 5))
                print("handle: ", date, 'up_limit', " ok")

    # 处理数据
    def handle_all_date_limitup_save(self):
        ds_date = self.get_trade_cal_list()
        print(" start_date: ", ds_date[0], "end_date: ", ds_date[ds_date.shape[0] - 1])
        for d in ds_date:
            self.handle_date_limitup_save(d)

    # 选定标的次日表现存贮，用于修正学习方向。
    def handle_all_date_limitup_nextday_save(self):
        ds_date = self.get_trade_cal_list()
        d = ds_date[:-1]
        for i in range(len(d)):
            dt = d[i]
            if self.r1.hexists(dt, 'up_limit_nextday') == 1:
                return
                # pass

            if self.r1.hexists(dt, 'up_limit') == 0:
                return
            dtdf = pickle.loads(zlib.decompress(self.r1.hget(dt, 'up_limit')))

            nxdt = ds_date[i + 1]
            if self.r0.hexists(nxdt, 'daily') == 0:
                return
            nxdtdf = pickle.loads(zlib.decompress(self.r0.hget(nxdt, 'daily')))

            a = nxdtdf[nxdtdf.ts_code.isin(dtdf)]
            a = a.sort_values("pct_chg", ascending=False)
            a = a.reset_index(drop=True)
            if a.empty is False:
                self.r1.hset(dt, 'up_limit_nextday', zlib.compress(pickle.dumps(a), 5))
                print("handle: ", dt, 'up_limit_nextday', " ok")

    def get_date_limitup_nextday(self, date):
        df = pickle.loads(zlib.decompress(self.r1.hget(date, 'up_limit_nextday')))
        return df

    # 从 数据库0 查询 获取某天某代码
    def get_date_stock_num(self, date, code):
        ret = self.r0.hexists(date, 'daily')
        if ret == 0:
            return None
        df = pickle.loads(zlib.decompress(self.r0.hget(date, 'daily')))
        df = df[df.ts_code == code]
        if df.empty is False:
            return df
        return None

    # 获取截止某天的某代码前40天数据
    def get_date_stock_num_last_40days(self, date, code):
        ds_date = self.get_trade_cal_list()
        ds_date = ds_date[ds_date <= date]
        ds_date = ds_date.sort_values(ascending=False)
        ds_date = ds_date.reset_index(drop=True)

        data = pd.DataFrame()
        for d in ds_date:
            d = self.get_date_stock_num(d, code)
            if d is not None:
                data = data.append(d)
                if data.shape[0] == 40:
                    data = data.sort_values(by='trade_date')
                    data = data.reset_index(drop=True)
                    return data
        return None

    # data for trainning save in db2
    def handle_trainning_data_save(self, date):
        if self.r1.hexists(date, 'up_limit_nextday') == 1:
            d = self.get_date_limitup_nextday(date)
            df_tscode = d['ts_code']

            for code in df_tscode:
                if self.r2.hexists(date, code) == 0:
                    data = self.get_date_stock_num_last_40days(date, code)
                    if data is not None:
                        dnc = d[d.ts_code == code]
                        # append up_limit next day for correct
                        data = data.append(dnc)
                        data = data.reset_index(drop=True)
                        self.r2.hset(date, code, zlib.compress(pickle.dumps(data), 5))
                        print("handle_trainning_data_save: ", date, code)
                    else:
                        derror = pd.DataFrame()
                        self.r2.hset(date, code, zlib.compress(pickle.dumps(derror), 5))
                        print("handle_trainning_data_save: ", date, code, "error and mark")

    def handle_trainning_data_all_save(self):
        cal = self.get_trade_cal_list()
        cal = cal[cal >= '20150101']
        cal = cal.reset_index(drop=True)
        for d in cal:
            self.handle_trainning_data_save(d)


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    A = stockdata()
    if len(sys.argv) > 1:
        # check data del empty
        if sys.argv[1] == 'c':
            A.check_all_download_data()
        # download index_daily top stk daily
        elif sys.argv[1] == 'd1':
            A.download_trade_cal_list()
            A.download_all_data()
        # download other
        elif sys.argv[1] == 'd2':
            A.get_all_data2_save()
        elif sys.argv[1] == 'h':
            A.handle_all_date_limitup_save()
            A.handle_all_date_limitup_nextday_save()
            A.handle_trainning_data_all_save()
    else:
        # d = A.get_trade_cal_list()
        # A.handle_date_limitup_save('20140103')
        # A.download_trade_cal_list()
        # A.handle_trainning_data_save('20150105')
        # A.handle_trainning_data_all_save()
        # A.get_date_stock_num('20190911', '600818.SH')
        A.get_date_stock_num_last_40days('20190911', '600818.SH')

    print("Time taken:", datetime.datetime.now() - startTime)


# import zlib, pickle
# redis.hset(key_name, field, df.to_msgpack(compress='zlib'))
# pd.read_msgpack(redis.hget(key_name, field))

# import zlib, pickle
# redis.hset(key_name, field, zlib.compress(pickle.dumps(df), 5))
# df = pickle.loads(zlib.decompress(redis.hget(key_name, field)))
#
# nxdtdf = nxdtdf.drop(index=(nxdtdf.loc[nxdtdf.ts_code == '002232.SZ'].index))
'''
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
'''
