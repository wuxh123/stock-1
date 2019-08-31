#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  ZtData.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-18 16:07:49
#  Last Modified:  2019-08-31 13:46:31
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import pickle
import datetime
import tushare as ts


class ZtData:
    def __init__(self):
        with open('tk.pkl', 'rb') as f:
            tk = pickle.load(f)
            ts.set_token(tk)
        self.pro = ts.pro_api()

    # 获取前多少天历史数据
    def GetData(self, code):
        startTime = datetime.datetime.now() - datetime.timedelta(weeks=12)
        startTimeStr = startTime.strftime("%Y%m%d")
        df = self.pro.query('daily', ts_code=code, start_date=startTimeStr)

        if df.empty is not True:
            filename = "data/" + code
            with open(filename, 'wb') as f:
                pickle.dump(df, f)
        return df

    def GetAll(self):
        return self.pro.query('stock_basic', exchange='', list_status='L')

    def GetDateLimitUp(self, date):
        df = self.pro.daily(trade_date=date)
        return df[round(df.pre_close * 1.1, 2) == df.close]

    def GetDateLimitDown(self, date):
        df = self.pro.daily(trade_date=date)
        return df[round(df.pre_close * 0.9, 2) == df.close]

    def GetToday(self):
        startTime = datetime.datetime.now()
        startTimeStr = startTime.strftime("%Y%m%d")
        data = self.pro.daily(trade_date=startTimeStr)
        print(data)

    def Get(self):
        data = self.pro.get_stock_basics()
        return data


A = ZtData()
# a = A.GetDateLimitUp('20190830')
a = A.GetDateLimitDown('20190830')
print(a)
