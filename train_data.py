#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-10-10 14:19:47
#       Revision:  none
#       Compiler:  gcc #
#         Author:  zt ()
#   Organization:

import sys
import datetime
import math
import numpy as np
np.set_printoptions(threshold=np.inf)
import pandas as pd
from stockdata import stockdata


class train_data:
    def __init__(self):
        self.sd = stockdata()
        self.batch_size = 32        # 一次训练多少组数据
        self.num_input = 13         # 每组数据的每一行
        self.timesteps = 10         # 多少行
        self.num_classes = 2        # 数据集类别数
        self.test_size = 0          # 填充多少个0
        self.ndays = 2              # 几日差值
        self.epochs = 40

    def calc_delta_days(self, d1, d2):
        d = (datetime.datetime.strptime(d1, "%Y%m%d") -
             datetime.datetime.strptime(d2, "%Y%m%d")).days
        return float(d)

    def gen_train_data_from_df(self, df):
        code = df.iat[0, 0]
        df = df[::-1]
        df = df.drop(['change', 'ts_code', 'pre_close', 'pct_chg'], axis=1)

        min_len = self.timesteps + self.batch_size + self.ndays
        if df.shape[0] < min_len:
            return ()

        cnt = df.shape[0]
        cnt = cnt - min_len
        cnt = cnt // self.batch_size

        df = df.tail(cnt * self.batch_size + min_len)
        dif = self.sd.get_index_daily_by_code(code)

        dif = dif.drop(['change', 'ts_code', 'pre_close', 'pct_chg'], axis=1)
        df = pd.merge(df, dif, on='trade_date')

        df['td2'] = df['trade_date'].shift(1)
        df['td2'].iat[0] = df['trade_date'].iat[0]

        df['trade_date'] = df.apply(lambda x: self.calc_delta_days(
            x['trade_date'], x['td2']), axis=1)

        df = df.drop(['td2'], axis=1)

        datanums = df.shape[0] - self.timesteps - self.ndays

        xn = np.empty(shape=[0, self.timesteps, self.num_input, 1])
        yn = np.empty(shape=[0, self.num_classes])
        for i in range(datanums):
            dfx = df[i: i + self.timesteps]
            dfx = dfx.apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))
            xtmp = np.array(dfx).reshape(1, self.timesteps, self.num_input, 1)
            xn = np.vstack((xn, xtmp))

            yo = df['open_x'].iat[i + self.timesteps]
            yc = df['close_x'].iat[i + self.timesteps + 1]
            y = 100.0 * (yc - yo) / yo
            ytmp = np.zeros(self.num_classes)
            if y > 1.0:
                ytmp[1] = 1
            else:
                ytmp[0] = 1
            yn = np.vstack((yn, ytmp))
        return xn, yn

    def get_test_data_df(self):
        pass

    def get_predict_data(self, code, date):
        df = self.sd.get_data_by_code(code)
        df = df[df.trade_date <= date]
        df = df.head(self.timesteps)
        df = df[::-1]
        df = df.drop(['change', 'ts_code', 'pre_close', 'pct_chg'], axis=1)

        dif = self.sd.get_index_daily_by_code(code)

        dif = dif.drop(['change', 'ts_code', 'pre_close', 'pct_chg'], axis=1)
        df = pd.merge(df, dif, on='trade_date')

        df['td2'] = df['trade_date'].shift(1)
        df['td2'].iat[0] = df['trade_date'].iat[0]

        df['trade_date'] = df.apply(lambda x: self.calc_delta_days(
            x['trade_date'], x['td2']), axis=1)

        df = df.drop(['td2'], axis=1)
        df = df.apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))

        xn = np.array(df).reshape(1, self.timesteps, self.num_input, 1)

        return xn


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    a = train_data()
    if len(sys.argv) > 1:
        if sys.argv[1] == 'g':  # gen train data
            pass
        elif sys.argv[1] == 't':  # gen test data
            pass
        elif sys.argv[1] == 'p':  # get predict data
            pass
    else:
        # df = a.sd.get_data_by_code('600737.SH')
        df = a.sd.get_data_by_code('000058.SZ')
        # df = a.get_predict_data('600737.SH', '20190925')
        # df = a.gen_train_data_from_df(df)
        # df['res'] = 0.0
        print(df)
        print(df.shape)
        # print(df.shape)
        # print(df[0][0])
        # print(df[0][1])
        # print(c)
        # print(type(c))
        # print(a.calc_delta_days("20190926", "20190821"))

    print("Time taken:", datetime.datetime.now() - startTime)
