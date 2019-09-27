#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-27 14:15:40
#       Revision:  none
#       Compiler:  gcc #
#         Author:  zt ()
#   Organization:

import datetime
import math
import numpy as np
np.set_printoptions(threshold=np.inf)
import pandas as pd
from stockdata import stockdata


class train_data:
    def __init__(self):
        self.sd = stockdata()
        self.batch_size = 200    # 一次训练多少组数据
        self.num_input = 15     # 每组数据的每一行
        self.timesteps = 5     # 多少个时间序列 (多少行)
        self.num_classes = 21   # 数据集类别数
        self.test_size = 0      # 填充多少个0

    def calc_delta_days(self, d1, d2):
        d = (datetime.datetime.strptime(d1, "%Y%m%d") - datetime.datetime.strptime(d2, "%Y%m%d")).days
        return float(d)

    def make_a_train_data_from_df(self, dfx, y):
        xn = np.array(dfx)
        xn = xn.reshape(self.num_input * self.timesteps)

        xnpad = np.zeros(self.test_size)
        xn = np.append(xn, xnpad, axis=0)

        xn = xn.reshape(1, self.num_input * self.timesteps + self.test_size)

        yn = np.zeros(self.num_classes)
        _y = int(math.floor(y * self.num_classes / 20.0 + self.num_classes / 2.0 - 0.1))
        yn[_y] = 1
        yn = yn.reshape(1, self.num_classes)
        return (xn, yn)

    def calc_train_data_list_from_df(self, df):
        lt = []
        min_len = self.timesteps + self.batch_size + 1
        if df.shape[0] < min_len:
            return lt

        # 条件删除
        df['pct_chg'] = df.pct_chg.apply(lambda x: 10.0 if x > 9.5 else x)
        df['pct_chg'] = df.pct_chg.apply(lambda x: -10.0 if x < -9.5 else x)

        df = df[::-1]
        code = df.iat[0, 0]

        cnt = df.shape[0]
        cnt = cnt - min_len
        cnt = cnt // self.batch_size
        cnt = cnt * self.batch_size

        df = df.tail(cnt + min_len)
        df = df.drop(['change', 'ts_code', 'pre_close'], axis=1)

        dif = self.sd.get_index_daily_by_code(code)

        dif = dif.drop(['change', 'ts_code', 'pre_close'], axis=1)
        df = pd.merge(df, dif, on='trade_date')

        self.num_input = df.shape[1]
        cnt = df.shape[0]

        df['td2'] = df['trade_date'].shift(1)
        df['td2'].iat[0] = df['trade_date'].iat[0]

        df['trade_date'] = df.apply(lambda x: self.calc_delta_days(
            x['trade_date'], x['td2']), axis=1)

        df = df.drop(['td2'], axis=1)

        for i in range(cnt - 1 - self.timesteps):
            dfx = df[i: i + self.timesteps]
            y = df['pct_chg_x'].iat[i + self.timesteps]
            xn, yn = self.make_a_train_data_from_df(dfx, y)
            lt.append((xn, yn))

        return lt

    def get_batch_data_from_list(self, ll, n):
        if len(ll) == 0:
            return None
        xt, yt = ll[n * self.batch_size]
        for i in range(1, self.batch_size):
            x, y = ll[i]
            xt = np.vstack([xt, x])
            yt = np.vstack([yt, y])
        return (xt, yt)

    def get_test_data_df(self):
        pass

    def get_predict_data_df(self):
        pass

    def test(self):
        ll = self.sd.get_all_code()
        for c in ll:
            d = self.sd.get_data_by_code(c)
            df = self.calc_train_data_list_from_df(d)
            print(c)
            if df is None:
                pass


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    a = train_data()
    d = a.sd.get_data_by_code('000001.SZ')
    df = a.calc_train_data_list_from_df(d)
    print(df[0][0])
    print(df[0][1])
    # d = a.test()
    # c = a.get_batch_data_from_list(d, 100)
    # print(c)
    # print(type(c))
    # print(a.calc_delta_days("20190926", "20190821"))

    print("Time taken:", datetime.datetime.now() - startTime)
