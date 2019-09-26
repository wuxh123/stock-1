#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-26 15:15:01
#       Revision:  none
#       Compiler:  gcc #
#         Author:  zt ()
#   Organization:

import datetime
import numpy as np
import pandas as pd
from stockdata import stockdata


class train_data:
    def __init__(self):
        self.sd = stockdata()
        self.batch_size = 25    # 一次训练多少组数据
        self.num_input = 15     # 每组数据的每一行
        self.timesteps = 40     # 多少个时间序列 (多少行)
        self.num_classes = 21   # 数据集类别数
        self.test_size = 5      # 留多少数据用于测试

    def calc_delta_days(self, d1, d2):
        d = (datetime.datetime.strptime(d1, "%Y%m%d") - datetime.datetime.strptime(d2, "%Y%m%d")).days
        return float(d)

    def make_a_train_data_from_df(self, dfx, y):
        xn = np.array(dfx)
        xn = xn.reshape(1, self.num_input * self.timesteps)
        _y = int(round(y + 10.005, 0))
        yn = np.zeros(self.num_classes)
        yn[_y] = 1
        yn = yn.reshape(1, self.num_classes)
        return (xn, yn)

    def get_train_data_from_df(self, df):
        lt = []
        min_len = self.timesteps + self.batch_size + 1
        if df.shape[0] < min_len:
            return lt

        df = df[::-1]

        cnt = df.shape[0]
        cnt = cnt - min_len
        cnt = cnt // self.batch_size
        cnt = cnt * self.batch_size
        df = df.tail(cnt + min_len)

        df = df.drop(['change', 'ts_code', 'pre_close'], axis=1)
        dif = self.sd.get_index_daily_sh()
        dif = dif.drop(['change', 'ts_code', 'pre_close'], axis=1)
        df = pd.merge(df, dif, on='trade_date')

        self.num_input = df.shape[1]
        cnt = df.shape[0]

        df['td2'] = df['trade_date'].shift(1)
        df['td2'].iat[0] = df['trade_date'].iat[0]

        df['trade_date'] = df.apply(lambda x: self.calc_delta_days(
            x['trade_date'], x['td2']), axis=1)

        df = df.drop(['td2'], axis=1)

        for i in range(cnt - self.test_size - self.timesteps):
            dfx = df[i: i + self.timesteps]
            y = df['pct_chg_x'].iat[i + self.timesteps]
            xn, yn = self.make_a_train_data_from_df(dfx, y)
            lt.append((xn, yn))

        return lt

    def get_test_data_df(self):
        pass

    def get_predict_data_df(self):
        pass

    def test(self):
        # return self.sd.get_index_daily_sh()
        return self.sd.get_data_by_code('600818.SH')


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    a = train_data()
    d = a.test()
    a.get_train_data_from_df(d)
    # print(a.calc_delta_days("20190926", "20190821"))

    print("Time taken:", datetime.datetime.now() - startTime)
