#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-24 16:33:09
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import datetime
import numpy as np
import pandas as pd
from stockdata import stockdata
import matplotlib.pyplot as plt


class train_data:
    def __init__(self):
        self.sd = stockdata()
        self.i = 0
        self.df_sh = self.get_stock_daily_train_df()
        self.ninput = 0
        self.tinput = 0

    def make_a_predictor_x_data_from_df(self, dsf):
        dsf = dsf.tail(40)
        dsf = dsf.drop(['change'], axis=1)

        dif = self.sd.get_index_daily_sh()
        dif = dif.drop(['change', 'ts_code'], axis=1)

        df = pd.merge(dsf, dif, on='trade_date')

        df['trade_date'] = df.trade_date.apply(lambda x: float(x))
        df['ts_code'] = df.ts_code.apply(lambda x: float(x[:-3]))
        df = df.reset_index(drop=True)
        print(df.shape)

        self.ninput = df.shape[1]
        self.tinput = df.shape[0]

        df = np.array(df)
        df = df.reshape(self.ninput * self.tinput)
        b = df
        # b = np.pad(df, ((0, 384)), 'constant')
        return b

    def make_train_data_from_df(self, df):
        dn = df.tail(1)
        _x = self.make_a_predictor_x_data_from_df(df)
        _y = dn['pct_chg_x'].iat[0]
        # -10-> 10 转化为 0-9
        _y = _y + 10.005
        _y = int(round(_y, 0))
        y = np.zeros(21)
        y[_y] = 1
        x = _x

        # x = x.reshape(1, 784)
        x = x.reshape(1, self.ninput * self.tinput)
        y = y.reshape(1, 21)

        return (x, y)

    def get_all_data_for_predictor(self):
        ld = self.sd.get_latest_data_for_predictor()
        lnpar = []
        for df in ld:
            lnpar.append(self.make_a_predictor_x_data_from_df(df))
        return lnpar

    def get_all_train_data_list(self):
        return self.sd.get_all_train_data_list()

    def get_batch_data(self, dl, n):
        x, y = self.make_train_data_from_df(dl[self.i])
        for i in range(1, n):
            _x, _y = self.make_train_data_from_df(dl[self.i + i])
            x = np.vstack([x, _x])
            y = np.vstack([y, _y])
        self.i = self.i + n
        # print(x.shape)
        # print(y.shape)
        return (x, y)

    def test(self):
        d = self.sd.get_train_data_df('20190919', '002915.SZ')
        print(d)
        if d.empty:
            print(" empty........")
            return
        batch = self.make_train_data_from_df(d)
        print(batch[0][0])
        print(batch[1][0])
        print("batch: ", len(batch), type(batch))
        print("batch[0]: ", len(batch[0]), type(batch[0]), batch[0].shape)
        print("batch[1]: ", len(batch[1]), type(batch[1]), batch[1].shape)
        print("batch[0][0]: ", len(batch[0][0]), type(batch[0][0]), batch[0][0].shape)
        print("batch[1][0]: ", len(batch[1][0]), type(batch[1][0]), batch[1][0].shape)

    def make_a_lstm_data(self, df):
        pass

    def get_stock_daily_train_df(self):
        df = self.sd.get_index_daily_sh()
        # df = self.sd.get_index_daily_sz()
        # df = self.sd.get_index_daily_cyb()
        df = df.drop(['ts_code'], axis=1)
        df['trade_date'] = df.trade_date.apply(lambda x: float(x))
        df = df.sort_values(by='trade_date', ascending=True)
        return df.reset_index(drop=True)

    def get_stock_daily_test_df(self):
        pass

    def get_stock_daily_predict_df(self):
        pass

    def test2(self):
        # print(self.df_sh)
        d = self.df_sh.iloc[:, 4].tolist()
        # print(d)
        plt.plot(d)
        plt.show()


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    a = train_data()
    d = a.sd.get_train_data_df('20190923', '002354.SZ')
    a.make_a_predictor_x_data_from_df(d)
    # a.test2()
    # dl = a.get_all_train_data_list()
    # c = a.get_batch_data(dl, 5)
    # d = a.get_batch_data(dl, 5)
    # print(c)
    # print(d)

    print("Time taken:", datetime.datetime.now() - startTime)
