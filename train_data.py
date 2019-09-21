#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-21 15:22:33
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import datetime

import numpy as np
from PIL import Image, ImageFilter
from stockdata import stockdata


class train_data:
    def __init__(self):
        self.sd = stockdata()

    def make_a_predictor_x_data_from_df(self, df):
        df = df.tail(40)
        df = df.drop(['change'], axis=1)
        df['trade_date'] = df.trade_date.apply(lambda x: float(x))
        df['ts_code'] = df.ts_code.apply(lambda x: float(x[:-3]))
        df = df.reset_index(drop=True)
        df = np.array(df)
        df = df.reshape(400)

        b = np.pad(df, ((0, 384)), 'constant')
        return b

    def make_train_data_from_df(self, df):
        dn = df.tail(1)
        _x = self.make_a_predictor_x_data_from_df(df)
        _y = dn['pct_chg'].iat[0]
        # -10-> 10 转化为 0-9
        _y = 0.45 * _y + 4.5
        _y = _y + 0.005
        _y = int(round(_y, 0))
        y = np.zeros(10)
        y[_y] = 1
        x = _x

        x = x.reshape(1, 784)
        y = y.reshape(1, 10)

        return (x, y)

    def get_all_data_for_predictor(self):
        ld = self.sd.get_latest_data_for_predictor()
        lnpar = []
        for df in ld:
            lnpar.append(self.make_a_predictor_x_data_from_df(df))
        return lnpar

    def get_all_train_data_list(self):
        return self.sd.get_all_train_data_list()

    def test(self):
        d = 0
        d = self.sd.get_train_data_df('20190919', '002915.SZ')
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
        return d


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    a = train_data()
    a.test()
    print("Time taken:", datetime.datetime.now() - startTime)
