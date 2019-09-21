#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-21 14:05:13
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
        x = self.make_a_predictor_x_data_from_df(df)
        dn = df.tail(1)
        y = dn['pct_chg'].iat[0]
        # -10-> 10 转化为 0-9
        y = 0.45 * y + 4.5
        y = y + 0.005
        y = int(round(y, 0))
        yn = np.zeros(10)
        yn[y] = 1
        return (x, yn)

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
        ll = self.get_all_train_data_list()
        print(len(ll))
        print(ll[0])
        return d


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    a = train_data()
    a.test()
    print("Time taken:", datetime.datetime.now() - startTime)
