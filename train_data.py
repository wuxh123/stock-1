#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-21 11:24:54
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

    def make_a_predictor_data_from_df(self, df):
        df = df.tail(40)
        df = df.drop(['change'], axis=1)
        df['trade_date'] = df.trade_date.apply(lambda x: float(x))
        df['ts_code'] = df.ts_code.apply(lambda x: float(x[:-3]))
        df = df.reset_index(drop=True)
        df = np.array(df)
        df = df.reshape(400)

        b = np.pad(df, ((0, 384)), 'constant')
        return b

    def get_all_data_for_predictor(self):
        ld = self.sd.get_latest_data_for_predictor()
        lnpar = []
        for df in ld:
            lnpar.append(self.make_a_predictor_data_from_df(df))
        return lnpar

    def test(self):
        d = 0
        d = self.get_all_data_for_predictor()
        print(d[0], len(d[0]), len(d))
        return d


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    a = train_data()
    a.test()
    print("Time taken:", datetime.datetime.now() - startTime)
