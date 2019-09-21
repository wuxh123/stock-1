#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-20 23:45:16
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

    def test(self):
        # d = self.sd.get_today_data_for_predictor()
        # a = d[0]
        # print(a)
        a = self.sd.get_train_data_df("20190918", "603505.SH")
        a = a.tail(40)
        a['trade_date'] = a.trade_date.apply(lambda x: float(x))
        a['ts_code'] = a.ts_code.apply(lambda x: float(x[:-3]))
        a = a.drop(['change'], axis=1)
        a = a.reset_index(drop=True)
        a = np.array(a)
        a = a.reshape(400)
        # numpy.ndarray
        print(a)
        print(a.shape, type(a))

        b = np.pad(a, ((0, 384)), 'constant')
        print(b, b.shape)
        return b


def imageprepare():
    im = Image.open('a.png')
    im = im.convert('L')
    tv = list(im.getdata())
    tva = [(255 - x) * 1.0 / 255.0 for x in tv]
    print(type(tva[100]), len(tva))
    return tva


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    # imageprepare()
    a = train_data()
    a.test()
    print("Time taken:", datetime.datetime.now() - startTime)
