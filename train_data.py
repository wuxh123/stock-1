#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-20 16:20:17
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

from stockdata import stockdata


class train_data:
    def __init__(self):
        self.sd = stockdata()

    def test(self):
        d = self.sd.get_today_data_for_predictor()
        print(d[0])


a = train_data()
a.test()
