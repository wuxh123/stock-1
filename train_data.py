#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  train_data.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 10:07:56
#  Last Modified:  2019-09-26 09:58:21
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
        self.batch_size = 50

    def get_train_data_df(self):
        pass

    def get_test_data_df(self):
        pass

    def get_predict_data_df(self):
        pass

    def test(self):
        return self.sd.get_index_daily_sh()


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    a = train_data()
    a.test()

    print("Time taken:", datetime.datetime.now() - startTime)
