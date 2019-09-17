#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  r2.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-10 15:42:58
#  Last Modified:  2019-09-17 16:11:56
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import redis as r
import pickle
import zlib
import pandas as pd

r0 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=0)
r1 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=1)
r2 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=2)

# df1 = pickle.loads(zlib.decompress(r0.get("trade_cal")))
# df1 = pickle.loads(zlib.decompress(r0.hget("20160104", "block_trade")))
# df1 = pickle.loads(zlib.decompress(r0.hget("20160107", "block_trade")))
# df1 = pickle.loads(zlib.decompress(r0.hget("20170123", "daily")))
df1 = pickle.loads(zlib.decompress(r2.hget("20170106", "600698.SH")))
# df1 = pickle.loads(zlib.decompress(r2.hget("20170615", "300425.SZ")))
# df1 = pickle.loads(zlib.decompress(r2.hget("20170107", "300319.SZ")))
# df1 = pickle.loads(zlib.decompress(r0.get("stock_basic")))
d = df1[df1.trade_date == '20170104']
print(d)
# c = d['close']
# c = c.iat[0]
# print(c, type(c))
# print(df1)
# print(type(df1))
# df2 = pickle.loads(zlib.decompress(r0.hget("20170107", "top_list")))
# df2 = df2[df2.ts_code == '300319.SZ']
# df2 = df2.drop(['ts_code', 'trade_date', 'name', 'close', 'pct_change', 'reason'], axis=1)
# print(df2, type(df2))
# df1 = r1.keys("*")
# df1.sort()
