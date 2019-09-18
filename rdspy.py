#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  r2.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-10 15:42:58
#  Last Modified:  2019-09-18 11:40:02
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
# df1 = pickle.loads(zlib.decompress(r2.hget("20170106", "600698.SH")))
# df1 = r2.hkeys("20170106")
# df1 = pickle.loads(zlib.decompress(r2.hget("20170615", "300425.SZ")))
# df1 = pickle.loads(zlib.decompress(r2.hget("20170107", "300319.SZ")))
# df1 = pickle.loads(zlib.decompress(r0.get("stock_basic")))
df1 = pickle.loads(zlib.decompress(r0.hget("20190917", "top_list")))
df1 = df1.drop(['trade_date', "reason", "name"], axis=1)
# df1 = df1.sort_values(by='pct_change', ascending=False)
df1 = df1[df1.pct_change > 9.5]
# print(type(df1))
# print(df1.iat[0, 2])
# print(type(df1.iat[0, 2]))
# df1 = df1[df1.trade_date == '20170104']
# df1 = df1.drop(['change', 'ts_code'], axis=1)
print(df1)
# print(df1.shape)
# d = df1.iat[0, 0]
# print(float(d))
'''
ks = r2.keys("20*")
print(len(ks))
ks.sort()
cnt = 0

for k in ks:
    hks = r2.hkeys(k)
    c = len(hks)
    cnt = cnt + c
print(cnt)
'''

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
