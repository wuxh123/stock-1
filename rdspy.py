#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  r2.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-10 15:42:58
#  Last Modified:  2019-09-14 13:57:17
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import redis as r
import pickle
import zlib
import pandas as pd

rr = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=0)
r1 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=1)
r2 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=2)

# redis.hset(key_name, field, zlib.compress(pickle.dumps(df), 5))
# df = pickle.loads(zlib.decompress(redis.hget(key_name, field)))
# a = rr.hkeys("20190910")
# a.sort()

# df1 = pickle.loads(zlib.decompress(rr.hget("20160104", "block_trade")))
# df1 = pickle.loads(zlib.decompress(rr.hget("20160107", "block_trade")))
# df1 = pickle.loads(zlib.decompress(rr.hget("20150123", "daily")))
# df1 = pickle.loads(zlib.decompress(r1.hget("20160525", "up_limit")))
# df1 = pickle.loads(zlib.decompress(r1.hget("20190911", "up_limit_nextday")))
df1 = pickle.loads(zlib.decompress(rr.get("trade_cal")))
# df1 = r1.keys("*")
# df1.sort()
print(df1, type(df1))
# print(df1)
# print(type(df1))

# print(a, type(a))
'''
a = rr.hget("20140415", "000001.SH")
df = pickle.loads(zlib.decompress(a))
print(df)
df2 = pickle.loads(zlib.decompress(rr.hget("20150123", "stk_limit")))

df2 = df2.drop(['trade_date'], axis=1)
df = pd.merge(df1, df2, on='ts_code')
df = df[df.close == df.up_limit]
df = df.reset_index(drop=True)
print(type(df))
df = df['ts_code']
print(type(df))
print(df)
'''
'''
df1 = pickle.loads(zlib.decompress(rr.hget("20150115", "hk_hold")))
print(df1)

# df2 = pickle.loads(zlib.decompress(rr.hget("20150114", "block_trade")))
# print(df2)
'''
