#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  r2.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-10 15:42:58
#  Last Modified:  2019-09-12 13:00:13
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

# redis.hset(key_name, field, zlib.compress(pickle.dumps(df), 5))
# df = pickle.loads(zlib.decompress(redis.hget(key_name, field)))
# a = rr.hkeys("20190910")
# a.sort()

# df1 = pickle.loads(zlib.decompress(rr.hget("20140102", "block_trade")))
# df1 = pickle.loads(zlib.decompress(rr.hget("20181214", "top_list")))
df1 = pickle.loads(zlib.decompress(rr.hget("20181218", "top_inst")))
print(df1)
print(type(df1))

# print(a, type(a))
'''
a = rr.hget("20140415", "000001.SH")
df = pickle.loads(zlib.decompress(a))
print(df)
df1 = pickle.loads(zlib.decompress(rr.hget("20150123", "daily")))
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