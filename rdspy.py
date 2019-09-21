#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  r2.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-10 15:42:58
#  Last Modified:  2019-09-22 00:23:25
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import redis as r
import pickle
import zlib
import pandas as pd
import numpy as np

r0 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=0)
r1 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=1)
r2 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=2)
r3 = r.Redis(host='192.168.0.188', password='zt@123456', port=6379, db=3)


# df1 = pickle.loads(zlib.decompress(r0.hget("20170123", "daily")))
r3.set('test', 'aaa')
d = r3.get('test')
print(d, type(d))
# print(type(df1))
