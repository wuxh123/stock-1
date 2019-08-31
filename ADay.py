#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  ADay.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-06-17 15:31:48
#  Last Modified:  2019-08-31 13:50:02
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:
import time

import numpy as np
from datetime import datetime
import pandas as pd
import tushare as ts
# import matplotlib.pyplot as plt
from tabulate import tabulate

today = datetime.now().strftime('%Y%m%d')


def monitor():
    data = ts.get_day_all()
    stock_basics = ts.get_stock_basics()
    df = pd.merge(data, stock_basics, how='inner')
    amount = df['amount'].sum()
    volume = df['volume'].sum()
    abvalues = df['abvalues'].sum()

    industry = df[['industry', 'p_change'
                   ]].groupby('industry').mean().sort_values(by='p_change')
    area = df[['area',
               'p_change']].groupby('area').mean().sort_values(by='p_change')

    pe = df['pe'].mean()
    pb = df['pb'].mean()
    turnover = df['turnover'].mean()

    p_chg = np.array(df['p_change'])
    up = p_chg[p_chg > 0].size
    down = p_chg[p_chg < 0].size
    even = p_chg[p_chg == 0].size
    up_limit = p_chg[p_chg > 9.7].size
    down_limit = p_chg[p_chg < -9.7].size

    return {
        'A股成交总额': amount / 100000000,
        'A股成交总量': volume,
        'AB股总市值': round(abvalues, 4),
        '平均涨幅居前的行业': industry.index[0],
        '平均跌幅居前的行业': industry.index[-1],
        '平均涨幅居前的地区': area.index[0],
        '平均跌幅居前的地区': area.index[-1],
        '平均市盈率': round(pe, 4),
        '平均市净率': round(pb, 4),
        '平均换手率': round(turnover, 4),
        'A股上涨家数': up,
        'A股平盘家数': even,
        'A股下跌家数': down,
        'A股涨停家数': up_limit,
        'A股跌停家数': down_limit
    }


def main():
    info = monitor()

    print('收盘行情监控'.center(36, '-'))
    table = [[x, info.get(x)] for x in info.keys()]
    print(tabulate(table, tablefmt='grid'))

    print('END'.center(42, '-'))


if __name__ == '__main__':
    start = time.process_time()
    main()
    print('程序耗时：{}'.format(time.process_time() - start))
