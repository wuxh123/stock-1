**码农炒股**

**用于学习进度的记录,tensorflow 机器学习完全是外行，从零开始学习**


# 学习内容
1. 学习markdown
2. 学习python 及各种常用包
3. 学习tensorflow
4. 专注于某一件事情

# 学习目标
1. 熟练掌握上面三个工具，学会一点手艺。
2. 赚点零花钱。


# 学习计划
1. 抓取数据并保存/读取等
   * 在 data 类中实现
       * 使用tushare获取数据
       * 保存和加载对象使用pickle
       * 使用redis保存数据
       * 使用pandas

   * 需要抓取的数据 原始数据存数据库0  tushare下载 压缩保存(pickle) hset(key,filed)
       * 每日大盘数据   (date,(index_daily: 000001.SH  399001.SZ 399006.SZ))
       * 龙虎榜         (date,top_list)
       * 机构龙虎榜     (date,top_inst)
       * 每日涨跌停数值 (date,stk_limit)
       * 每日全市数据   (date,daily)

2. TBD



9. 理论分析
  * 考虑个股三个月的历史数据, 如果时间长度不合适，以后再修改
  * 考虑大盘数据，大盘与个股有一定的关联
  * 考虑板块数据，道理同上
  * 突发利好、利空不在本项目考虑之内





