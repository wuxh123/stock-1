#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  lstm_p1.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-26 09:51:27
#  Last Modified:  2019-09-26 10:43:56
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import pandas as pd
import numpy as np
import tensorflow as tf
import matplotlib.pyplot as plt
from train_data import train_data

td = train_data()
dfsh = td.test()

df = dfsh
data = np.array(df['close'])
data = data[::-1]
# plt.figure()
# plt.plot(data)
# plt.show()

# 标准化
normalize_data = (data - np.mean(data)) / np.std(data)
# 增加维度
normalize_data = normalize_data[:, np.newaxis]

# 每一批次训练多少个样例
batch_size = 60

# hidden layer units
rnn_unit = 10

# 时间步 多少行
time_step = 20

# 输入层维度
input_size = 1

# 输出层维度
output_size = 1

# 学习率
lr = 0.0006

train_x, train_y = [], []  # 训练集
for i in range(len(normalize_data) - time_step - 1):
    x = normalize_data[i:i + time_step]
    y = normalize_data[i + 1:i + time_step + 1]
    train_x.append(x.tolist())
    train_y.append(y.tolist())

#  ——————————————————定义神经网络变量——————————————————
# 每批次输入网络的tensor
X = tf.placeholder(tf.float32, [None, time_step, input_size])
# 每批次tensor对应的标签
Y = tf.placeholder(tf.float32, [None, time_step, output_size])
# 输入层、输出层权重、偏置
weights = {'in': tf.Variable(tf.random_normal(
    [input_size, rnn_unit])), 'out': tf.Variable(tf.random_normal([rnn_unit, 1]))}

biases = {'in': tf.Variable(tf.constant(
    0.1, shape=[rnn_unit, ])), 'out': tf.Variable(tf.constant(0.1, shape=[1, ]))}


# 参数：输入网络批次数目
def lstm(batch):
    w_in = weights['in']
    b_in = biases['in']

    # 需要将tensor转成2维进行计算，计算后的结果作为隐藏层的输入
    input = tf.reshape(X, [-1, input_size])
    input_rnn = tf.matmul(input, w_in) + b_in

    # 将tensor转成3维，作为lstm cell的输入
    input_rnn = tf.reshape(input_rnn, [-1, time_step, rnn_unit])
    cell = tf.nn.rnn_cell.BasicLSTMCell(rnn_unit)
    init_state = cell.zero_state(batch, dtype=tf.float32)

    # output_rnn是记录lstm每个输出节点的结果，final_states是最后一个cell的结果
    output_rnn, final_states = tf.nn.dynamic_rnn(
        cell, input_rnn, initial_state=init_state, dtype=tf.float32)
    output = tf.reshape(output_rnn, [-1, rnn_unit])  # 作为输出层的输入

    w_out = weights['out']
    b_out = biases['out']
    pred = tf.matmul(output, w_out) + b_out
    return pred, final_states


cfg = tf.ConfigProto(gpu_options=tf.GPUOptions(allow_growth=True))
cfg.gpu_options.per_process_gpu_memory_fraction = 0.9
cfg.allow_soft_placement = True


def train_lstm():
    global batch_size
    pred, _ = lstm(batch_size)
    # 损失函数
    loss = tf.reduce_mean(tf.square(tf.reshape(pred, [-1]) - tf.reshape(Y, [-1])))
    train_op = tf.train.AdamOptimizer(lr).minimize(loss)
    saver = tf.train.Saver(tf.global_variables())
    with tf.Session(config=cfg) as sess:
        sess.run(tf.global_variables_initializer())
        for i in range(1000):
            step = 0
            start = 0
            end = start + batch_size
            while(end < len(train_x)):
                _, loss_ = sess.run([train_op, loss], feed_dict={
                                    X: train_x[start:end], Y: train_y[start:end]})
                start += batch_size
                end = start + batch_size
                if step % 1000 == 0:
                    print(i, step, loss_)
                step += 1
        print("保存模型：", saver.save(sess, './SAVE3/stock.model'))


train_lstm()

'''
def prediction():
    # 预测时只输入[1,time_step,input_size]的测试数据
    pred, _ = lstm(1)
    saver = tf.train.Saver(tf.global_variables())
    with tf.Session() as sess:
        # 参数恢复
        module_file = tf.train.latest_checkpoint('./SAVE3')
        saver.restore(sess, module_file)

        # 取训练集最后一行为测试样本。shape=[1,time_step,input_size]
        prev_seq = train_x[-1]
        predict = []
        # 得到之后100个预测结果
        for i in range(100):
            next_seq = sess.run(pred, feed_dict={X: [prev_seq]})
            predict.append(next_seq[-1])
            # 每次得到最后一个时间步的预测结果，与之前的数据加在一起，形成新的测试样本
            prev_seq = np.vstack((prev_seq[1:], next_seq[-1]))
        # 以折线图表示结果
        plt.figure()
        plt.plot(list(range(len(normalize_data))), normalize_data, color='b')
        plt.plot(list(range(len(normalize_data), len(
            normalize_data) + len(predict))), predict, color='r')
        plt.show()


prediction()
'''
