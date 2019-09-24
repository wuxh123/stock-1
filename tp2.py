#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  training_predictor.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-19 16:46:59
#  Last Modified:  2019-09-24 16:45:11
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2'
import tensorflow as tf
from tensorflow.contrib import rnn
from train_data import train_data as trd

# 训练参数
learning_rate = 0.001  # 学习率
batch_size = 25  # 批量大小
display_step = 50

A = trd()
a = A.get_all_train_data_list()
al = len(a)
al = al // batch_size


# 网络参数
num_input = 18  # 每行多少个数据
timesteps = 40  # 多少个时间序列
num_hidden = 64  # 隐藏层神经元数
num_classes = 21  # 数据集类别数


# 定义输入
X = tf.placeholder("float", [None, timesteps, num_input])
Y = tf.placeholder("float", [None, num_classes])

# 定义权重和偏置
# weights矩阵[128, 10]
weights = {'out': tf.Variable(tf.random_normal([num_hidden, num_classes]))}
biases = {'out': tf.Variable(tf.random_normal([num_classes]))}


# 定义LSTM网络
def LSTM(x, weights, biases):

    # Prepare data shape to match `rnn` function requirements
    # 输入数据x的shape: (batch_size, timesteps, n_input)
    # 需要的shape: 按 timesteps 切片，得到 timesteps 个 (batch_size, n_input)

    # 对x进行切分
    # tf.unstack(value,num=None,axis=0,name='unstack')
    # value：要进行分割的tensor
    # axis：整数，打算进行切分的维度
    # num：整数，axis（打算切分）维度的长度
    x = tf.unstack(x, timesteps, 1)

    # 定义一个lstm cell，即上面图示LSTM中的A
    # n_hidden表示神经元的个数，forget_bias就是LSTM们的忘记系数，如果等于1，就是不会忘记任何信息。如果等于0，就都忘记。
    lstm_cell = rnn.BasicLSTMCell(num_hidden, forget_bias=1.0)

    # 得到 lstm cell 输出
    # 输出output和states
    # outputs是一个长度为T的列表，通过outputs[-1]取出最后的输出
    # state是最后的状态
    outputs, states = rnn.static_rnn(lstm_cell, x, dtype=tf.float32)

    # 线性激活
    # 矩阵乘法
    return tf.matmul(outputs[-1], weights['out']) + biases['out']


logits = LSTM(X, weights, biases)
prediction = tf.nn.softmax(logits)

# 定义损失函数和优化器
loss_op = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits_v2(
    logits=logits, labels=Y))
optimizer = tf.train.GradientDescentOptimizer(learning_rate=learning_rate)
train_op = optimizer.minimize(loss_op)

# 模型评估(with test logits, for dropout to be disabled)
correct_pred = tf.equal(tf.argmax(prediction, 1), tf.argmax(Y, 1))
accuracy = tf.reduce_mean(tf.cast(correct_pred, tf.float32))

# 初始化全局变量
init = tf.global_variables_initializer()

# cfg = tf.ConfigProto(gpu_options=tf.GPUOptions(allow_growth=True))
# cfg.gpu_options.per_process_gpu_memory_fraction = 0.9
# cfg.allow_soft_placement = True


# Start training
# with tf.Session(config=cfg) as sess:
with tf.Session() as sess:
    sess.run(init)

    for step in range(al):
        batch_x, batch_y = A.get_batch_data(a, batch_size)
        batch_x = batch_x.reshape((batch_size, timesteps, num_input))
        sess.run(train_op, feed_dict={X: batch_x, Y: batch_y})
        if step % display_step == 0 or step == 1:
            # Calculate batch loss and accuracy
            loss, acc = sess.run([loss_op, accuracy], feed_dict={X: batch_x, Y: batch_y})
            print("Step " + str(step) + ", Minibatch Loss= " + "{:.4f}".format(loss) + ", Training Accuracy= " + "{:.3f}".format(acc))

    print("Optimization Finished!")

    # Calculate accuracy for 128 mnist test images
    # test_len = 128
    # test_data = mnist.test.images[:test_len].reshape((-1, timesteps, num_input))
    # test_label = mnist.test.labels[:test_len]
    # print("Testing Accuracy:", sess.run(
    # accuracy, feed_dict={X: test_data, Y: test_label}))
