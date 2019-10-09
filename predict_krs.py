#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  predict.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-10-08 16:50:42
#  Last Modified:  2019-10-09 17:54:42
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

import tensorflow as tf
import numpy as np
from train_data import train_data as trd
from keras.models import load_model
A = trd()

cfg = tf.compat.v1.ConfigProto(gpu_options=tf.compat.v1.GPUOptions(allow_growth=True))
cfg.gpu_options.per_process_gpu_memory_fraction = 0.9
cfg.allow_soft_placement = True
sess = tf.compat.v1.InteractiveSession(config=cfg)


# 加载
model = load_model('stock_keras.h5')
x = A.get_predict_data('600737.SH', '20190925')
# 预测
predict = model.predict(x)
# 取最大值的位置
predict = np.argmax(predict)

# print('index', index)
# print('original:', y)
print('predicted:', predict)
