#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  predict.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-10-08 16:50:42
#  Last Modified:  2019-10-11 14:51:06
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


MODEL_NAME = "mdl.h5"
C = '002308.SZ'
D = '20191008'

model = load_model(MODEL_NAME)

x = A.get_predict_data(C, D)
# print(x)
predict = model.predict(x)
print(predict)
predict = model.predict_classes(x)
print(predict[0])
# predict = np.argmax(predict)

# print('index', index)
# print('original:', y)
print('predicted:', C, D, predict)
