#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  mnist2.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-10-09 11:01:04
#  Last Modified:  2019-10-10 15:55:00
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:
import os
import tensorflow as tf
from keras.models import Sequential, load_model
from keras.layers import Conv2D, MaxPool2D, Flatten, Dropout, Dense, LSTM
from keras.optimizers import Adadelta

from train_data import train_data as trd
A = trd()

MODEL_NAME = "mdl.h5"

cfg = tf.compat.v1.ConfigProto(gpu_options=tf.compat.v1.GPUOptions(allow_growth=True))
cfg.gpu_options.per_process_gpu_memory_fraction = 0.9
cfg.allow_soft_placement = True
sess = tf.compat.v1.InteractiveSession(config=cfg)


if os.path.isfile(MODEL_NAME):
    print("load_model:------->", MODEL_NAME)
    model = load_model(MODEL_NAME)
else:
    print("gen new model:------->", MODEL_NAME)
    model = Sequential()
    model.add(Conv2D(32, (5, 5), activation='relu', input_shape=[A.timesteps, A.num_input, 1]))
    model.add(Conv2D(64, (5, 5), activation='relu'))
    model.add(MaxPool2D(pool_size=(2, 2)))
    model.add(Flatten())
    # model.add(LSTM(128))
    model.add(Dropout(0.5))
    model.add(Dense(128, activation='relu'))
    model.add(Dropout(0.5))
    model.add(Dense(A.num_classes, activation='softmax'))

    # model.compile(loss=categorical_crossentropy, optimizer=Adadelta(), metrics=['accuracy'])
    model.compile(loss='binary_crossentropy', optimizer='rmsprop', metrics=['accuracy'])

model.summary()

# ll = A.sd.get_all_code()
# ll = A.sd.temp.hkeys("test9")
# ll = ['600737.SH']
ll = ['000829.SZ']
# ll = ['600193.SH']
# ll = ['000425.SZ']
# ll = ['600818.SH']
for c in ll:
    df = A.sd.get_data_by_code(c)
    xn, yn, xt, yt = A.gen_train_data_from_df(df)
    # model.fit(xn, yn, batch_size=A.batch_size, epochs=100)
    model.fit(xn, yn, batch_size=A.batch_size, epochs=A.epochs)
    loss, accuracy = model.evaluate(xt, yt, verbose=1)
    print(c, 'loss:%.4f accuracy:%.4f' % (loss, accuracy))
    model.save(MODEL_NAME)

