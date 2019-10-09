#!/usr/bin/env python
# -*- coding:utf-8 -*-
#       FileName:  mnist2.py
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-10-09 11:01:04
#  Last Modified:  2019-10-09 17:20:52
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:
import tensorflow as tf
from keras.models import Sequential
from keras.layers import Conv2D, MaxPool2D, Flatten, Dropout, Dense
from keras.losses import categorical_crossentropy
from keras.optimizers import Adadelta
from keras.models import load_model

from train_data import train_data as trd
A = trd()
df = A.sd.get_data_by_code('600737.SH')
xn, yn = A.gen_train_data_from_df(df)
print(xn.shape)
print(yn.shape)

cfg = tf.compat.v1.ConfigProto(gpu_options=tf.compat.v1.GPUOptions(allow_growth=True))
cfg.gpu_options.per_process_gpu_memory_fraction = 0.9
cfg.allow_soft_placement = True
sess = tf.compat.v1.InteractiveSession(config=cfg)


# model = Sequential()
# model.add(Conv2D(32, (5, 5), activation='relu', input_shape=[A.timesteps, A.num_input, 1]))
# model.add(Conv2D(64, (5, 5), activation='relu'))
# model.add(MaxPool2D(pool_size=(2, 2)))
# model.add(Flatten())
# model.add(Dropout(0.5))
# model.add(Dense(128, activation='relu'))
# model.add(Dropout(0.5))
# model.add(Dense(A.num_classes, activation='softmax'))

# model.compile(loss=categorical_crossentropy, optimizer=Adadelta(), metrics=['accuracy'])

model = load_model('stock_keras.h5')

batch_size = A.batch_size
epochs = 20
model.fit(xn, yn, batch_size=batch_size, epochs=epochs)

loss, accuracy = model.evaluate(xn, yn, verbose=1)
print('loss:%.4f accuracy:%.4f' % (loss, accuracy))
model.save('stock_keras.h5')
