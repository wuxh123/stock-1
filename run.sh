#!/bin/sh
#       FileName:  run.sh
#
#    Description:
#
#        Version:  1.0
#        Created:  2019-09-08 00:04:29
#  Last Modified:  2019-09-12 14:28:13
#       Revision:  none
#       Compiler:  gcc
#
#         Author:  zt ()
#   Organization:

while [ 1 ]; do
  python stockdata.py $1
done
