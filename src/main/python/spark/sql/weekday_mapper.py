#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Janice Cheng
#
# Hive UDF example
#

import sys
import datetime


# record = ["33,3699,2,978110600","1,1234,4,978110610"]

# https://cwiki.apache.org/confluence/display/Hive/GettingStarted
# http://whlminds.com/2015/10/07/hive-udf-java-python/


for line in sys.stdin:

    userid, movieid, rating, unixtime = line.strip().split(",")

    weekday = datetime.datetime.fromtimestamp(float(unixtime)).isoweekday()

    print('\t'.join([userid, movieid, rating, str(weekday)]))

