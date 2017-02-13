#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Janice Cheng
#
# Hive UDF example
#

import sys
import datetime


record = ["spark","scala"]

# https://cwiki.apache.org/confluence/display/Hive/GettingStarted
# http://whlminds.com/2015/10/07/hive-udf-java-python/



for line in sys.stdin:

    name = line.strip().upper()

    print('\t'.join([name,"UDF"]))

