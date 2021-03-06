#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Janice Cheng
# Spark Application for Python

from __future__ import print_function
from spark import viu_setting as setting
from operator import add

from pyspark.sql import SparkSession

# Define a function


if __name__ == "__main__":

    warehouse_location = setting.SPARK_WAREHOUSE

    file = setting.GENERAL_DATA + "/spark.txt"

    # if len(sys.argv) !=2 0:
        # print("Usage: wordcount <file>", file=sys.stderr)
        # exit(-1)

    spark = SparkSession.builder\
        .appName("PythonWordCount")\
        .config("spark.sql.warehouse",warehouse_location)\
        .getOrCreate()

    sc = spark.sparkContext

    sc.setLogLevel("WARN")

    lines = spark.read.text(file).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()


