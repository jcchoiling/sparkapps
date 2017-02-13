# import some package
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *


# Define some functions


def func():
    return 1



# Define the main function

if __name__ == "__main__":

    # Define SparkSession and SparkContext

    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()


    func()

    spark.stop()