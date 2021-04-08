'''
initSession.py

    establishes a unified spark context and session
    to be used in subsequent scripts.

'''

import pyspark
from pyspark.sql import SparkSession

sc = pyspark.SparkContext('local[*]', "temp")
spark = SparkSession.builder.master("local[*]").appName("temp").getOrCreate()