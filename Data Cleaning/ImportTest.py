################################
## EARLY DATA PRE-PROCESSING  ##
################################


##################
##    SET UP    ##
##################

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pandas as pd
import pyspark.sql.functions as f
import os
import re

#####################################
## METHODS FOR GETTING CLEAN RDDS  ##
#####################################

def get_books(sc):
   '''
   Arguments: sc
       pyspark spark context obeject

   Returns: rdd
       cleaned spark rdd of the hacker_news_sample.csv
   '''
   bookLists = []
   books =  os.listdir('/spring2021/project1/comparison/')
   bookRDDs = []
   for book in books:
       bookRDDs.append(sc.textFile("".join(['/spring2021/project1/comparison/',book])))
   
   return bookRDDs

def get_blogs(sc):
   '''
   Arguments: sc
        pyspark spark context obeject
   Returns: rdd
        cleaned spark rdd of the hacker_news_sample.csv and the blogtext.csv
   '''
   spark = SparkSession.builder.getOrCreate()
   newstextDF = spark.read.csv("/spring2021/project1/hacker_news_sample.csv", inferSchema = True, header = True).dropna(subset=['text'])
   blogtextDF = spark.read.csv("/spring2021/project1/blogtext.csv", inferSchema = True, header = True).dropna(subset=['text'])

   blogtextDF = blogtextDF.select('text')
   newstextDF = newstextDF.select('text')
   webtext = newstextDF.union(blogtextDF)

   def cleanThis(x):
      import re
      x = re.sub('[<@*&?].*[>@*&?]','',x)
      x = re.sub('[&@*&?].*[;@*&?]','',x)
      x = re.sub('[#@*&?].*[;@*&?]','',x)
      output = re.sub(r'[^0-9a-zA-z,.!?]+', ' ', x)
      return output


   cleanThis = f.udf(cleanThis)
   cleanText = webtext.select(cleanThis('text').alias('text'))
   web_rdd = cleanText.rdd.flatMap(list)
   return web_rdd

def get_fics(sc):
   '''
   Arguments: sc
        pyspark spark context object
   Returns: rdd
        cleaned spark rdd of the fanfic data
   '''
    fanficRDDs = []
    fanfics =  os.listdir('/spring2021/project1/gemini/fanficComparison/')

    for fic in fanfics:
        fanficRDDs.append(sc.textFile("".join(['/spring2021/project1/gemini/fanficComparison/', fic])))

    return fanficRDDs

