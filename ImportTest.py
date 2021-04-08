import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
import pandas as pd
import os
import re

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

      # bookRDDs[-1] = bookRDDs[-1].filter(lambda x: x > 1)

   return bookRDDs
  # row = Row("text")
  # bookLists = []
  # for book in bookRDDs:
  #    book_list = book.flatMap(row).collect()
  #    booktext = "".join(book_list)
  #    bookLists.append(booktext)
  # booksDF = sc.parallelize(bookLists).map(row).toDF(["text"])
  # return booksDF

def get_blogs(sc):
    '''
    Arguments: sc
        pyspark spark context obeject

    Returns: rdd
        cleaned spark rdd of the hacker_news_sample.csv
    '''
    tempDataFrame = pd.read_csv("/spring2021/project1/hacker_news_sample.csv",
                                error_bad_lines=False,
                                engine='python').dropna(subset=['text'])

    tempDataFrame['text'] = [re.sub('[<@*&?].*[>@*&?]','',str(x)) for x in tempDataFrame["text"]]
    tempDataFrame['text'] = [re.sub('[&@*&?].*[;@*&?]','',str(x)) for x in tempDataFrame["text"]]
    tempDataFrame['text'] = [re.sub('[#@*&?].*[;@*&?]','',str(x)) for x in tempDataFrame["text"]]

    rdd = sc.parallelize(tempDataFrame['text'])

    return rdd