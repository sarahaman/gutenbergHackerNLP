##########################################
##  EARLY STAGE VERSION OF SENTENCIFY   ##
##########################################

import pyspark
from pyspark.sql import SparkSession
from initSession import sc, spark
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql import Row
from ImportTest import get_books, get_blogs, get_fics
from TokenizeTest import TokenizeThis
import os
import nltk

 puncArray = ["!", "&", "(", ")", "*", ",", ".", ":", ";", "<", ">", "?", "@", "[", "]", "^", "_", "`", "``", "+", "-", "''",  "~", ";", "'", '"', "“", "”", "’"]

schema = ["source", "sentence"]

# Sentence Fanfiction

ficRDDs = get_fics(sc)

for i in range(len(ficRDDs)):
    fic = ficRDDs[i].flatMap(nltk.sent_tokenize)\
                      .map(nltk.word_tokenize)\
                      .map(lambda sentence: [x for x in sentence if x not in puncArray])\
                      .map(lambda x: ('Fanfic', x))\
                      .toDF(schema)
    if i == 0:
        ficDF = fic
    else:
        ficDF = ficDF.union(ficDF)

ficDF.show(10)

# Sentence Blogs
blogRDD = get_blogs(sc)

blogsDF = blogRDD.map(nltk.word_tokenize)\
               .map(lambda sentence: [x for x in sentence if x not in puncArray])\
               .map(lambda x: ('Blogs', x))\
               .toDF(schema)

blogsDF.show(10)

# Sentence Books

bookRDDs = get_books(sc)

for i in range(len(bookRDDs)):
    book = bookRDDs[i].flatMap(nltk.sent_tokenize)\
                      .map(nltk.word_tokenize)\
                      .map(lambda sentence: [x for x in sentence if x not in puncArray])\
                      .map(lambda x: ('Books', x))\
                      .toDF(schema)
    if i == 0:
        bookDF = book
    else:
        bookDF = bookDF.union(book)

bookDF.show(10)