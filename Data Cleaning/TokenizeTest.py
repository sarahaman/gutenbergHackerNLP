import pyspark
#from pyspark.sql import SparkSession
#from initSession import sc, spark
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql import Row
from ImportTest import get_books, get_blogs, get_fics
import os
import nltk


def TokenizeThis(sc, TknsFrom):
    '''
    Function for extracting tokens from each of the data sources as a DF with two id columns and one token column.

Arguments:
    sc <- SparkContext Object
    TknsFrom <- Specify Data Source ("All", "Blogs", "Books", "Fanfics")

Returns:
    A spark DF that contains columns for "Source", "Title", and "Tokens" with data returned based on string passe$
    '''

    # Individual DataFrames #

    DFschema = ["Source", "Title", "Token"]

    ## Hacker Blogs
    if TknsFrom == "All" or TknsFrom == "Blogs":
        hackRDD = get_blogs(sc)
        hackRDD = hackRDD.map(lambda s: s.replace("n't", " not"))
        hackRDD = hackRDD.flatMap(nltk.word_tokenize)
        hackRDD = hackRDD.map(lambda x: ("Blogs", "Hacker Blogs", x))
        hackDF = hackRDD.toDF(DFschema)
        hackDF.show(5)

    ## Gutenberg Books
    if TknsFrom == "All" or TknsFrom == "Books":
        booksRDDs = get_books(sc)
        books = os.listdir('/spring2021/project1/comparison/')
        for i in range(len(books)):
           # bookRDDs[i] = booksRDDs[i].map(lambda s: s.replace("n't", " not"))
            bookRDD = booksRDDs[i].flatMap(nltk.word_tokenize)
            bookRDD = bookRDD.map(lambda x: ("Books", books[i].split("-")[1][1:-4], x))
            bookDF = bookRDD.toDF(DFschema)
            if i == 0:
                booksDF = bookDF
            else:
                booksDF = booksDF.union(bookDF)
            print(booksDF.count())
        bookDF.show(5)

    if TknsFrom == "All" or TknsFrom == "Fanfics":
        fficRDDs = get_fics(sc)
        fics = os.listdir('/spring2021/project1/gemini/fanficComparison/')
        for i in range(len(fics)):
            fficRDDs[i] = fficRDDs[i].map(lambda s: s.replace("n\'t", " not"))
            fficRDD = fficRDDs[i].flatMap(nltk.word_tokenize)
            fficRDD = fficRDD.map(lambda x: ("Fanfic", fics[i][:-3], x))
            fficDF = fficRDD.toDF(DFschema)
            if i == 0:
                fficsDF = fficDF
            else:
                fficsDF = fficsDF.union(fficDF)
        fficsDF.show(5)

    # Unified DataFrame #
    if TknsFrom == "Blogs":
        outputDF = hackDF
    elif TknsFrom == "Books":
        outputDF = booksDF
    elif TknsFrom == "Fanfics":
        outputDF = fficsDF
    else:
        outputDF = booksDF.union(hackDF).union(fficsDF)

    return outputDF