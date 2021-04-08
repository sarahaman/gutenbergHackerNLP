import pyspark
from pyspark.sql import SparkSession
#from initSession import sc, spark
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import col, udf
from pyspark.sql import Row
from ImportTest import get_books, get_blogs, get_fics
from TokenizeTest import TokenizeThis
import os
import nltk


def SentencifyThis(sc, source):
    '''
A function for taking text source RDDs (or lists of RDDs) and converting each into a DF wherein row in to a list of tokens in each sentence with an identifier at index 0.

Arguments:
    sc <- SparkContext
    source <- the source you want to return as an DF of tokenized sentences
    '''
    puncArray = ["!", "&", "(", ")", "*", ",", ".", ":", ";", "<", ">", "?", "@", "[", "]", "^", "_", "`", "~", ";", "'", '"', "“", "”", "’", "="]
    schema = ["source", "sentence"]
    allDF = 'null'
    ficDF = 'null'
    bookDF = 'null'
    blogDF = 'null'

    # Sentence Fanfiction
    if source in ["All", "Fanfics"]:
        ficRDDs = get_fics(sc)
        for i in range(len(ficRDDs)):
            fic = ficRDDs[i].flatMap(nltk.sent_tokenize)\
                            .map(nltk.word_tokenize)\
                            .map(lambda sentence: [x for x in sentence if x not in puncArray])\
                            .map(lambda sentence: [x.lower() for x in sentence])\
                            .map(lambda x: ('Fanfic', x))\
                            .toDF(schema)
            if i == 0:
                ficDF = fic
            else:
                ficDF = ficDF.union(fic)
        ficDF.show(10)

    # Sentence Blogs
    if source in ["All", "Blogs"]:
        blogRDD = get_blogs(sc)
        blogDF = blogRDD.flatMap(nltk.sent_tokenize)\
                        .map(nltk.word_tokenize)\
                        .map(lambda sentence: [x for x in sentence if x not in puncArray])\
                        .map(lambda sentence: [x.lower() for x in sentence])\
                        .map(lambda x: ('Blogs', x))\
                        .toDF(schema)

        blogDF.show(10)

    # Sentence Books
    if source in ["All", "Books"]:
        bookRDDs = get_books(sc)
        for i in range(len(bookRDDs)):
            book = bookRDDs[i].flatMap(nltk.sent_tokenize)\
                              .map(nltk.word_tokenize)\
                              .map(lambda sentence: [x for x in sentence if x not in puncArray])\
                              .map(lambda sentence: [x.lower() for x in sentence])\
                              .map(lambda x: ('Books', x))\
                              .toDF(schema)
            if i == 0:
                bookDF = book
            else:
                bookDF = bookDF.union(book)
        bookDF.show(10)

    if source == "All":
        allDF = bookDF.union(blogDF).union(ficDF)

    dfReturn  = {
        "All": allDF,
        "Fanfics": ficDF,
        "Blogs": blogDF,
        "Books": bookDF
    }

    return dfReturn.get(source, "Invalid Entry. Please select 'All', 'Fanfics', 'Books', 'Blogs'")