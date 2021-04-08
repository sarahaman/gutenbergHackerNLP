from Sentencify import SentencifyThis
from pyspark.sql import SQLContext
from initSession import sc
import pyspark.sql.functions as f
import math
from nltk.corpus import stopwords
from pyspark.sql.functions import *


StopWords = stopwords.words("english")

BlogsDF = SentencifyThis(sc, "Blogs")
BooksDF = SentencifyThis(sc, "Books")
FicsDF = SentencifyThis(sc, "Fanfics")

FicsDF = FicsDF.rdd\
               .map(lambda sentence: (sentence[0], [x.lower() for x in sentence[1] if x not in StopWords]))\
               .toDF(["source", "sentence"])
BooksDF = BooksDF.rdd\
                 .map(lambda sentence: (sentence[0], [x.lower() for x in sentence[1] if x not in StopWords]))\
                 .toDF(["source", "sentence"])
BlogsDF = BlogsDF.rdd\
                 .sample(False, 0.1)\
                 .map(lambda sentence: (sentence[0], [x.lower() for x in sentence[1] if x not in StopWords]))\
                 .toDF(["source", "sentence"])

allDF = BlogsDF.union(BooksDF).union(FicsDF)

expDF = allDF.withColumn("exploded_lists", f.explode(f.col("sentence")))
allDF = expDF.groupBy("source").agg(f.collect_list(f.col("exploded_lists")).alias("contents"))

allRDD = allDF.select("contents").rdd.zipWithIndex()

# Get term frequency
map_words =allRDD.flatMap(lambda x: [((x[1]+1,word),1) for word in x[0][0]])
reduce_freq = map_words.reduceByKey(lambda x,y:x+y)
term_freq = reduce_freq.map(lambda x: (x[0][1],(x[0][0],x[1])))

# Get inverse Doc frequency
word_tf = reduce_freq.map(lambda x: (x[0][1],(x[0][0],x[1],1))).map(lambda x:(x[0],x[1][2]))
word_df = word_tf.reduceByKey(lambda x,y: x+y)
term_idf = word_df.map(lambda x: (x[0],math.log10(3/x[1])))

# Join and Transform Data
joined = term_freq.join(term_idf)
rdd = joined.map(lambda x: (x[1][0][0],(x[0],x[1][0][1],x[1][1],x[1][0][1]*x[1][1]))).sortByKey()
df = rdd.map(lambda x: (x[0],x[1][0],x[1][1],x[1][2],x[1][3]))\
         .toDF(["DocumentId","Token","TF","IDF","TF-IDF"])

#df.write.csv('tfidf.csv')

df.show(20)