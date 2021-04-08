#####################################################
# DO NOT RUN THIS FILE !!! DO NOT RUN THIS FILE !!! #
#####################################################

# file used as a reference for work I did in pyspark; the script
# will not run as a cohesive document

################
#    NGRAMS    #
################

from ImportTest import get_books, get_blogs, get_fics
import pyspark.sql.functions as f
from Sentencify import SentencifyThis
from pyspark.ml.feature import NGram
from pyspark.sql.types import ArrayType, FloatType, StringType
import nltk

# READING THE DATAFRAMES
# Books: worked with full dataset
sentenceData = SentencifyThis(sc, "Books")

# Blogs: need very small samples
sentenceData = SentencifyThis(sc, "Blogs")
sentenceData = sentenceData.sample(0.01, 123)

# Fanfics: randomly select 1/4 of the data
importData = SentencifyThis(sc, "Fanfics")
sentenceData = importData.sample(0.25, 917)

################
#     SETUP    #
################

stop_words = nltk.corpus.stopwords.words('english')
new_stop_words = ['ebook', 'project', 'gutenberg', '--', 'copy', '``', "prev", "id='storytext", "''", '│', "'s", $
stop_words.extend(new_stop_words)

def stopword_remover(sentence):
        output = [word for word in sentence if word not in stop_words]
        return output

def replace_function(sentence):
        output = [w.replace("n't", 'not') for w in sentence]
        return output

stopword_remover = f.udf(stopword_remover, ArrayType(StringType()))
replace_function = f.udf(replace_function, ArrayType(StringType()))

sentenceData = sentenceData.select("source", replace_function("sentence").alias("Sentence"))
sentenceData = sentenceData.select("source", stopword_remover("Sentence").alias("Sentence"))

################
#   TRIGRAMS   #
################

# DROPPED FROM THE PRESENTATION // don't reliably run without very small samples
# Also provide little additional useful information: very few repeated trigram phrases

trigram_df = NGram(n=3, inputCol="Sentence", outputCol="Trigrams").transform(sentenceData)
trigram_df = trigram_df.select("source", f.explode("Trigrams").alias("Trigrams"))
triCounted = trigram_df.groupBy("Trigrams").count()
triCounted.orderBy(f.col("count").desc()).show(10)

# FOR BOOKS
triCounted.filter((f.col("Count") != 14) & (f.col("Count") != 13)).orderBy(f.col("Count").desc()).show(10)

#OTHERS
triCounted.orderBy(f.col("Count").desc()).show(10)

################
#   BIGRAMS   #
################

bigram_df = NGram(n=2, inputCol="Sentence", outputCol="Bigrams").transform(sentenceData)
bigram_df = bigram_df.select("source", f.explode("Bigrams").alias("Bigrams"))
biCounted = bigram_df.groupBy("Bigrams").count()
biCounted.orderBy(f.col("count").desc()).show(10)