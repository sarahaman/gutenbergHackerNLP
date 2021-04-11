from ImportTest import get_books, get_blogs, get_fics
from TokenizeTest import TokenizeThis
from tidyRoutine import TidyThis
from Sentencify import SentencifyThis
from nltk.corpus import cmudict
from pyspark.sql.functions import udf, col

d = cmudict.dict()

tokenizedData = TokenizeThis(sc, "All")
tidiedData = TidyThis(tokenizedData, "No", "No")

def nysl(word):
        try:
                a = [len(list(y for y in x if y[-1].isdigit())) for x in d[word.lower()]]
                a = a[0]
        except:
                a = 2
        return a

nysl_2 = udf(nysl)
sylData = tidiedData.select("Source", "Token", nysl_2("Token").alias("Syl"))

# Length function
def item_length(x):
        return len(x)

item_length = udf(item_length)

# Get Sentence Length
sentenceData = SentencifyThis(sc, "All")
senLen = sentenceData.select("source", "sentence", item_length("sentence").alias("Len"))

# Average syllabes and sentence length per source
avgSyl = sylData.withColumn("Syl", sylData.Syl.cast('int')).groupBy("Source").avg("Syl")
avgSenLen = senLen.withColumn("Len", senLen.Len.cast('int')).groupBy("Source").avg("Len")


# FLESCH-KINCAID
ffData = avgSyl.join(avgSenLen, avgSyl.Source == avgSenLen.Source)
FK = ffData.withColumn('Flesch-Kincaid', (0.39*ffData['avg(Len)'] + (11.8 * ffData['avg(Syl)']) - 15.59))
FK.show(3)
