
########################################################
# DO NOT RUN! YOU CAN BUT LIKE? IT'S NOT SUPER USEFUL! #
########################################################

# READ BEFORE RUNNING:
#      Will only give the output for the Fanfic dataframe as it is written
#      right now. To change the output, adjust the input in SentencifyThis.
#      will not work with "All" at the moment.
# RESULTS:
#      Screenshots of the results are located in the DC_Project1_Results
#      google document along with interpretations to be included in the
#      final slides.
#         - Sara

###################
# SENTIMENT SETUP #
###################


from ImportTest import get_books, get_blogs, get_fics
from TokenizeTest import TokenizeThis
from tidyRoutine import TidyThis
from Sentencify import SentencifyThis
import pyspark.sql.functions as f
from nltk.sentiment import SentimentIntensityAnalyzer
from pyspark.sql.types import ArrayType, StringType, FloatType


######################
# SENTIMENT ANALYSIS #
######################

sia = SentimentIntensityAnalyzer()
sentenceData = SentencifyThis(sc, "Fanfics")

def polarity(text):
        text = ' '.join(text)
        score = sia.polarity_scores(text)
        return score

def sen_cleaner(str):
        li = list(str.values())
        return li

sen_cleaner = f.udf(sen_cleaner, ArrayType(FloatType()))
polarity = f.udf(polarity)
scoredDF = sentenceData.select("source", polarity("sentence").alias("Sentiment"))
cleanedDF = scoredDF.select("source", sen_cleaner("Sentiment").alias("Sentiment"))
sentiment = cleanedDF.select("source", cleanedDF.Sentiment[0].alias("NEG"), cleanedDF.Sentiment[1].alias("NEU"), cleanedDF.Sentiment[2].alias("POS"), cleanedDF.Sentiment[3].alias("Compound"))
avgsentiment = sentiment.agg(f.avg("NEG"), f.avg("NEU"), f.avg("POS"), f.avg("Compound"))