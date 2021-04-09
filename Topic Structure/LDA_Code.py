from pyspark.ml.feature import CountVectorizer, IDF
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import LDA, LDAModel
#from initSession import sc
from pyspark.sql import SQLContext
from nltk.corpus import stopwords
#from Sentencify import Sentencify
import nltk
import os

'''
*Not to be included in final Analysis*

Original Plan:
    LDA topic modeling is challenging because tuning the model with respect to perplexity can lead to a better discriminant model.
    The ultimate consequence of this is that the model that shows the greatest semantic similarity (i.e. the most human-interpretable) is not the most effective at predicting topic distibution.

Known Issues:
    This takes a very long time and often runs out of memory as a result of processing blog data in line 47
'''

def LDAThis(sc, RDD, minFreq,  numTopics, maxIter, wordsPerTopic):
     '''
Arguments:
     sc: A SparkContext Object
     RDD: An RDD with rows as tokenized sentences
     minFreq: Minimum document frequency for CountVectorizer
     numTopics: Number of Topics
     maxIter: Max number of iterations for LDA train
     wordsPerTopic: Number of words to show per topic
     topWords: Number of words to show per topic
Requirements
     sqlContext = SQLContext(sc) <- must be defined outside function
     '''
     StopWords = stopwords.words("english")
     sqlContext = SQLContext(sc)
     # Structure Data
     idRDD = RDD.map(lambda words: [x for x in words if x.isalpha() and x not in StopWords]).filter(lambda x: len(x) > 2).zipWithIndex()
     idDF = sqlContext.createDataFrame(idRDD, ["tokens",'index'])
     # Term Frequency
     CVecModel = CountVectorizer(inputCol = "tokens", outputCol = "rawFeatures", vocabSize = 5000, minDF = minFreq).fit(idDF)
     resultCVec = CVecModel.transform(idDF)
     vocabArray = CVecModel.vocabulary
     #IDF
     idf = IDF(inputCol = "rawFeatures", outputCol = "features")
     idfModel = idf.fit(resultCVec)
     resultTFIDF = idfModel.transform(resultCVec)
     # LDA
     resultLDA = LDA.train(resultTFIDF.select('index', 'features').rdd.mapValues(Vectors.fromML).map(list), k = numTopics, maxIterations = maxIter)
     topicIndices = sc.parallelize(resultLDA.describeTopics(maxTermsPerTopic = wordsPerTopic))
     topicsFinal = topicIndices.map(lambda topic: render_topics(topic, wordsPerTopic, vocabArray)).collect()

     # Show Topics
     for topic in range(len(topicsFinal)):
          print("Topic" + str(topic) + ":")
          for term in topicsFinal[topic]:
               print(term)
          print('\n')
     return resultLDA




def render_topics(topic, wordsPerTopic, vocabArray):
     topic_terms = topic[0]
     topic_weights = topic[1]
     result = []
     for i in range(wordsPerTopic):
          term = (vocabArray[topic_terms[i]], topic_weights[i])
          result.append(term)
     return result


### Import Data
'''
booksDF = SentencifyThis(sc, "Books")
ficsDF = SentencifyThis(sc, "Fanfics")
blogsDF = SentencifyThis(sc, "Blogs")

### Wrangle into row per sentence
blogsRDD = blogsDF.rdd.sample(False, 0.15).map(lambda x: x[1])
ficsRDD = ficsDF.rdd.map(lambda x: x[1])
booksRDD = booksDF.map(lambda x: x[1])

LDAThis(sc, booksRDD, 5, 7, 100, 25)
LDAThis(sc, ficsRDD, 5, 7, 100, 25)
LDAThis(sc, blogsRDD, 5, 7, 100, 25)
'''
