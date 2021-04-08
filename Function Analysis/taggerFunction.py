from ImportTest import get_books, get_blogs, get_fics
from Sentencify import SentencifyThis
import pyspark.sql.functions as f
import pyspark.sql.types as T
import nltk

def getPOS(sentenceData):
    '''
    Given a dataframe of sentences, returns a dataframe with the POS tagged where each row corresponds to a token from a source.
    '''

    def pos_tagger(sentence):
        x = nltk.pos_tag(sentence)
        return x

    pos_tagger = f.udf(pos_tagger, T.ArrayType(T.ArrayType(T.StringType())))
    tagged = sentenceData.select("Source", pos_tagger("sentence").alias("Sen POS"))
    tagged = tagged.select("Source", f.explode("Sen POS"))
    pos_tagged = tagged.select("Source", tagged.col[0].alias("Token"), tagged.col[1].alias("POS"))
    pos_tagged = pos_tagged.withColumn("POS", f.when(pos_tagged["Token"] == "i", "PRP").otherwise(pos_tagged["POS"]))
    pos_tagged = pos_tagged.withColumn("POS", f.when(pos_tagged["Token"] == "s", "VBP").otherwise(pos_tagged["POS"]))
    return pos_tagged