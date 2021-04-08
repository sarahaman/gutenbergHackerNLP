import pyspark
from pyspark.sql import SparkSession
#from initSession import sc, spark
from pyspark.sql.functions import col, udf
#from ImportTest import get_books, get_blogs
#from TokenizeTest import TokenizeThis
import os
import nltk

def TidyThis(tokenizedData, lem, tg):

    # Packages neccessary.f
    import pyspark.sql.functions as f
    import nltk
    from nltk.corpus import stopwords
    from nltk.stem import WordNetLemmatizer
    nltk.download('stopwords')
    lm = WordNetLemmatizer()

    ###################
    ### Subroutines ###
    ###################

    # PARTS OF SPEECH TAGGER
    def pos_tagger(word, dtype):
        x = nltk.pos_tag(nltk.word_tokenize(word))[0][1]
        return x

    # LEMMATIZING THE TOKENS
    def word_lemmatizer(word):
        x = lm.lemmatize(word)
        return x

    # BODY OF THE ROUTINE

    # To Lower
    tokenizedData = tokenizedData.withColumn("Token", f.lower(f.col("Token")))

    # Remove Punctuation
    puncArray = ["!", "&", "(", ")", "*", ",", ".", ":", ";", "<", ">", "?", "@", "[", "]", "^", "_", "`", "``", "+", "-", "''",  "~", ";", "'", '"', "“", "”", "’"]
    tokenizedData = tokenizedData.filter(tokenizedData.Token.isin(puncArray) == False)

    # Remove Stopwords
    stop_words = set(stopwords.words('english'))
    tokenizedData = tokenizedData.filter(tokenizedData.Token.isin(stop_words) == False)

    # POS tagging
    if tg == "Yes":
        pos_tagger = f.udf(pos_tagger)
        tokenizedData = tokenizedData.select("Source", "Title", "Token", pos_tagger("Token").alias("POS"))

    # Lemmatization
    if lem == "Yes":
        word_lemmatizer = f.udf(word_lemmatizer)
        if tg == "Yes":
            tokenizedData = tokenizedData.select("Source", "Title", "POS",  word_lemmatizer("Token").alias("Lemma"))
        else:
            tokenizedData = tokenizedData.select("Source", "Title",  word_lemmatizer("Token").alias("Lemma"))

    # Return Statement
	return tokenizedData
