################################################
# Most common nouns, pronouns, and adjectives! #
################################################
# Doing this by corpus rather than doing it all at once and then filtering
# because it takes a lot less processing time rather than having to apply
# an extra group by. This also prevents us from encountering potential
# java memory errors which have cropped up surprisingly often with the
# blogs data while using the pyspark.sql functions.

# WHEN TURNING THIS INTO A METHOD:
# All you have to do is create a function that takes either "Books", "Blogs"
# or "Fanfic" as the input. ***The sever CANNOT handle an "All" option.
# It will give you a Java OOM error.***

##################
##   Set - Up   ##
##################
from ImportTest import get_books, get_blogs, get_fics
from Sentencify import SentencifyThis
from taggerFunction import getPOS
import pyspark.sql.functions as f
import pyspark.sql.types as T
import nltk

###############
#### Books ####
###############

sentenceData = SentencifyThis(sc, "Books")
pos_tagged = getPOS(sentenceData)

# Nouns
nouns = pos_tagged.filter((f.col("POS") == "NN") | (f.col("POS") == "NNS"))
nounsCounted = nouns.groupby("Token").count()
nounsCounted = nounsCounted.withColumn("Prop", (f.col("count")/814952))
nounsCounted.orderBy(f.col("count").desc()).show(10)

# Pronouns
pronouns = pos_tagged.filter((f.col("POS") == "PRP") | (f.col("POS") == "PRP$"))
pronounsCounted = pronouns.groupby("Token").count()
pronounsCounted = pronounsCounted.withColumn("Prop", (f.col("count")/814952))
pronounsCounted.orderBy(f.col("count").desc()).show(10)

# Adjectives
adjectives = pos_tagged.filter((f.col("POS") == "JJ") | (f.col("POS") == "JJR") | (f.col("POS") == "JJS"))
adjectivesCounted = adjectives.groupby("Token").count()
adjectivesCounted = adjectivesCounted.withColumn("Prop", (f.col("count")/814952))
adjectivesCounted.orderBy(f.col("count").desc()).show(10)

###############
#### Blogs ####
###############

sentenceData = SentencifyThis(sc, "Blogs")
pos_tagged = getPOS(sentenceData)

# Nouns
nouns = pos_tagged.filter((f.col("POS") == "NN") | (f.col("POS") == "NNS"))
nounsCounted = nouns.groupby("Token").count()
nounsCounted = nounsCounted.withColumn("Prop", (f.col("count")/10230782))
nounsCounted.orderBy(f.col("count").desc()).show(10)

# Pronouns
pronouns = pos_tagged.filter((f.col("POS") == "PRP") | (f.col("POS") == "PRP$"))
pronounsCounted = pronouns.groupby("Token").count()
pronounsCounted = pronounsCounted.withColumn("Prop", (f.col("count")/10230782))
pronounsCounted.orderBy(f.col("count").desc()).show(10)

# Adjectives
adjectives = pos_tagged.filter((f.col("POS") == "JJ") | (f.col("POS") == "JJR") | (f.col("POS") == "JJS"))
adjectivesCounted = adjectives.groupby("Token").count()
adjectivesCounted = adjectivesCounted.withColumn("Prop", (f.col("count")/10230782))
adjectivesCounted.orderBy(f.col("count").desc()).show(10)

# Total Proportions
totalPOS = pos_tagged.groupBy("POS").count()
totalPOS = totalPOS.withColumn("prop", (f.col("count")/10230782)).orderBy(f.col("count").desc())

#################
#### Fanfics ####
#################

sentenceData = SentencifyThis(sc, "Fanfics")
pos_tagged = getPOS(sentenceData)

# Nouns
nouns = pos_tagged.filter((f.col("POS") == "NN") | (f.col("POS") == "NNS"))
nounsCounted = nouns.groupby("Token").count()
nounsCounted = nounsCounted.withColumn("Prop", (f.col("count")/401037))
nounsCounted.orderBy(f.col("count").desc()).show(10)

# Pronouns
pronouns = pos_tagged.filter((f.col("POS") == "PRP") | (f.col("POS") == "PRP$"))
pronounsCounted = pronouns.groupby("Token").count()
pronounsCounted = pronounsCounted.withColumn("Prop", (f.col("count")/401037))
pronounsCounted.orderBy(f.col("count").desc()).show(10)

# Adjectives
adjectives = pos_tagged.filter((f.col("POS") == "JJ") | (f.col("POS") == "JJR") | (f.col("POS") == "JJS"))
adjectivesCounted = adjectives.groupby("Token").count()
adjectivesCounted = adjectivesCounted.withColumn("Prop", (f.col("count")/401037))
adjectivesCounted.orderBy(f.col("count").desc()).show(10)

# Total proportion of each POS
totalPOS = pos_tagged.groupBy("POS").count()
totalPOS = totalPOS.withColumn("prop", (f.col("count")/401037)).orderBy(f.col("count").desc())
