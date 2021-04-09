#########################
## COMPLEXITY ANALYSES ##
#########################

# Code to calculate Type-Token ratio, Hapax Richness, and Avg. Word Length

# The purpose of the complexity analyses will be expanded on in the presentation, 
# but in summary they provide a glimpse at the lexical profile of a text. The measures
# were decided on after doing a literature review of text complexity measures uses in 
# linguistic analysis. Measures that were easily understood and could be computed in a 
# distributed way were prioritized. 

########## SET - UP ########## 

from ImportTest import get_books, get_blogs, get_fics
from TokenizeTest import TokenizeThis
from tidyRoutine import TidyThis
from pyspark.sql.functions import udf, col, count, countDistinct, sum

tokenizedData = TokenizeThis(sc, "All")
tidiedData = TidyThis(tokenizedData, "Yes", "No")

# Note: We chose to lemmatize the data before calculating these statistics for two reasons: 
#       1. Lemmatization (or stemming) was performed in the cleaning process for many of the resources on this topic that I read
#       2. Lemmatization prevents artificial inflation of the complexity measures when variants of a root word (i.e., 
#          walk, walks, walking) are used. 

##########  TYPE-TOKEN RATIO ########## 
''' The type-token ratio (TTR) represents the ratio of distinct words used in a document 
to the total words used in the document. It serves as a measure of the complexity of
the vocabulary in a document, in particular, the lexical diversity of the document.'''

ttr = tidiedData.groupBy("Source").agg(countDistinct("Lemma").alias("Unique"), count("Lemma").alias("Total"))
ttr = ttr.withColumn('Type-Token Ratio', (ttr['Unique']/ttr['Total']))
ttr.show(3)

##########  HAPAX RICHNESS ##########
''' The Hapax richness is a linguistic measure that calculates the ratio of words 
used only once over the total number of words in the document.'''
 
numWords = tidiedData.groupBy("Source", "Lemma").agg(count("Lemma").alias("Count"))
oneWord = numWords.filter(numWords["Count"] == 1)
oneWord.groupBy("Source").agg(count("Count").alias("Hapax")).show()

##########  AVERAGE WORD LENGTH ##########
'''Average word length represents the average length of words in the document.'''

from fleschKincaid import item_length
avgWL = tidiedData.select("Source", "Lemma", item_length("Lemma").alias("Len"))
avgWL = avgWL.withColumn("Len", avgWL.Len.cast('int'))
avgWL2 = avgWL.groupBy("Source").agg(sum("Len").alias("GroupLen"), count("Lemma").alias("Total"))
avgWL2.withColumn("Average Word Length", (avgWL2["GroupLen"]/avgWL2["Total"])).show(3)
