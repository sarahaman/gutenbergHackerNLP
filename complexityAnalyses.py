from ImportTest import get_books, get_blogs, get_fics
from TokenizeTest import TokenizeThis
from tidyRoutine import TidyThis
from pyspark.sql.functions import udf, col, count, countDistinct

# TYPE TOKEN RATIO
tokenizedData = TokenizeThis(sc, "All")
tidiedData = TidyThis(tokenizedData, "Yes", "No")
ttr = tidiedData.groupBy("Source").agg(countDistinct("Stem").alias("Unique"), count("Stem").alais("Total"))
ttr = ttr.withColumn('Type-Token Ratio', (ttr['Unique']/ttr['Total']))
ttr.show(3)

# HAPAX RICHNESS
numWords = tidiedData.groupBy("Source", "Stem").agg(count("Stem").alias("Count"))
oneWord = numWords.filter(numWords["Count"] == 1)
oneWord.groupBy("Source").agg(count("Count").alias("Hapax")).show()

# AVERAGE WORD LENGTH
from fleschKincaid import item_length
avgWL = tidiedData.select("Source", "Stem", item_length("Stem").alias("Len"))
avgWL = avgWL.withColumn("Len", avgWL.Len.cast('int'))
avgWL2 = avgWL.groupBy("Source").agg(pyspark.sql.functions.sum("Len").alias("GroupLen"), count("Stem").alias("Tot$
avgWL2.withColumn("Average Word Length", (avgWL2["GroupLen"]/avgWL2["Total"])).show(3)