#####################
# COUNT PUNCTUATION #
#####################

# For analysis of the function of sentences throughout the three corpi

### SET UP ###
from ImportTest import get_books, get_blogs, get_fics
from TokenizeTest import TokenizeThis
import pyspark.sql.functions as f

### ORGANIZING THE DATA ###
tokenizedData = TokenizeThis(sc, "All")
li = ['!', '?', '.']
fdf = tokenizedData.filter(f.col("Token").isin(li))
fdfCount = fdf.groupBy("Source", "Token").count()

### BOOKS ###
fdfCount.filter(f.col("Source") == "Books").agg(f.sum("count")).show(1)
fdfCount.filter(f.col("Source") == "Books").withColumn("prop", (f.col("count")/42235)).orderBy(f.col("count").desc()).show(3)

### BLOGS ###
fdfCount.filter(f.col("Source") == "Blogs").agg(f.sum("count")).show(1)
fdfCount.filter(f.col("Source") == "Blogs").withColumn("prop", (f.col("count")/256264)).orderBy(f.col("count").desc()).show(3)

### FANFICS ###
fdfCount.filter(f.col("Source") == "Fanfic").agg(f.sum("count")).show(1)
fdfCount.filter(f.col("Source") == "Fanfic").withColumn("prop", (f.col("count")/24129)).orderBy(f.col("count").desc()).show(3)
