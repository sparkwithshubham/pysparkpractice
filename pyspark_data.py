from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Initiallize sparksession
spark=SparkSession.builder.getOrCreate()

# Initiallize sparkcontext
sc=spark.sparkContext

rdd=sc.parallelize([1,2,3,4,5,6,7,8,9,10,11,12],4)

# get number of partition
rdd.getNumPartitions()

# use glom function
rdd.glom().collect()

# use reduce function
rdd.reduce(lambda x,y: x+y)

# use fold function
rdd.fold (0,lambda x,y : x+y)

#wordcount program (count each word in a given file)
text_file=sc.textFile("C:trial/testfile.txt")

count_word=text_file.flatMap(lambda line:line.split(" ")).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)

output=count_word.collect()

for (word,count) in output:
    print("%s: %i" % (word,count))





