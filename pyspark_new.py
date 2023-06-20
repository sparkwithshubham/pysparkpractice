from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#Initialize spark

spark=SparkSession.builder.master("local").appName("sparkglue").getOrCreate()

#Timestamp convert to datetype

df=spark.createDataFrame(data=[("1","2023-06-20 12:01:19.000")], schema=("input","input_timestamp"))

df.show()

# Check schema

df.printSchema()

# change timestamp type to datetype by using cast function
df.withColumn("date_type",col("input_timestamp").cast("date")).show(truncate=False)

#how to get current date and current time stamp

df.withColumn("current__date",current_date())\
    .withColumn("current_timestamp",current_timestamp()).show(truncate=False)


