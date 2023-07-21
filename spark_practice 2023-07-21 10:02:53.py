# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *



# COMMAND ----------

spark = SparkSession.builder.appName('Spark_practice').getOrCreate()

# COMMAND ----------

states = {"NY":"New York", "CA":"California", "FL":"Florida"}

# COMMAND ----------

broadcastStates = spark.sparkContext.broadcast(states)

# COMMAND ----------

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]


# COMMAND ----------

def state_convert(code):
    return broadcastStates.value[code]


# COMMAND ----------

result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)
result.show(truncate=False)

# COMMAND ----------

dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]

# COMMAND ----------

df = spark.createDataFrame(data=dataDictionary, schema = ['name','properties'])


# COMMAND ----------

df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# Using StructType schema
from pyspark.sql.types import StructField, StructType, StringType, MapType,IntegerType
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])

# COMMAND ----------

df2 = spark.createDataFrame(data=dataDictionary, schema = schema)
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

df3=df.rdd.map(lambda x: \
    (x.name,x.properties["hair"],x.properties["eye"])) \
    .toDF(["name","hair","eye"])
df3.printSchema()
df3.show()

# COMMAND ----------

df.withColumn("hair",df.properties.getItem("hair")) \
  .withColumn("eye",df.properties.getItem("eye")) \
  .drop("properties") \
  .show()


# COMMAND ----------

df.withColumn("hair",df.properties["hair"]) \
  .withColumn("eye",df.properties["eye"]) \
  .drop("properties") \
  .show()

# COMMAND ----------

from pyspark.sql.functions import explode,map_keys,col
keysDF = df.select(explode(map_keys(df.properties))).distinct()
keysList = keysDF.rdd.map(lambda x:x[0]).collect()
keyCols = list(map(lambda x: col("properties").getItem(x).alias(str(x)), keysList))
df.select(df.name, *keyCols).show()

# COMMAND ----------


