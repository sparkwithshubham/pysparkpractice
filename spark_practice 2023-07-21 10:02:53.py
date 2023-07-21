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

simpleData = (("James","","Smith","36636","NewYork",3100), \
    ("Michael","Rose","","40288","California",4300), \
    ("Robert","","Williams","42114","Florida",1400), \
    ("Maria","Anne","Jones","39192","Florida",5500), \
    ("Jen","Mary","Brown","34561","NewYork",3000) \
  )

# COMMAND ----------

columns= ["firstname","middlename","lastname","id","location","salary"]


# COMMAND ----------

df = spark.createDataFrame(data = simpleData, schema = columns)

# COMMAND ----------

df.printSchema()
df.show(truncate=False)


# COMMAND ----------

#drop column by different types of methods

df.drop("firstname")


# COMMAND ----------

df.drop(col("firstname"))

# COMMAND ----------

df.drop(df.firstname) 

# COMMAND ----------

#drop all columns by creating new list of it

cols = ("firstname","middlename","lastname")

df.drop(*cols) \

# COMMAND ----------

df.na.drop().show(truncate=False)

# COMMAND ----------

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})
        ]

# COMMAND ----------

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.show()

# COMMAND ----------

df2 = df.select(df.name,explode(df.knownLanguages))
df2.printSchema()
df2.show()

# COMMAND ----------

df3 = df.select(df.name,explode(df.properties))
df3.show()

# COMMAND ----------

from pyspark.sql.functions import explode_outer,posexplode
df.select(df.name,explode_outer(df.knownLanguages)).show()

# COMMAND ----------

df.select(df.name,posexplode(df.knownLanguages)).show()

# COMMAND ----------


