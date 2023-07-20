# Databricks notebook source
def total(list1):
    list2=0
    for i in list1:
        list2=list2+i
    return list2

# COMMAND ----------

total([1,2,3,4,5])

# COMMAND ----------

df.WithColumn("column1", trim("column11")).show(Truncate=False)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data = [(1,"ABC    "), (2,"     DEF"),(3,"        GHI    ") ]

# COMMAND ----------

sample_data=[(1,"  shubham  "),(2," pratap"),(  5  ,"saurabh ")]

# COMMAND ----------

df=spark.createDataFrame(data=sample_data, schema=("id","name"))

# COMMAND ----------

display(df)

# COMMAND ----------

df.withColumn("name", trim("name")).show(truncate=False)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %scala
# MAGIC import spark.sqlContext.implicits._
# MAGIC val data = Seq(("","CA"), ("Julia",""),("Robert",null),(null,""))
# MAGIC val df = data.toDF("name","state")
# MAGIC df.show(false)

# COMMAND ----------

# MAGIC %scala
# MAGIC val data = Seq(("James","Sales",34), ("Michael","Sales",56),
# MAGIC                ("Robert","Sales",30), ("Maria","Finance",24) )
# MAGIC import spark.implicits._
# MAGIC val df1 = data.toDF("name","dept","age")
# MAGIC df1.printSchema()

# COMMAND ----------

data = [["1", "sravan", "IT", 45000],
        ["2", "ojaswi", "CS", 85000],
        ["3", "rohith", "CS", 41000],
        ["4", "sridevi", "IT", 56000],
        ["5", "bobby", "ECE", 45000],
        ["6", "gayatri", "ECE", 49000],
        ["7", "gnanesh", "CS", 45000],
        ["8", "bhanu", "Mech", 21000]
        ]

columns = ['ID', 'NAME', 'DEPT', 'FEE']
  

# COMMAND ----------

df=spark.createDataFrame(data=data, schema=columns)

# COMMAND ----------

display(df)

# COMMAND ----------

df.groupBy("DEPT")

# COMMAND ----------

df.filter(col('FEE') >= 56700).show()

# COMMAND ----------

df.where(col('FEE') >= 56700).show()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

window_spec=df.Window.partitionBy("DEPT").orderBy("NAME")
#df=df.withColumn("row_number",row_number().over(window_spec))

# COMMAND ----------

from pyspark.sql import SparkSession

from functools import reduce




# Create a SparkSession

spark = SparkSession.builder.getOrCreate()

# Create a sample DataFrame

data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]

df = spark.createDataFrame(data, ["id", "name", "age"])

# Define the column mapping dictionary

column_mapping = {"id": "new_id", "name": "new_name", "age": "new_age"}

# Rename the columns using reduce and withColumnRenamed

renamed_df = reduce(lambda acc, mapping: acc.withColumnRenamed(mapping[0], mapping[1]), column_mapping.items(), df)

# Show the renamed DataFrame

renamed_df.show()

# COMMAND ----------

# DBTITLE 1,Replace Empty Value with None on All DataFrame Columns
df=df.select([when(col(c))=="",None).otherwise(col(c)).alias (c) for c in df.columns])

# COMMAND ----------

# df.select(countDistinct("col1","col2")).show()

# COMMAND ----------

#df.groupBy("same").count().show()

# COMMAND ----------

import numpy as np
from pyspark.sql.functions import col,isnan, when, count
data = [
    ("James","CA",np.NaN), ("Julia","",None),
    ("Ram",None,200.0), ("Ramya","NULL",np.NAN)
]
df =spark.createDataFrame(data,["name","state","number"])
df.show()

# COMMAND ----------

#df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]
   ).show()


# COMMAND ----------

# DBTITLE 1,Find Count of Null, None, NaN of All DataFrame Columns
df.select([count(when(isnan(c) | col(c).isNull(), c)).alias (c) for c in df.columns]).show()

# COMMAND ----------


