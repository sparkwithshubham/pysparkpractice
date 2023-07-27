# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

simple_data=[("sagar","CSE","UP",80),
             ("shivam","IT","MP",86)]

# COMMAND ----------

column_name=["student_name","department","city","marks"]

# COMMAND ----------

df_1=spark.createDataFrame(data=simple_data,schema=column_name)

# COMMAND ----------

display(df_1)
df_1.printSchema()

# COMMAND ----------

simple_data1=[("pavan","CSE","UP",80),
             ("nikita","MECH","AP",70)]

column_name1=["student_name","department","city","marks"]

# COMMAND ----------

df_2=spark.createDataFrame(data=simple_data1,schema=column_name1)

# COMMAND ----------

df_2.printSchema()
df_2.show()

# COMMAND ----------

df_1.union(df_2).show()

# COMMAND ----------

simple_data3=[("sagar","CSE","UP",80,"marathi"),
             ("shivam","IT","MP",86,"telgu")]

column_name3=["student_name","department","city","marks","language"]

# COMMAND ----------

df_3=spark.createDataFrame(data=simple_data3,schema=column_name3)

# COMMAND ----------

display(df_3)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

schema=StructType([StructField("RechargeID",StringType(),True),
                   StructField("RechargeDate", IntegerType(),True),
                   StructField("RemainingDays",IntegerType(),True),
                   StructField("ValidityStatus",StringType(),True)])

df=spark.read.schema(schema).csv("dbfs:/FileStore/date_file.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn("Recharge_Date" , date_format("RechargeDate","yyyy-MM-dd"))

# COMMAND ----------


def create_new_column(df, name, colName, datatype):
    df=df.withColumn(name , date_format(colName,datatype))
    return df

# COMMAND ----------

df4=create_new_column(df,"changed_Recharge_date" , "RechargeDate","yyyy-MM-dd")

# COMMAND ----------

def create_new_column(df, name, colName, datatype):
    df=df.withColumn(name .date_format(colName .cast(to_date), datatype))
    return df

# COMMAND ----------

df3=create_new_column(df,"changed_Recharge_date" , "RechargeDate", "YYYY-MM-DD")

# COMMAND ----------

def expiry_date(df, colName1, colName2):
    df=df.withColumn(colName1, df[colName2]).withColumnRenamed(colName1,colName2)
    return df

# COMMAND ----------

df2=expiry_date(df, "changed_Recharge_date", "RechargeDate")

# COMMAND ----------

display(df2)

# COMMAND ----------

def expiry_date(df, colName1, colName2,colName3):
    df=df.withColumn(colName1, df[colName2]).withColumnRenamed(colName1,colName3)
    return df

# COMMAND ----------

df3=expiry_date(df, "RemainingDays", "changed_Recharge_date","expiryDate")

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/FileStore/feeds_json_data.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

like_dislike_schema = StructType([
    StructField("dislikes", LongType(), nullable=True),
    StructField("likes", LongType(), nullable=True),
    StructField("userAction", LongType(), nullable=True)
])

multi_media_schema = ArrayType(
    StructType([
        StructField("createAt", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("id", LongType(), nullable=True),
        StructField("likeCount", LongType(), nullable=True),
        StructField("mediatype", LongType(), nullable=True),
        StructField("name", StringType(), nullable=True),
        StructField("place", StringType(), nullable=True),
        StructField("url", StringType(), nullable=True)
    ]),
    containsNull=True
)

feeds_schema = ArrayType(
    StructType([
        StructField("code", LongType(), nullable=True),
        StructField("commentCount", LongType(), nullable=True),
        StructField("createdAt", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True),
        StructField("feedsComment", StringType(), nullable=True),
        StructField("id", LongType(), nullable=True),
        StructField("imagePaths", StringType(), nullable=True),
        StructField("images", StringType(), nullable=True),
        StructField("isdeleted", BooleanType(), nullable=True),
        StructField("lat", LongType(), nullable=True),
        StructField("likeDislike", like_dislike_schema, nullable=True),
        StructField("lng", LongType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("mediatype", LongType(), nullable=True),
        StructField("msg", StringType(), nullable=True),
        StructField("multiMedia", multi_media_schema, nullable=True),
        StructField("profilePicture", StringType(), nullable=True),
        StructField("title", StringType(), nullable=True),
        StructField("userId", LongType(), nullable=True),
        StructField("videoUrl", StringType(), nullable=True)
    ]),
    containsNull=True
)

# Define the main schema for the DataFrame
custom_schema = StructType([
    StructField("feeds", feeds_schema, nullable=True),
    StructField("totalFeed", LongType(), nullable=True)
])

# COMMAND ----------

df1=spark.read.option("multiline",True).schema(custom_schema).json("dbfs:/FileStore/feeds_json_data.json")

# COMMAND ----------

display(df1)

# COMMAND ----------

df2 = df1.select("*").withColumn("feeds",explode_outer("feeds")).select("*","feeds.*")\
    .select("*","likeDislike.*")\
    .select("*").withColumn("multiMedia",explode_outer("multiMedia")).select("*","multiMedia.*")\
    .drop("feeds","likeDislike","multiMedia")    
display(df2)

# COMMAND ----------

df1 = df.select("*").withColumn("feeds",explode_outer("feeds")).select("*","feeds.*").drop("feeds")

# COMMAND ----------

df2 = df1.select("*","likeDislike.*").drop("likeDislike")

# COMMAND ----------

df3 = df2.select("*").withColumn("multiMedia",explode_outer("multiMedia")).select("*","multiMedia.*").drop("multiMedia")

# COMMAND ----------

display(df3)

# COMMAND ----------

df7=df3.na.fill("replaced_val_image_path",["imagePaths"])

# COMMAND ----------

df8 = df3.withColumn("createAt", to_date(col("createAt")))

# COMMAND ----------

df9=df3.filter(col("commentCount")>0).count()

# COMMAND ----------

print(df9)

# COMMAND ----------




# COMMAND ----------

display(df8)

# COMMAND ----------

display(df7)

# COMMAND ----------

df7.printSchema()

# COMMAND ----------

display(df5)

# COMMAND ----------


