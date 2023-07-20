# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

spark=SparkSession.builder.master("local[1]").appName("spark_practice").getOrCreate()

# COMMAND ----------

csv_option={
"inferSchema":"true",
"header":"true",
"delimiter":","
}
path="dbfs:/FileStore/Product.csv"
def sample_csv(path,csv_option):
    df=spark.read.options(**csv_option).csv(path)
    return df

# COMMAND ----------

df5=sample_csv(path,csv_option)
display(df5)

# COMMAND ----------

df5.groupBy("brand_name").filter(col("unitprice") > 300).show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df5.groupBy("brand_name").agg(sum("unitprice").alias("sorted")).where(col("sorted") > 750).show(truncate=False)

# COMMAND ----------

df1=sample_csv(path,csv_option).show()

# COMMAND ----------

csv_option={
"inferSchema":"true",
"header":"true",
}
path="dbfs:/FileStore/Product.csv"
def sample_csv(path,csv_option):
    df=spark.read.options(**csv_option).csv(path)
    return df

# COMMAND ----------

df2=sample_csv(,path)

# COMMAND ----------

display(df)

# COMMAND ----------

# path="dbfs:/FileStore/Product.csv"
# inferSchema=True
# header=True
 

def sample(path,infer,head):
    df=spark.read.option('inferSchema',infer).option('header',head).csv(path)
    return df



# COMMAND ----------

df=sample('dbfs:/FileStore/Product.csv',True,True).show()

# COMMAND ----------

path="dbfs:/FileStore/Product.csv"
infer=True
head=True
delimiter=","


def sample(path,infer,head,delimiter):
    df=spark.read.option('inferSchema',infer).option('header',head).option("delimiter",delimiter).csv(path)
    return df

# COMMAND ----------

df1=sample(path,infer,head,delimiter)

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.select("product_id","product_name","brand_name","category","unitprice","stock").filter((df1.category == "Eye Makeup") &(df1.stock >= 50) & (df1.stock <= 100 )).show()

# COMMAND ----------

df1.createOrReplaceTempView("sample_table")

# COMMAND ----------

spark.sql(select * from sample_table)

# COMMAND ----------

# MAGIC %scala
# MAGIC val ds = spark.catalog.listDatabases
# MAGIC ds.show(false)

# COMMAND ----------

df1.withColumn(.where(col("stock") == 100).show()

# COMMAND ----------

df1.groupBy("category").filter(col("stock") > 50).show()

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.select("product_id","product_name","brand_name","category","unitprice","stock").show(truncate=False)

# COMMAND ----------

df.groupBy("brand_name").sum("unitprice").show(truncate=False)

# COMMAND ----------

display(df1)

# COMMAND ----------

df=spark.read.option("inferSchema",True).option("header", True).csv("dbfs:/FileStore/employee.csv")
display(df)

# COMMAND ----------

df.distinct().count()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df.withColumn("salary", lit(2000)).show(truncate=False)

# COMMAND ----------

df4=df.na.fill("shubham")

# COMMAND ----------



# COMMAND ----------

display(df4)

# COMMAND ----------

df.select("id","name").show(truncate=False)

# COMMAND ----------



simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

windowSpec  = Window.partitionBy("department").orderBy(desc("salary"))

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)

# COMMAND ----------

df.withColumn("dense rank", dense_rank().over(windowSpec))\
.show()

# COMMAND ----------


