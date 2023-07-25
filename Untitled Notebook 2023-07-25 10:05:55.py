# Databricks notebook source
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

emp_data=[(1,"nakul",2345,"M",5000),
          (2,"pratap",1234,"F",7500),
          (3,"harshal",6789,"M",12500),
          (4,"Jones",2005,"F",2000),
          (5,"Brown",2010,"",-1),
          (6,"Brown",2010,"",-1)]

column_name=("id","name","batch_id","gender","salary")

df=spark.createDataFrame(emp_data,column_name)

# COMMAND ----------

deptData = [("Finance",10,"2018"),
    ("Marketing",20,"2010"),
    ("Marketing",20,"2018"),
    ("Sales",30,"2005"),
    ("Sales",30,"2010"),
    ("IT",50,"2010")
  ]

deptColumns = ["dept_name","dept_id","branch_id"]

df2=spark.createDataFrame(data=deptData, schema=deptColumns)

# COMMAND ----------

join_df=df.join(df2,df.batch_id==df2.dept_id, "fullouter")

# COMMAND ----------

display(join_df)

# COMMAND ----------

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["product","amount","country"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)

# COMMAND ----------

pivot_table=df.groupBy("product").pivot("country").sum("amount")

# COMMAND ----------

display(pivot_table)

# COMMAND ----------

nullfill_value=pivot_table.na.fill(value=12)

# COMMAND ----------

display(nullfill_value)

# COMMAND ----------

fill_anyvalue=pivot_table.na.fill(1000,["Mexico"])

# COMMAND ----------

display(fill_anyvalue)

# COMMAND ----------

fill_empty=pivot_table.na.fill( ,["Mexico"])

# COMMAND ----------

fill_empty.explain()

# COMMAND ----------

display(fill_empty)

# COMMAND ----------

display(fill_anyvalue)

# COMMAND ----------

df.groupBy("Product").pivot("country").min("Amount").show()

# COMMAND ----------

display(df)

# COMMAND ----------

data = [
    (1, "Male","2022-10-25"),
    (2, "Female","2021/12/17"),
    (3, "Female","18.11.2021")
]

df = spark.createDataFrame(data, schema=["id","gender","dob"]
df.show(truncate=False)


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,apply regexp operation to get proper format dob
from pyspark.sql.functions import col
df5=df.withColumn("correct_dob", regexp_replace(col("dob"),"[/,.]","-")).drop(col("dob"))

# COMMAND ----------

display(df5)

# COMMAND ----------

data = [("111",50000),("222",60000),("333",40000)]
columns= ["EmpId","Salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

# COMMAND ----------

df.withColumn("add_value",lit(1000)).show(truncate=False)

# COMMAND ----------


