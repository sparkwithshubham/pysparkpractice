from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.getOrCreate()

# # schema = StructType([
#     StructField("EmployeeID", IntegerType(), True),
#     StructField("EmployeeName", StringType(), True),
#     StructField("Address", StringType(), True),
#     StructField("_corrupt_record", StringType(), True)
# ])

# handle bad record data
df=spark.read.option("mode","PERMISSIVE").option("header","true").option("inferSchema","true").csv("C:/Users/shubhampradipraobhad/Documents/employee_csv.txt")

df=spark.read.option("mode","FAILFAST").option("header","true").option("inferSchema","true").csv("C:/Users/shubhampradipraobhad/Documents/employee_csv.txt")

df=spark.read.option("mode","DROPMALFORMED").option("header","true").option("inferSchema","true").csv("C:/Users/shubhampradipraobhad/Documents/employee_csv.txt")

df.printSchema()

df.count()

# filter and drop corrupt record column which has null record
# df.where(col("corrupt_record").isNull()).drop("corrupt_record")

#By using select statement record fetch which has not null

#df.select(col("corrupt_record")).where("currupt_record").isNotNull()

#rdd_data=SparkContext.textFile("C:/Users/shubhampradipraobhad/Documents/employee_csv.txt")

#rdd.data.count()

#drop column if we apply join on same column
#rdd=df1.join(df2,df1.id==df2(id)).drop(df1.id)

df = spark.createDataFrame(
    [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
    ("id", "an_array", "a_map")
)
df.select("id", "an_array", posexplode_outer("a_map")).show()



# df_data=df.select("employee_id",explode("employee_id")).show()

data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)
rdd.collect()

rdd.count()

rdd.flatMap(lambda x:x.split(' '))

simpleData = (("James", "Sales", 3000),\
              ("Michael", "Sales", 4600),\
              ("Robert", "Sales", 4100),\
              ("Maria", "Finance", 3000),\
              ("James", "Sales", 3000),\
              ("Scott", "Finance", 3300),\
              ("Jen", "Finance", 3900),\
              ("Jeff", "Marketing", 3000),\
              ("Kumar", "Marketing", 2000),\
              ("Saif", "Sales", 4100)\
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)

df.printSchema()

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec=Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)).show(truncate=False)

df.withColumn("dense_rank", dense_rank().over(windowSpec)).show(truncate=False)

df.withColumn("rank",rank().over(partitionBy("department"),orderBy("salary")))











