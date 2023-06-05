#Initializing sparksession
from pyspark.sql import SparkSession

spark =SparkSession.builder.getOrCreate()

#Read csv file

df=spark.read.option("header","True").option("inferSchema","True").csv("C:/Users/shubhampradipraobhad/Downloads/annual-enterprise-survey-2021-financial-year-provisional-csv.csv")
#df.show()

#count number of columns
print(df.count())

#drop column
#df.drop("Industry_name_NZSIOC").show()

#add column
from pyspark.sql.functions import *
df2=df.withColumn("city_name",lit("california"))

#Renamed column name
df3=df2.withColumnRenamed("city_name", 'city')

#Select column name
df3.select("Units","Variable_name")

#Print all column name
print(df.columns)

#Fill null value
df3.na.fill("shubham").show()

#Creating RDD
dataset=[("shubham",2000),("pratap",8000),("satish",3000)]
rdd=spark.sparkContext.parallelize(dataset)
print(rdd.collect())
print(type(rdd))

#Conver RDD to Dataframe
rdd1=rdd.toDF()

#Check data schema
rdd1.printSchema()

List=[("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
        ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
        ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"), ("Beans", 2000, "Mexico"),
        ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada")]

df_data=spark.createDataFrame(data=List,schema=("fruit","price","country"))

#d2=df_data.groupby("country")

#df_data.show()

#df_data.select("fruit","country").show()

#df_data.show()

df_data.withColumn("city",lit("Delhi")).show()

df_data.show()

ef = df_data.withColumn("city", when(df_data["country"]=="USA", "Delhi").when(df_data["country"]=="China", "Wuhan").when(df_data["country"] =="Canada","mumbai").otherwise(df_data["price"]))
ef.show()









