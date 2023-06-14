from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()



data1 = [("alpha", {"morning" : "valo", "hi":"hello"}), ("beta", {"afternoon": "csgo"}), ("gama", {"evening": "ark"}),
         ("delta", {"night": "genshin"})]
sch = StructType([StructField("name", StringType()),
                  StructField("game", MapType(StringType(), StringType()))])

df = spark.createDataFrame(data=data1, schema=sch)

df_rdd=df_data=df.withColumn("time",map_keys(df["game"])[0]).withColumn("game_2",map_values(df["game"])[0]).withColumn("time",map_keys(df["game"])).withColumn("game_2",map_values(df["game"])).show()
df_rdd.drop("game").show()

