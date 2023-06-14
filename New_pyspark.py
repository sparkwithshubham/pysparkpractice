from pyspark.sql import SparkSesssion
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()

df=spark.read.