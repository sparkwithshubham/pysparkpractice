from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.getOrCreate()

data = {"name": ["A", "B", "C", "D", "E"],
        "score": [1, 2, 3, 4, 5]}
df = spark.createDataFrame(data).show()

