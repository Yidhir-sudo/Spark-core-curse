# Seance 3: Introduction au DataFrames avec PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row

spark = SparkSession.builder \
   .appName("IntroductionSpark") \
   .master("local[*]") \
   .getOrCreate()

sc = spark.sparkContext

rdd = sc.parallelize(range(1, 21))

df = rdd\
        .map(lambda x: Row(x))\
        .toDF(["numbers"])\
        .withColumn("numbers", col("numbers")*col("numbers"))\
        .filter("numbers > 10")


spark.read.csv("path/to/your/csvfile.csv", header=True, inferSchema=True)

df.show()
