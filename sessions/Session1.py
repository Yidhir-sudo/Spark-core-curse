# Session 1: Introduction to Spark

## Installing and configuring Spark with Python (PySpark)
from pyspark.sql import SparkSession
spark = SparkSession.builder \
   .appName("IntroductionSpark") \
   .master("local[*]") \
   .getOrCreate()

sc = spark.sparkContext
print("----------------> "+sc.version)

## Core Spark concepts: RDDs
rdd = sc.parallelize(range(1, 11))
print("----------> ",rdd.collect())
print("Nombre d'éléments :", rdd.count())
print("Somme :", rdd.reduce(lambda a, b: a + b))

## RDD transformations and actions
rdd_carre = rdd.map(lambda x: x * x)
rdd_pairs = rdd.filter(lambda x: x % 2 == 0)
print("Carrés :", rdd_carre.collect())
print("Pairs :", rdd_pairs.collect())

## Practical example: Word Count
text = "Spark is fast Spark is scalable Spark is powerful"
rdd_text = sc.parallelize(text.split(" "))
wordcount = (
   rdd_text
   .map(lambda word: word.lower())
   .map(lambda word: (word, 1))
   .reduceByKey(lambda a, b: a + b)
)
print(wordcount.collect())

## Graceful Spark session shutdown
spark.stop()
