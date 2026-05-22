# Session 4: Transformations and actions on RDDs

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Row

spark = SparkSession.builder \
   .appName("IntroductionSpark") \
   .master("local[*]") \
   .getOrCreate()

sc = spark.sparkContext


# Example dataset
data = [
    ("Alice", 100),
    ("Bob", 50),
    ("Alice", 25),
    ("Bob", 75),
    ("Charlie", 200)
]


# Exercise 1
rdd = sc.parallelize(data)

result = rdd.reduceByKey(lambda a, b: a + b)

print(result.sortByKey().collect())

# Exercise 2
grouped = rdd.groupByKey()
sum_group = grouped.mapValues(lambda values: sum(values))

print(sum_group.collect())

reduced = rdd.reduceByKey(lambda a, b: a + b)
print(reduced.collect())

print(sum_group.toDebugString())
print(reduced.toDebugString())

# Exercise 3
mapped = rdd.mapValues(lambda x: (x, 1))

reduced = mapped.reduceByKey(
lambda a, b: (a[0] + b[0], a[1] + b[1])
)

average = reduced.mapValues(lambda x: x[0] / x[1])

print(average.collect())

# Exercise 4
logs = [
    ("ERROR", 1),
    ("INFO", 1),
    ("ERROR", 1),
    ("WARNING", 1),
    ("INFO", 1),
    ("ERROR", 1)
]

rdd_logs = sc.parallelize(logs)
counts = rdd_logs.reduceByKey(lambda a, b: a + b)
print(counts.collect())

most_frequent = counts.sortBy(lambda x: x[1], ascending=False).first()

print("Plus fréquent :", most_frequent)