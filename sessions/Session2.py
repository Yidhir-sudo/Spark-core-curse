# Session 2: Data manipulation with PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
   .appName("IntroductionSpark") \
   .master("local[*]") \
   .getOrCreate()

sc = spark.sparkContext

## 1 - Understanding lazy behavior
rdd = sc.parallelize(range(1, 21))
rdd_transfo = (
    rdd
    .map(lambda x: x * 2)
    .filter(lambda x: x > 10)
)

# No execution here
 
## 2 - Actions to trigger execution
print("Count :", rdd_transfo.count())
print("Collect :", rdd_transfo.collect())

## 3 - Difference between map and flatMap
text = "Spark makes big data processing fast"
rdd_text = sc.parallelize([text])
map_result = rdd_text.map(lambda line: line.split(" "))
flatmap_result = rdd_text.flatMap(lambda line: line.split(" "))
print("map :", map_result.collect())
print("flatMap :", flatmap_result.collect())

## 4 - Practical example: Word Count
text = "Spark makes Spark fast and Spark scalable"
rdd = sc.parallelize(text.split(" "))
cleaned = rdd.map(lambda w: w.lower())
wordcount = (
   cleaned
   .map(lambda w: (w, 1))
   .reduceByKey(lambda a, b: a + b)
)
print("WordCount :", wordcount.collect())
print("Total mots :", cleaned.count())
print("Mots distincts :", cleaned.distinct().count())