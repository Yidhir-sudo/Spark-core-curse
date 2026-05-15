# Seance 2: Manipulation de données avec PySpark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
   .appName("IntroductionSpark") \
   .master("local[*]") \
   .getOrCreate()

sc = spark.sparkContext

## 1 - Comprendre le lazy behavior
rdd = sc.parallelize(range(1, 21))
rdd_transfo = (
    rdd
    .map(lambda x: x * 2)
    .filter(lambda x: x > 10)
)

# Aucune exécution ici
 
## 2 - Actions pour déclencher l'exécution 
print("Count :", rdd_transfo.count())
print("Collect :", rdd_transfo.collect())

## 3 - Différence entre map et flatMap
text = "Spark makes big data processing fast"
rdd_text = sc.parallelize([text])
map_result = rdd_text.map(lambda line: line.split(" "))
flatmap_result = rdd_text.flatMap(lambda line: line.split(" "))
print("map :", map_result.collect())
print("flatMap :", flatmap_result.collect())

## 4 - Exemple pratique: Word Count
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