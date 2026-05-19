from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

spark = SparkSession.builder \
.appName("DataFrameCourse5") \
.master("local[*]") \
.getOrCreate()

# Introduction to DataFrames with PySpark
sc = spark.sparkContext
rdd = sc.parallelize(range(1, 21))

## Convert RDD to DataFrame, perform transformations and actions
df = rdd\
        .map(lambda x: Row(x))\
        .toDF(["numbers"])\
        .withColumn("numbers", col("numbers")*col("numbers"))\
        .filter("numbers > 10")

## Show the resulting DataFrame
df.show()

## Write a DataFrame to CSV format
df.write.csv("output/session5/csvfile.csv", header=True, mode="overwrite")

## Read CSV file into DataFrame
spark.read.csv("output/session5/csvfile.csv", header=True, inferSchema=True)

# Exercise 1
data = [
    (1, "Alice", "FR", 100),
    (2, "Bob", "FR", 200),
    (3, "Charlie", "UK", 150),
    (4, "David", "FR", 50),
    (5, "Eve", "UK", 300)
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("country", StringType(), False),
    StructField("amount", IntegerType(), False)
])

df = spark.createDataFrame(data, schema)

df.printSchema()
df.show()

# Exercise 2

df2 = df\
    .select("name", "amount")\
    .filter(df.amount > 100)\
    .orderBy(df.amount.desc())

df2.show()
df2.explain(True)

# Exercise 3

from pyspark.sql.functions import sum

agg_df = df.groupBy("country").agg(sum("amount").alias("total"))

agg_df.show()
agg_df.explain(True)

# Exercise 4
# Compare the execution plans of the two approaches - predicate pushdown 
df_filtered = df.filter(df.amount > 100)
agg = df_filtered.groupBy("country").agg(sum("amount"))

agg.explain(True)

# Exercise 5

df.createOrReplaceTempView("transactions")

sql_df = spark.sql("""
SELECT country, SUM(amount) as total
FROM transactions
GROUP BY country
""")

sql_df.show()
sql_df.explain(True)