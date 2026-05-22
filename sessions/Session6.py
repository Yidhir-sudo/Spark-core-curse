from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("WindowsCourse7") \
    .master("local[*]") \
    .getOrCreate()

data = [
    (1, "FR", 100, "2025-01-01"),
    (1, "FR", 200, "2025-01-02"),
    (1, "FR", 150, "2025-01-03"),
    (2, "FR", 300, "2025-01-01"),
    (2, "FR", 100, "2025-01-05"),
    (3, "UK", 250, "2025-01-02"),
    (3, "UK", 50, "2025-01-03"),
    (4, "DE", 400, "2025-01-01"),
    (4, "DE", 100, "2025-01-02")
]

schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("country", StringType(), False),
    StructField("amount", IntegerType(), False),
    StructField("date", StringType(), False)
])

df = spark.createDataFrame(data, schema)

# Exercise 1
window_spec = Window \
    .partitionBy("country") \
    .orderBy(col("amount").desc())

df_ranked = df.withColumn(
    "rn",
    row_number().over(window_spec)
)

top_clients = df_ranked.filter(col("rn") == 1)
top_clients.show()

# Exercise 2
window_cum = Window \
    .partitionBy("user_id") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_cum = df.withColumn(
    "cumulative_amount",
    sum("amount").over(window_cum)
)

df_cum.show()

# Exercise 3
df_dup = df.union(
    spark.createDataFrame(
        [(1, "FR", 999, "2025-02-01")],
        schema
    )
)

window_dedup = Window \
    .partitionBy("user_id") \
    .orderBy(col("date").desc())

df_clean = df_dup.withColumn(
    "rn",
    row_number().over(window_dedup)
).filter(col("rn") == 1).drop("rn")

df_clean.show()

# Exercise 4
## 1. Filtering
df_filtered = df.filter(col("amount") > 100)

## 2. Cumulative sum
window_cum = Window \
    .partitionBy("user_id") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_enriched = df_filtered.withColumn(
    "cumulative_amount",
    sum("amount").over(window_cum)
)

## 3. Country ranking
window_rank = Window \
    .partitionBy("country") \
    .orderBy(col("amount").desc())

df_ranked = df_enriched.withColumn(
    "rank",
    row_number().over(window_rank)
)

## 4. Final result
final_df = df_ranked.filter(col("rank") == 1)
final_df.show()