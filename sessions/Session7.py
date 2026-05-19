from pyspark.sql import SparkSession
from pyspark.sql.functions import rand, when
import time

spark = SparkSession.builder \
.appName("Course7_Storage") \
.master("local[*]") \
.getOrCreate()

df = spark.range(0, 1_000_000) \
.withColumn("country",
    when(rand() > 0.7, "FR")
    .when(rand() > 0.4, "UK")
    .otherwise("DE")
) \
.withColumn("amount", (rand() * 1000).cast("int"))

# Function to measure execution time
def time_execution(func, name):
    start = time.time()
    func()
    end = time.time()
    print(f"> {name}: {end - start:.2f} secondes")
    return end - start

# Exercise 1
time_csv = time_execution(
    lambda: df.write.mode("overwrite").csv("output/data_csv/"),
    "Écriture CSV"
)

time_parquet = time_execution(
    lambda: df.write.mode("overwrite").parquet("output/data_parquet/"),
    "Écriture Parquet"
)

print(f"\n> Comparaison: Parquet ({time_parquet:.2f} secondes) est {time_csv / time_parquet:.1f}x plus rapide que CSV ({time_csv:.2f} secondes)")

# Exercise 2
df_parquet = spark.read.parquet("output/data_parquet/")
df_parquet.select("amount").explain(True)

# Exercise 3
df_parquet.filter("country = 'FR'").explain(True)

# Exercise 4
df.write \
    .mode("overwrite") \
    .partitionBy("country") \
    .parquet("output/data_partitioned/")

# Exercise 5
# Poor partitioning
time_execution(
    lambda: df.repartition(200).write.mode("overwrite").parquet("output/too_many_files/"),
    "Ecriture partitionnée (mauvaise partition)"
)

# Fix
time_execution(
    lambda: df.repartition(4).write.mode("overwrite").parquet("output/better_partitioning/"),
    "Ecriture partitionnée (meilleure partition)"
)

# Bonus: Compute the optimal number of partitions
def calculate_optimal_partitions(df, target_file_size_mb=128):
    total_size_bytes = df.rdd.map(lambda row: len(str(row))).sum()
    total_size_mb = total_size_bytes / (1024 * 1024)
    optimal_partitions = max(1, int(total_size_mb / target_file_size_mb))
    return optimal_partitions