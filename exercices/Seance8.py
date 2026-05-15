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

# Fonction pour mesurer le temps d'exécution
def time_execution(func, name):
    start = time.time()
    func()
    end = time.time()
    print(f"{name}: {end - start:.2f} secondes")
    return end - start

# Exercice 1
time_csv = time_execution(
    lambda: df.write.mode("overwrite").csv("data_csv/"),
    "Écriture CSV"
)

time_parquet = time_execution(
    lambda: df.write.mode("overwrite").parquet("data_parquet/"),
    "Écriture Parquet"
)

print(f"\nComparaison: Parquet ({time_parquet:.2f} secondes) est {time_csv / time_parquet:.1f}x plus rapide que CSV ({time_csv:.2f} secondes)")

# Exercice 2
df_parquet = spark.read.parquet("data_parquet/")
df_parquet.select("amount").explain(True)

# Exercice 3
df_parquet.filter("country = 'FR'").explain(True)

# Exercice 4
df.write \
    .mode("overwrite") \
    .partitionBy("country") \
    .parquet("data_partitioned/")

# Exercice 5
# Mauvaise partition
time_execution(
    lambda: df.repartition(200).write.mode("overwrite").parquet("too_many_files/"),
    "Lecture partitionnée (mauvaise partition)"
)

# Correction 
time_execution(
    lambda: df.repartition(4).write.mode("overwrite").parquet("better_partitioning/"),
    "Lecture partitionnée (meilleure partition)"
)

def calculate_optimal_partitions(df, target_file_size_mb=128):
    total_size_bytes = df.rdd.map(lambda row: len(str(row))).sum()
    total_size_mb = total_size_bytes / (1024 * 1024)
    optimal_partitions = max(1, int(total_size_mb / target_file_size_mb))
    return optimal_partitions