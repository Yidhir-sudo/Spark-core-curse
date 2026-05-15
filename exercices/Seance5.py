from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
.appName("DataFrameCourse5") \
.master("local[*]") \
.getOrCreate()

# Exercice 1

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

# Exercice 2

df2 = (
df
    .select("name", "amount")
    .filter(df.amount > 100)
    .orderBy(df.amount.desc())
)

df2.show()
df2.explain(True)

# Exercice 3

from pyspark.sql.functions import sum

agg_df = df.groupBy("country").agg(sum("amount").alias("total"))

agg_df.show()
agg_df.explain(True)

# Exercice 4
# Compare the execution plans of the two approaches - predicate pushdown 
df_filtered = df.filter(df.amount > 100)
agg = df_filtered.groupBy("country").agg(sum("amount"))

agg.explain(True)

# Exercice 5

df.createOrReplaceTempView("transactions")

sql_df = spark.sql("""
SELECT country, SUM(amount) as total
FROM transactions
GROUP BY country
""")

sql_df.show()
sql_df.explain(True)