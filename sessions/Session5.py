from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import sum, broadcast

spark = SparkSession.builder \
.appName("DataFrameCourse6") \
.master("local[*]") \
.getOrCreate()

# Create DataFrames
users_data = [
    (1, "FR"),
    (2, "FR"),
    (3, "UK"),
    (4, "DE"),
    (5, "ES")
]

users_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("country", StringType(), False)
])

users = spark.createDataFrame(users_data, users_schema)

# Create transactions DataFrame with 50 transactions for user_id 1, and 5 transactions for each of the other users
transactions_data = [(1, 100)] * 50 + \
[(2, 200)] * 5 + \
[(3, 150)] * 5 + \
[(4, 50)] * 5 + \
[(5, 300)] * 5

transactions_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("amount", IntegerType(), False)
])

transactions = spark.createDataFrame(transactions_data, transactions_schema)

# Exercise 1
joined = transactions.join(users, on = "user_id", how = "inner")
agg = joined.groupBy("country").agg(sum("amount").alias("total"))

agg.show()
agg.explain(True)

# Exercise 2
joined_broadcast = transactions.join(
    broadcast(users),
    "user_id"
)

agg_broadcast = joined_broadcast.groupBy("country") \
    .agg(sum("amount").alias("total"))

agg_broadcast.show()
agg_broadcast.explain(True)

# Exercise 3
transactions.groupBy("user_id").count().show() # user_id == 1 dominates

# Exercise 4
transactions_repart = transactions.repartition("user_id")
joined_repart = transactions_repart.join(users, "user_id")

joined_repart.show()
joined_repart.explain(True)