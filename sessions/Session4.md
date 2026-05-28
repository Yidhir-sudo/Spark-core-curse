# Session 4 — Introduction to DataFrames and Spark SQL

> Solution: [Session4.py](Session4.py)

## Goals

- Build a **DataFrame** from an RDD or from an explicit schema.
- Use the DataFrame API: `select`, `filter`, `orderBy`, `groupBy`, `agg`.
- Read the execution plan with `explain(True)`.
- Discover **predicate pushdown**.
- Run SQL on a DataFrame via `createOrReplaceTempView`.

```bash
python sessions/Session4.py
```

## Warm-up — From RDD to DataFrame

1. Build an RDD of integers from 1 to 20.
2. Turn it into a DataFrame with a single column `numbers`.
3. Replace `numbers` by its square.
4. Keep only the values strictly greater than 10.
5. Display the result, then write it to CSV under
   `output/session5/csvfile.csv` (mode `overwrite`, header included).
6. Read the CSV back with `inferSchema=True`.

## Shared dataset

```python
data = [
    (1, "Alice",   "FR", 100),
    (2, "Bob",     "FR", 200),
    (3, "Charlie", "UK", 150),
    (4, "David",   "FR",  50),
    (5, "Eve",     "UK", 300),
]
```

The schema must be explicit: `id (int)`, `name (str)`, `country (str)`,
`amount (int)`.

## Exercises

### Exercise 1 — Typed DataFrame

1. Build a `StructType` schema matching the data above.
2. Create the DataFrame with `spark.createDataFrame`.
3. Print the schema (`printSchema`) and display the content (`show`).

### Exercise 2 — Select, filter, sort

From the DataFrame:

1. Select the columns `name` and `amount`.
2. Keep the rows where `amount > 100`.
3. Sort by `amount` in descending order.
4. Display the result and inspect the plan with `explain(True)`.

### Exercise 3 — Aggregation per country

1. Group by `country`.
2. Compute the total `amount` per country, alias the column as `total`.
3. Display the result and inspect the plan.

### Exercise 4 — Predicate pushdown

1. Filter rows with `amount > 100`.
2. Group the result by `country` and sum the `amount`.
3. Display the execution plan and observe **where** the filter is applied.

### Exercise 5 — Spark SQL

1. Register the DataFrame as a temporary view named `transactions`.
2. Write the SQL query equivalent to Exercise 3:

   ```sql
   SELECT country, SUM(amount) AS total
   FROM transactions
   GROUP BY country
   ```

3. Display the result and inspect the plan. Compare with the DataFrame plan.

---

← [Session 3](Session3.md) · [Back to README](../README.md) · Next: [Session 5 →](Session5.md)
