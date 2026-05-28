# Session 7 — Storage, formats and partitioning

> Solution: [Session7.py](Session7.py)

## Goals

- Compare write performance between **CSV** and **Parquet**.
- Observe the benefits of Parquet: **column pruning** and **predicate
  pushdown**.
- Apply **partitioning** to data on disk.
- Find a sensible number of output files (avoid the "too many small files"
  anti-pattern).

```bash
python sessions/Session7.py
```

## Synthetic dataset

Build a DataFrame with 1,000,000 rows containing:

- An auto-generated `id` column (`spark.range`).
- A `country` column distributed roughly as:
  - `FR` ~ 30 %
  - `UK` ~ 30 %
  - `DE` ~ 40 %
- A random `amount` column in `[0, 1000]`.

Tip: use `rand()`, `when` and `otherwise`.

A helper to measure the runtime of a callable:

```python
def time_execution(func, name):
    start = time.time()
    func()
    end = time.time()
    print(f"> {name}: {end - start:.2f} seconds")
    return end - start
```

## Exercises

### Exercise 1 — CSV vs Parquet

1. Write the DataFrame to CSV in `output/data_csv/`.
2. Write the DataFrame to Parquet in `output/data_parquet/`.
3. Measure each duration and compare them.

### Exercise 2 — Column pruning

1. Read the Parquet output into `df_parquet`.
2. Select only the `amount` column and call `explain(True)`.
3. Observe **column pruning** in the plan.

### Exercise 3 — Predicate pushdown

1. From `df_parquet`, filter `country = 'FR'`.
2. Call `explain(True)` and locate the **PushedFilters** in the plan.

### Exercise 4 — Partitioning on disk

1. Write the DataFrame to Parquet partitioned by `country` under
   `output/data_partitioned/`.
2. Verify the resulting directory layout (`country=FR`, `country=UK`,
   `country=DE`).

### Exercise 5 — Too many small files

1. Repartition the DataFrame into `200` partitions and write the result to
   `output/too_many_files/`.
2. Repartition into `4` partitions and write the result to
   `output/better_partitioning/`.
3. Compare the durations and the number of files produced.

### Bonus — Optimal number of partitions

Write a function `calculate_optimal_partitions(df, target_file_size_mb=128)`
that estimates the optimal number of partitions so that each file is close to
the target size.

---

← [Session 6](Session6.md) · [Back to README](../README.md)
