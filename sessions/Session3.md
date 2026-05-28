# Session 3 — Transformations and actions on key/value RDDs

> Solution: [Session3.py](Session3.py)

## Goals

- Manipulate **pair RDDs** (key/value).
- Use `reduceByKey`, `groupByKey`, `mapValues`, `sortByKey`, `sortBy`.
- Compare execution plans using `toDebugString()`.
- Understand why `reduceByKey` is preferable to `groupByKey`.

```bash
python sessions/Session3.py
```

## Shared dataset

```python
data = [
    ("Alice", 100),
    ("Bob", 50),
    ("Alice", 25),
    ("Bob", 75),
    ("Charlie", 200),
]
```

## Exercises

### Exercise 1 — Sum per key

From `data`:

1. Build an RDD with `sc.parallelize`.
2. Compute the **total amount per name** with `reduceByKey`.
3. Sort the result alphabetically with `sortByKey` and display it.

### Exercise 2 — `groupByKey` vs `reduceByKey`

Always from `data`:

1. Compute the sum per key via `groupByKey` + `mapValues(sum)`.
2. Compute the same sum via `reduceByKey`.
3. Display both results, then print the execution plan with `toDebugString()`
   for each variant.
4. Conclude: which approach is the most efficient and why?

### Exercise 3 — Average per key

Compute the **average amount per name**:

1. Transform each value into `(amount, 1)` with `mapValues`.
2. Aggregate `(sum, count)` per key with `reduceByKey`.
3. Derive the average with `mapValues`.
4. Display the result.

### Exercise 4 — Log analysis

Given the log dataset:

```python
logs = [
    ("ERROR", 1),
    ("INFO", 1),
    ("ERROR", 1),
    ("WARNING", 1),
    ("INFO", 1),
    ("ERROR", 1),
]
```

1. Compute the count of each log level with `reduceByKey`.
2. Identify the **most frequent level** using `sortBy` then `first`.
3. Display the result.

---

← [Session 2](Session2.md) · [Back to README](../README.md) · Next: [Session 4 →](Session4.md)
