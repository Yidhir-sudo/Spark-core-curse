# Session 2 — Data manipulation with PySpark

> Solution: [Session2.py](Session2.py)

## Goals

- Understand **lazy evaluation** in Spark.
- Differentiate **transformations** from **actions**.
- Master the difference between `map` and `flatMap`.
- Reproduce a Word Count and derive simple metrics.

```bash
python sessions/Session2.py
```

## Exercises

### Exercise 1 — Lazy evaluation

From an RDD of integers from 1 to 20:

1. Multiply each element by 2.
2. Keep only the values strictly greater than 10.
3. Verify that **no computation** has been triggered yet (no Spark job in the
   logs).

### Exercise 2 — Trigger execution with actions

Reuse the RDD from Exercise 1 and trigger execution with two actions:

1. `count()` to obtain the number of elements.
2. `collect()` to retrieve all elements as a Python list.

### Exercise 3 — `map` vs `flatMap`

Given the sentence:

```text
Spark makes big data processing fast
```

1. Build an RDD that contains this sentence as a single string.
2. Apply `map(lambda line: line.split(" "))` and observe the result.
3. Apply `flatMap(lambda line: line.split(" "))` and explain the difference.

### Exercise 4 — Word Count and metrics

Given the sentence:

```text
Spark makes Spark fast and Spark scalable
```

1. Build an RDD of words and normalize them to lowercase.
2. Compute the count for each word with `reduceByKey`.
3. Display:
   - the `(word, count)` pairs,
   - the total number of words,
   - the number of **distinct** words.

---

← [Session 1](Session1.md) · [Back to README](../README.md) · Next: [Session 3 →](Session3.md)
