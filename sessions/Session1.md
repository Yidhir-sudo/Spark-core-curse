# Session 1 — Introduction to Spark

> Solution: [Session1.py](Session1.py)

## Goals

- Install and configure PySpark.
- Understand the role of `SparkSession` and `SparkContext`.
- Discover the core abstraction: the **RDD** (Resilient Distributed Dataset).
- Run your first transformations and actions.

## Prerequisites

- Python 3.8+ with the project virtual environment activated.
- PySpark 4.x installed (`pip install pyspark`).

```bash
source .venv/bin/activate
python sessions/Session1.py
```

## Exercises

### Exercise 1 — Create a Spark session

Create a local `SparkSession` named `IntroductionSpark` using all available
cores (`local[*]`), then print the Spark version available via the
`SparkContext`.

### Exercise 2 — Build your first RDD

From the integers from 1 to 10 (inclusive):

1. Build an RDD with `sc.parallelize`.
2. Collect and print every element.
3. Print the number of elements.
4. Compute the sum of the elements with `reduce`.

### Exercise 3 — Transformations on an RDD

Starting from the previous RDD:

1. Build a new RDD containing the **square** of each value.
2. Build a new RDD containing only the **even** values.
3. Collect and display both results.

### Exercise 4 — Word Count

Given the sentence:

```text
Spark is fast Spark is scalable Spark is powerful
```

1. Split the text into words and create an RDD.
2. Normalize the words to lowercase.
3. Produce `(word, count)` pairs.
4. Aggregate the counts with `reduceByKey`.
5. Display the result.

### Exercise 5 — Clean shutdown

Stop the `SparkSession` properly at the end of the script.

---

← [Back to README](../README.md) · Next: [Session 2 →](Session2.md)
