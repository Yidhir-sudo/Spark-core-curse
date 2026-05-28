# Session 6 — Window functions

> Solution: [Session6.py](Session6.py)

## Goals

- Define a `Window` with `partitionBy` and `orderBy`.
- Use `row_number`, cumulative `sum` and ranking.
- Deduplicate a dataset using a window.
- Chain several steps (filter, cumulative sum, ranking) in a single pipeline.

```bash
python sessions/Session6.py
```

## Shared dataset

```python
data = [
    (1, "FR", 100, "2025-01-01"),
    (1, "FR", 200, "2025-01-02"),
    (1, "FR", 150, "2025-01-03"),
    (2, "FR", 300, "2025-01-01"),
    (2, "FR", 100, "2025-01-05"),
    (3, "UK", 250, "2025-01-02"),
    (3, "UK",  50, "2025-01-03"),
    (4, "DE", 400, "2025-01-01"),
    (4, "DE", 100, "2025-01-02"),
]
```

Schema: `user_id (int)`, `country (str)`, `amount (int)`, `date (str)`.

## Exercises

### Exercise 1 — Top client per country

1. Define a window partitioned by `country` and ordered by `amount`
   descending.
2. Add a column `rn = row_number()` over this window.
3. Keep only the rows where `rn == 1`.
4. Display the result.

### Exercise 2 — Cumulative sum per user

1. Define a window partitioned by `user_id`, ordered by `date` and bounded
   between `unboundedPreceding` and `currentRow`.
2. Add a column `cumulative_amount` containing the cumulative sum of `amount`.
3. Display the result.

### Exercise 3 — Deduplication

1. Add the row `(1, "FR", 999, "2025-02-01")` to the dataset using `union`.
2. Define a window partitioned by `user_id` and ordered by `date` descending.
3. Keep only the **most recent** row for each user (drop the helper
   column).
4. Display the result.

### Exercise 4 — Combined pipeline

Chain the following steps:

1. **Filter** rows with `amount > 100`.
2. **Enrich** the data with a cumulative sum per user ordered by `date`.
3. **Rank** by country (descending `amount`) using `row_number`.
4. **Keep** only the top rank for each country.
5. Display the final result.

---

← [Session 5](Session5.md) · [Back to README](../README.md) · Next: [Session 7 →](Session7.md)
