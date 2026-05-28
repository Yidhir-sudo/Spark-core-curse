# Session 5 — Joins, broadcast and data skew

> Solution: [Session5.py](Session5.py)

## Goals

- Perform a join between two DataFrames.
- Understand **broadcast join** and when to use it.
- Identify **data skew**.
- Apply `repartition` to mitigate skew.
- Read the execution plans (`explain(True)`).

```bash
python sessions/Session5.py
```

## Shared datasets

### Users

```python
users_data = [
    (1, "FR"),
    (2, "FR"),
    (3, "UK"),
    (4, "DE"),
    (5, "ES"),
]
```

Schema: `user_id (int)`, `country (str)`.

### Transactions

A small skewed transactions dataset:

- 50 transactions of amount `100` for `user_id = 1`.
- 5 transactions for each other `user_id` (amounts: `200`, `150`, `50`, `300`).

Schema: `user_id (int)`, `amount (int)`.

## Exercises

### Exercise 1 — Standard join

1. Inner join `transactions` and `users` on `user_id`.
2. Group by `country` and sum the `amount` under an alias `total`.
3. Display the result and inspect the plan with `explain(True)`.
4. Identify the join strategy used by Spark.

### Exercise 2 — Broadcast join

1. Repeat Exercise 1 by broadcasting the **smallest** DataFrame
   (`broadcast(users)`).
2. Display the result and inspect the plan.
3. Compare with Exercise 1: what changed in the plan?

### Exercise 3 — Detecting data skew

1. Group `transactions` by `user_id` and count the rows.
2. Display the result and confirm that `user_id = 1` is dominant.

### Exercise 4 — Mitigating skew with `repartition`

1. Repartition `transactions` by `user_id`.
2. Join the result with `users`.
3. Display the result and inspect the plan.

---

← [Session 4](Session4.md) · [Back to README](../README.md) · Next: [Session 6 →](Session6.md)
