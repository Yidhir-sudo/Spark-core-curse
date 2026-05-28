# Spark Core Curse (PySpark)

This repository provides a set of exercises and exams to learn Apache Spark
with Python (PySpark). It is used as course material for engineering classes on
distributed data processing.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Execution](#execution)
- [Sessions](#sessions)
- [Project Structure](#project-structure)

## Features

- **Apache Spark Core**: distributed data processing with RDD and DataFrame.
- **PySpark**: Apache Spark Python API.
- **Learning progression**: 7 sessions from fundamentals to storage optimization.
- **Exam material**: exam prompt and solution are provided in the `exams/` folder.

## Prerequisites

Install the following on your machine:

- **Python 3.8+ (ideally 3.14.4)**
- **Java 17** (required by Spark)
- **Apache Spark 4.x+ (ideally 4.1.1)** (recommended)
- **pip** to install PySpark

## Installation

### 1. Clone the repository

```bash
git clone <repository-url>
cd Spark-core-curse
```

### 2. Automatic installation (recommended)

A script is provided to install Java, Python, Apache Spark, Scala/sbt,
VS Code and its extensions, plus a Python virtual environment with PySpark,
in a single command. Compatible with macOS (Homebrew) and
Debian/Ubuntu Linux (apt).

```bash
bash setup_spark_env.sh
```

Available options:

```bash
bash setup_spark_env.sh --no-scala     # skip Scala/sbt
bash setup_spark_env.sh --no-vscode    # skip VS Code
bash setup_spark_env.sh --help
```

At the end, restart your terminal and activate the environment:

```bash
source .venv/bin/activate
```

### 3. Manual installation (alternative)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install pyspark
```

## Execution

To run a local Python script:

```bash
source .venv/bin/activate
python sessions/Session1.py
```

If your shell does not resolve `python`, use the project interpreter directly:

```bash
.venv/bin/python sessions/Session1.py
```

To submit a script to a Spark cluster:

```bash
spark-submit --master <master-url> sessions/Session1.py
```

Replace `<master-url>` with your cluster URL (for example `local[*]`
for local mode using all available CPU cores).

## Sessions

Each session has two files in the `sessions/` folder:

- a Markdown file (`SessionN.md`) describing the goals and exercises;
- a Python file (`SessionN.py`) containing the reference solution.

| # | Topic | Exercises | Solution |
|---|---|---|---|
| 1 | Spark and RDD introduction | [Session1.md](sessions/Session1.md) | [Session1.py](sessions/Session1.py) |
| 2 | Lazy evaluation, `map` vs `flatMap` | [Session2.md](sessions/Session2.md) | [Session2.py](sessions/Session2.py) |
| 3 | Pair RDDs: `reduceByKey`, `groupByKey`, ranking | [Session3.md](sessions/Session3.md) | [Session3.py](sessions/Session3.py) |
| 4 | DataFrames, Spark SQL, predicate pushdown | [Session4.md](sessions/Session4.md) | [Session4.py](sessions/Session4.py) |
| 5 | Joins, broadcast and data skew | [Session5.md](sessions/Session5.md) | [Session5.py](sessions/Session5.py) |
| 6 | Window functions | [Session6.md](sessions/Session6.md) | [Session6.py](sessions/Session6.py) |
| 7 | Storage, formats and partitioning | [Session7.md](sessions/Session7.md) | [Session7.py](sessions/Session7.py) |

The `exams/` folder contains an exam prompt and its correction:

- [PySpark_Exam_Prompt.py](exams/PySpark_Exam_Prompt.py)
- [PySpark_Exam_Correction.py](exams/PySpark_Exam_Correction.py)

## Project Structure

```bash
Spark-core-curse/
├── README.md                         # This file
├── sessions/                         # Practice sessions (exercises + solutions)
│   ├── Session1.md / Session1.py     # Spark and RDD introduction
│   ├── Session2.md / Session2.py     # Lazy evaluation, map vs flatMap
│   ├── Session3.md / Session3.py     # Pair RDDs and key/value transformations
│   ├── Session4.md / Session4.py     # DataFrames, Spark SQL, predicate pushdown
│   ├── Session5.md / Session5.py     # Joins, broadcast and data skew
│   ├── Session6.md / Session6.py     # Window functions
│   └── Session7.md / Session7.py     # Storage, formats and partitioning
├── exams/                            # Exam prompt and correction
│   ├── PySpark_Exam_Prompt.py
│   └── PySpark_Exam_Correction.py
└── output/                           # Generated outputs from Session 7 (CSV, Parquet, partitioned)
```