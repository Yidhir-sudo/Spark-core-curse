# Spark Core Curse (PySpark)

This repository provides a set of exercises and exams to learn Apache Spark
with Python (PySpark). It is used as course material for ESGI classes on
distributed data processing.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Execution](#execution)
- [Project Structure](#project-structure)

## Features

- **Apache Spark Core**: distributed data processing with RDD and DataFrame.
- **PySpark**: Apache Spark Python API.
- **Learning progression**: 8 sessions from fundamentals to practical exercises.
- **Exam material**: exam prompt and solution are provided in the `exams_en/` folder.

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

## Project Structure

```bash
Spark-core-curse/
├── README.md                         # This file
├── sessions/                         # Practice sessions
│   ├── Session1.py                   # Spark and RDD introduction
│   ├── Session2.py                   # Data manipulation (RDD)
│   ├── Session3.py                   # Advanced RDD transformations and actions
│   ├── Session4.py                   # DataFrame basics and schema handling
│   ├── Session5.py                   # DataFrame filtering, aggregation, and SQL
│   ├── Session6.py                   # Joins and execution plan optimization
│   ├── Session7.py                   # Window functions and analytical queries
│   └── Session8.py                   # End-to-end practice and final recap
└── exams_en/                         # Exams
    ├── PySpark_Exam_Prompt.py
    └── PySpark_Exam_Correction.py
```