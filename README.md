# ESGI - Spark Core Template (PySpark)

Ce dépôt fournit un ensemble d'exercices et d'examens pour apprendre Apache Spark
avec Python (PySpark). Il sert de support pédagogique au cours ESGI sur le
traitement de données distribué.

## Table des matières

- [Fonctionnalités](#fonctionnalités)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Exécution](#exécution)
- [Structure du projet](#structure-du-projet)

## Fonctionnalités

- **Apache Spark Core** : traitement de données distribué avec RDD et DataFrame.
- **PySpark** : API Python d'Apache Spark.
- **Progression pédagogique** : 8 séances couvrant les bases jusqu'aux cas pratiques.
- **Examen** : énoncé et correction fournis dans le dossier `exams/`.

## Prérequis

À installer sur votre machine :

- **Python 3.8+**
- **Java 8 ou 11** (requis pour Spark)
- **Apache Spark 3.x** (recommandé)
- **pip** pour installer PySpark

## Installation

### 1. Cloner le dépôt

```bash
git clone https://github.com/Yidhir-sudo/esgi-spark-core-template.git
cd esgi-spark-core-template
```

### 2. Créer un environnement virtuel (recommandé)

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Installer PySpark

```bash
pip install pyspark
```

## Exécution

Pour exécuter un .py localement :

```bash
python exercices/Seance1.py
```

Pour soumettre un script à un cluster Spark :

```bash
spark-submit --master <master-url> exercices/Seance1.py
```

Remplacez `<master-url>` par l'URL de votre cluster (par exemple `local[*]`
pour un mode local utilisant tous les cœurs disponibles).

## Structure du projet

```bash
esgi-spark-core-template/
├── README.md                         # Ce fichier
├── exercices/                        # Séances de TP
│   ├── Seance1.py                    # Introduction à Spark et aux RDD
│   ├── Seance2.py                    # Manipulation de données (RDD)
│   ├── Seance3.py
│   ├── Seance4.py
│   ├── Seance5.py
│   ├── Seance6.py
│   ├── Seance7.py
│   └── Seance8.py
└── exams/                            # Examens
    ├── Examen_PySpark_Enonce.py
    └── Examen_PySpark_Correction.py
```
