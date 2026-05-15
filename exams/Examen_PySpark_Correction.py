"""
================================================================================
                   CORRECTION - EXAMEN PySpark - 4ème Année Ingénieur
================================================================================
Durée : 1h30
Barème : 20 points (2 points par question)

Cette correction contient les solutions détaillées avec explications.
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, sum, avg, count, broadcast, row_number, rank, dense_rank, lag, lead, coalesce, lit
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ExamenPySpark_Correction") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Données (identiques à l'énoncé)
users_data = [
    (1, "Alice", "Paris", "2023-01-15"),
    (2, "Bob", "Lyon", "2023-02-20"),
    (3, "Charlie", "Paris", "2023-01-10"),
    (4, "Diana", "Marseille", "2023-03-05"),
    (5, "Eve", "Lyon", "2023-02-28"),
    (6, "Frank", "Paris", "2022-12-01"),
    (7, "Grace", "Bordeaux", "2023-04-15"),
    (8, "Henry", "Lyon", "2023-05-01")
]

orders_data = [
    (1, 101, 1, 150.0, "2024-01-10"),
    (1, 102, 2, 200.0, "2024-01-15"),
    (1, 103, 1, 75.0, "2024-02-01"),
    (2, 104, 3, 300.0, "2024-01-20"),
    (2, 105, 2, 50.0, "2024-02-10"),
    (3, 106, 1, 500.0, "2024-01-05"),
    (3, 107, 3, 120.0, "2024-01-25"),
    (4, 108, 2, 80.0, "2024-02-15"),
    (5, 109, 1, 250.0, "2024-01-30"),
    (5, 110, 3, 175.0, "2024-02-20"),
    (6, 111, 2, 400.0, "2024-01-08"),
    (6, 112, 1, 220.0, "2024-02-05"),
    (1, 113, 3, 180.0, "2024-02-25"),
    (3, 114, 2, 90.0, "2024-03-01"),
]

products_data = [
    (1, "Laptop", "Electronics", 999.99),
    (2, "Headphones", "Electronics", 149.99),
    (3, "Book", "Media", 29.99)
]

print("=" * 80)
print("CORRECTION QUESTION 1 : RDD - Word Count Avancé")
print("=" * 80)
"""
Points clés évalués :
- Utilisation correcte de flatMap pour séparer les mots
- Conversion en minuscules avec lower()
- reduceByKey pour le comptage
- filter pour les occurrences > 2
- sortBy avec ordre décroissant
"""

texte = """
PySpark is a powerful tool for big data processing
Spark provides fast distributed computing
PySpark makes Spark accessible through Python
Big data needs powerful processing tools
Spark is fast and Spark is scalable
"""

# Solution
rdd_text = sc.parallelize(texte.strip().split("\n"))

word_count = (
    rdd_text
    .flatMap(lambda line: line.split())          # Séparer en mots
    .map(lambda word: word.lower())              # Convertir en minuscules
    .map(lambda word: (word, 1))                 # Paires (mot, 1)
    .reduceByKey(lambda a, b: a + b)             # Compter les occurrences
    .filter(lambda x: x[1] > 2)                  # Garder si count > 2
    .sortBy(lambda x: x[1], ascending=False)     # Trier par count décroissant
)

print("Résultats Question 1:")
print(word_count.collect())
# Résultat attendu: [('spark', 5), ('is', 4), ('pyspark', 2)...] - seuls les mots > 2 occurrences
print()

print("=" * 80)
print("CORRECTION QUESTION 2 : RDD - Calcul de Statistiques avec mapValues")
print("=" * 80)
"""
Points clés évalués :
- Utilisation de mapValues pour transformer les valeurs
- Pattern (somme, count) pour calculer la moyenne avec reduceByKey
- Pas d'utilisation de groupByKey (moins performant)
"""

ventes_data = [
    ("Alice", 100), ("Bob", 200), ("Alice", 150),
    ("Charlie", 300), ("Bob", 50), ("Alice", 200),
    ("Charlie", 100), ("Bob", 150), ("Charlie", 250)
]

# Solution
rdd_ventes = sc.parallelize(ventes_data)

stats = (
    rdd_ventes
    # Transformer chaque montant en tuple (montant, 1) pour pouvoir compter
    .mapValues(lambda x: (x, 1))
    # Réduire : additionner les montants et compter les ventes
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    # Calculer la moyenne et formater le résultat final
    .mapValues(lambda x: (x[0], x[1], x[0] / x[1]))
)

print("Résultat Question 2 - Stats par vendeur (total, count, moyenne):")
for vendeur, (total, nb_ventes, moyenne) in stats.collect():
    print(f"  {vendeur}: Total={total}, Ventes={nb_ventes}, Moyenne={moyenne:.2f}")
print()


print("=" * 80)
print("CORRECTION QUESTION 3 : DataFrame - Création avec Schema")
print("=" * 80)
"""
Points clés évalués :
- Définition correcte du schema avec StructType et StructField
- Types de données appropriés (IntegerType, StringType, DoubleType)
- Nullable correctement défini
"""

# Solution - Schema users
users_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("city", StringType(), False),
    StructField("registration_date", StringType(), False)
])

users_df = spark.createDataFrame(users_data, users_schema)

# Schema orders
orders_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("order_date", StringType(), False)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)

print("Schema Users:")
users_df.printSchema()
print("\nSchema Orders:")
orders_df.printSchema()
print()


print("=" * 80)
print("CORRECTION QUESTION 4 : DataFrame - Agrégations Complexes")
print("=" * 80)
"""
Points clés évalués :
- Jointure correcte entre users et orders
- Agrégations multiples avec agg()
- Utilisation d'alias pour nommer les colonnes
- Filtrage après agrégation
- explain() pour afficher le plan d'exécution
"""

# Solution
result_q4 = (
    orders_df
    .join(users_df, on="user_id", how="inner")
    .groupBy("city")
    .agg(
        count("order_id").alias("nb_commandes"),
        sum("amount").alias("montant_total"),
        avg("amount").alias("montant_moyen")
    )
    .filter(col("montant_total") > 500)
    .orderBy(col("montant_total").desc())
)

print("Résultat Question 4 - Agrégations par ville:")
result_q4.show()
print("\nPlan d'exécution:")
result_q4.explain(True)
print()


print("=" * 80)
print("CORRECTION QUESTION 5 : SQL - Requête Complexe")
print("=" * 80)
"""
Points clés évalués :
- Création correcte des vues temporaires
- Jointure en SQL
- GROUP BY avec HAVING pour le filtrage
- Utilisation de COUNT et SUM
- ORDER BY
"""

# Solution
users_df.createOrReplaceTempView("users")
orders_df.createOrReplaceTempView("orders")

result_q5 = spark.sql("""
    SELECT 
        u.name,
        u.city,
        COUNT(o.order_id) as nb_commandes,
        SUM(o.amount) as montant_total
    FROM users u
    INNER JOIN orders o ON u.user_id = o.user_id
    GROUP BY u.user_id, u.name, u.city
    HAVING COUNT(o.order_id) >= 2
    ORDER BY montant_total DESC
""")

print("Résultat Question 5 - Utilisateurs avec au moins 2 commandes:")
result_q5.show()
print()


print("=" * 80)
print("CORRECTION QUESTION 6 : Jointures et Broadcast")
print("=" * 80)
"""
Points clés évalués :
- Création du DataFrame products avec schema correct
- Utilisation de broadcast() pour la petite table
- Jointure et agrégation
- Vérification du plan d'exécution (BroadcastHashJoin attendu)
"""

# Solution
products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("price", DoubleType(), False)
])

products_df = spark.createDataFrame(products_data, products_schema)

# Jointure avec broadcast
result_q6 = (
    orders_df
    .join(broadcast(products_df), on="product_id", how="inner")
    .groupBy("category")
    .agg(sum("amount").alias("chiffre_affaires"))
    .orderBy(col("chiffre_affaires").desc())
)

print("Résultat Question 6 - CA par catégorie:")
result_q6.show()
print("\nPlan d'exécution (vérifier BroadcastHashJoin):")
result_q6.explain(True)
print()


print("=" * 80)
print("CORRECTION QUESTION 7 : Window Functions - Ranking")
print("=" * 80)
"""
Points clés évalués :
- Calcul du total par utilisateur avant le ranking
- Window partitionBy city, orderBy montant décroissant
- Utilisation de row_number() (pas rank pour éviter ex-aequo)
- Filtrage pour garder uniquement le top 1
"""

# Solution - D'abord calculer le total par utilisateur
user_totals = (
    orders_df
    .join(users_df, on="user_id")
    .groupBy("user_id", "name", "city")
    .agg(sum("amount").alias("total_depense"))
)

# Définir la window function
window_rank = Window.partitionBy("city").orderBy(col("total_depense").desc())

# Appliquer le ranking et filtrer
result_q7 = (
    user_totals
    .withColumn("rank_in_city", row_number().over(window_rank))
    .filter(col("rank_in_city") == 1)
    .select("city", "name", "total_depense", "rank_in_city")
    .orderBy("city")
)

print("Résultat Question 7 - Top 1 client par ville:")
result_q7.show()
print()


print("=" * 80)
print("CORRECTION QUESTION 8 : Window Functions - Analyse Temporelle")
print("=" * 80)
"""
Points clés évalués :
- Window partitionBy user_id, orderBy order_date
- rowsBetween pour le cumul (unboundedPreceding à currentRow)
- lag() pour accéder à la valeur précédente
- Gestion du cas NULL avec coalesce ou 0
"""

# Solution
window_cum = Window \
    .partitionBy("user_id") \
    .orderBy("order_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

window_lag = Window \
    .partitionBy("user_id") \
    .orderBy("order_date")

result_q8 = (
    orders_df
    .withColumn("montant_cumule", sum("amount").over(window_cum))
    .withColumn("montant_precedent", lag("amount", 1).over(window_lag))
    .withColumn(
        "difference_precedent",
        col("amount") - coalesce(col("montant_precedent"), lit(0))
    )
    .select("user_id", "order_id", "order_date", "amount", 
            "montant_cumule", "montant_precedent", "difference_precedent")
    .orderBy("user_id", "order_date")
)

print("Résultat Question 8 - Analyse temporelle par utilisateur:")
result_q8.show(20, truncate=False)
print()


print("=" * 80)
print("CORRECTION QUESTION 9 : Déduplication Avancée")
print("=" * 80)
"""
Points clés évalués :
- Window partitionBy order_id (clé de déduplication)
- orderBy amount DESC pour garder le plus élevé
- row_number() pour identifier les doublons
- Filter sur rn == 1 pour garder la première ligne (montant max)
"""

orders_with_duplicates = [
    (1, 101, 150.0, "2024-01-10"),
    (1, 101, 145.0, "2024-01-10"),
    (1, 102, 200.0, "2024-01-15"),
    (2, 103, 300.0, "2024-01-20"),
    (2, 103, 310.0, "2024-01-20"),
    (2, 103, 295.0, "2024-01-20"),
    (3, 104, 500.0, "2024-01-05"),
]

# Schema pour les données avec doublons
dup_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("amount", DoubleType(), False),
    StructField("order_date", StringType(), False)
])

df_duplicates = spark.createDataFrame(orders_with_duplicates, dup_schema)

print("Données AVANT déduplication:")
df_duplicates.show()

# Solution - Déduplication
window_dedup = Window \
    .partitionBy("order_id") \
    .orderBy(col("amount").desc())  # DESC pour garder le montant le plus élevé

df_clean = (
    df_duplicates
    .withColumn("rn", row_number().over(window_dedup))
    .filter(col("rn") == 1)
    .drop("rn")
)

print("Données APRÈS déduplication (montant max conservé):")
df_clean.show()
print()


print("=" * 80)
print("CORRECTION QUESTION 10 : Pipeline Complet d'Analyse")
print("=" * 80)
"""
Points clés évalués :
- Pipeline fluide et lisible
- Utilisation de broadcast pour les petites tables
- Agrégation correcte (ville, catégorie)
- Window function pour le ranking
- Filtrage du top 2
- Optimisation (éviter shuffles inutiles)
"""

# Solution - Pipeline complet
# Étape 1 & 2: Jointure avec broadcast pour les petites tables et filtrage
df_joined = (
    orders_df
    .filter(col("amount") > 100)  # Filtrer tôt pour réduire les données
    .join(broadcast(products_df), on="product_id", how="inner")
    .join(broadcast(users_df), on="user_id", how="inner")
)

# Étape 3: Agrégation par ville et catégorie
df_agg = (
    df_joined
    .groupBy("city", "category")
    .agg(sum("amount").alias("montant_total"))
)

# Étape 4: Ranking par ville
window_rank = Window.partitionBy("city").orderBy(col("montant_total").desc())

df_ranked = df_agg.withColumn("rang", row_number().over(window_rank))

# Étape 5 & 6: Filtrage top 2 et tri final
result_final = (
    df_ranked
    .filter(col("rang") <= 2)
    .orderBy("city", "rang")
)

print("Résultat Question 10 - Top 2 catégories par ville:")
result_final.show()

print("\nPlan d'exécution du pipeline complet:")
result_final.explain(True)

print()
print("=" * 80)
print("                         FIN DE LA CORRECTION")
print("=" * 80)

# ================================================================================
# GRILLE DE CORRECTION
# ================================================================================
"""
BARÈME DÉTAILLÉ PAR QUESTION (2 points chacune):

Question 1 (2 pts):
- flatMap correct : 0.5 pt
- lower() pour insensibilité casse : 0.25 pt
- reduceByKey pour comptage : 0.5 pt
- filter > 2 : 0.25 pt
- sortBy décroissant : 0.5 pt

Question 2 (2 pts):
- mapValues initial : 0.5 pt
- reduceByKey avec tuple (sum, count) : 0.75 pt
- mapValues final pour moyenne : 0.5 pt
- Pas de groupByKey (bonus sinon -0.25) : 0.25 pt

Question 3 (2 pts):
- Schema users correct : 0.75 pt
- Schema orders correct : 0.75 pt
- Types corrects (Int, String, Double) : 0.25 pt
- printSchema() appelé : 0.25 pt

Question 4 (2 pts):
- Jointure correcte : 0.5 pt
- 3 agrégations (count, sum, avg) : 0.75 pt
- Filter > 500 : 0.25 pt
- OrderBy desc : 0.25 pt
- explain() : 0.25 pt

Question 5 (2 pts):
- createOrReplaceTempView : 0.25 pt
- Requête SQL correcte : 1 pt
- HAVING >= 2 : 0.5 pt
- ORDER BY : 0.25 pt

Question 6 (2 pts):
- Schema products correct : 0.5 pt
- broadcast() utilisé : 0.75 pt
- Jointure et agrégation : 0.5 pt
- explain() avec vérification broadcast : 0.25 pt

Question 7 (2 pts):
- Calcul total par user : 0.5 pt
- Window correcte (partitionBy, orderBy) : 0.5 pt
- row_number() (pas rank) : 0.5 pt
- Filter top 1 : 0.5 pt

Question 8 (2 pts):
- Window cumul avec rowsBetween : 0.75 pt
- lag() correct : 0.5 pt
- Gestion NULL (coalesce/0) : 0.5 pt
- OrderBy final correct : 0.25 pt

Question 9 (2 pts):
- Création DataFrame avec schema : 0.25 pt
- Window partitionBy order_id : 0.5 pt
- orderBy amount DESC : 0.5 pt
- row_number et filter rn==1 : 0.5 pt
- drop rn propre : 0.25 pt

Question 10 (2 pts):
- Jointures correctes : 0.5 pt
- Filter > 100 (placement optimal) : 0.25 pt
- Agrégation ville/catégorie : 0.5 pt
- Window ranking : 0.5 pt
- Filter top 2 et tri : 0.25 pt

PÉNALITÉS:
- Code qui ne s'exécute pas : -50% sur la question
- Erreur de syntaxe mineure : -0.25 pt
- Utilisation de collect() inutile sur gros volumes : -0.25 pt
- groupByKey au lieu de reduceByKey : -0.25 pt
"""
