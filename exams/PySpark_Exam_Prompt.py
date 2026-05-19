"""
================================================================================
                        EXAMEN PySpark - 4ème Année Ingénieur
================================================================================
Durée : 1h30
Documents autorisés : Aucun
Tout code doit être écrit en PySpark. Les imports nécessaires sont fournis.

INSTRUCTIONS :
- Chaque question demande d'écrire du code PySpark fonctionnel
- Commentez brièvement votre code si nécessaire
- La lisibilité et l'efficacité du code seront prises en compte
================================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, sum, avg, count, broadcast, row_number, rank, dense_rank, lag, lead, coalesce, lit
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("ExamenPySpark") \
    .master("local[*]") \
    .getOrCreate(

sc = spark.sparkContext

# ================================================================================
# DONNÉES DE CONTEXTE : Plateforme e-commerce
# ================================================================================
# Vous travaillez sur les données d'une plateforme e-commerce avec :
# - Des utilisateurs (users)
# - Des commandes (orders)  
# - Des produits (products)

# Données utilisateurs
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

# Données commandes (user_id, order_id, product_id, amount, order_date)
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

# Données produits
products_data = [
    (1, "Laptop", "Electronics", 999.99),
    (2, "Headphones", "Electronics", 149.99),
    (3, "Book", "Media", 29.99)
]

# ================================================================================
# QUESTION 1 : RDD - Word Count Avancé (2 points)
# ================================================================================
"""
À partir du texte suivant, utilisez les RDDs pour :
1. Compter le nombre d'occurrences de chaque mot (insensible à la casse)
2. Filtrer uniquement les mots qui apparaissent plus de 2 fois
3. Trier les résultats par nombre d'occurrences décroissant

Le résultat doit être une liste de tuples (mot, count) triée.
"""

texte = """
PySpark is a powerful tool for big data processing
Spark provides fast distributed computing
PySpark makes Spark accessible through Python
Big data needs powerful processing tools
Spark is fast and Spark is scalable
"""

# Votre code ici :




# ================================================================================
# QUESTION 2 : RDD - Calcul de Statistiques avec mapValues (2 points)
# ================================================================================
"""
À partir des données de ventes ci-dessous (vendeur, montant), utilisez les RDDs pour :
1. Calculer pour chaque vendeur : la somme totale, le nombre de ventes, et la moyenne
2. Retourner un RDD de tuples : (vendeur, (total, count, moyenne))

Utilisez mapValues et reduceByKey (pas groupByKey pour des raisons de performance).
"""

ventes_data = [
    ("Alice", 100), ("Bob", 200), ("Alice", 150),
    ("Charlie", 300), ("Bob", 50), ("Alice", 200),
    ("Charlie", 100), ("Bob", 150), ("Charlie", 250)
]

# Votre code ici :




# ================================================================================
# QUESTION 3 : DataFrame - Création avec Schema et Transformations (2 points)
# ================================================================================
"""
1. Créez un DataFrame 'users_df' à partir de 'users_data' avec un schema explicite
   Colonnes : user_id (Int), name (String), city (String), registration_date (String)
   
2. Créez un DataFrame 'orders_df' à partir de 'orders_data' avec un schema explicite
   Colonnes : user_id (Int), order_id (Int), product_id (Int), amount (Double), order_date (String)

3. Affichez le schema des deux DataFrames
"""

# Votre code ici :




# ================================================================================
# QUESTION 4 : DataFrame - Agrégations Complexes (2 points)
# ================================================================================
"""
En utilisant les DataFrames créés précédemment (users_df et orders_df) :

1. Calculez pour chaque ville :
   - Le nombre total de commandes
   - Le montant total des commandes
   - Le montant moyen par commande
   
2. Filtrez uniquement les villes ayant un montant total > 500
3. Triez par montant total décroissant

Affichez le résultat et le plan d'exécution physique.
"""

# Votre code ici :




# ================================================================================
# QUESTION 5 : SQL - Requête Complexe (2 points)
# ================================================================================
"""
Enregistrez les DataFrames comme vues temporaires et écrivez une requête SQL qui :

1. Pour chaque utilisateur, retourne :
   - Son nom
   - Sa ville
   - Le nombre total de ses commandes
   - Le montant total de ses commandes
   
2. Ne gardez que les utilisateurs ayant passé au moins 2 commandes
3. Triez par montant total décroissant

Utilisez spark.sql() pour exécuter la requête.
"""

# Votre code ici :




# ================================================================================
# QUESTION 6 : Jointures et Broadcast (2 points)
# ================================================================================
"""
1. Créez un DataFrame 'products_df' à partir de 'products_data' avec le schema :
   product_id (Int), product_name (String), category (String), price (Double)

2. Effectuez une jointure entre orders_df et products_df
   Utilisez un broadcast hint car products_df est petit

3. Calculez le chiffre d'affaires total par catégorie de produit
   
4. Affichez le plan d'exécution pour vérifier que le broadcast est utilisé
"""

# Votre code ici :




# ================================================================================
# QUESTION 7 : Window Functions - Ranking (2 points)
# ================================================================================
"""
En utilisant les window functions :

1. Pour chaque ville, classez les utilisateurs par montant total dépensé (du plus au moins)
2. Ajoutez une colonne 'rank_in_city' contenant le rang de chaque utilisateur dans sa ville
3. Affichez uniquement le top 1 client de chaque ville (celui qui a le plus dépensé)

Utilisez row_number() et non rank() pour éviter les ex-aequo.
"""

# Votre code ici :




# ================================================================================
# QUESTION 8 : Window Functions - Analyse Temporelle (2 points)
# ================================================================================
"""
En utilisant les window functions sur orders_df :

1. Pour chaque utilisateur, calculez :
   - Le montant cumulé de ses commandes dans l'ordre chronologique
   - La différence de montant avec sa commande précédente (utilisez lag)
   
2. Ordonnez par user_id puis par date de commande
3. Affichez le résultat complet

Gérez le cas où il n'y a pas de commande précédente (première commande).
"""

# Votre code ici :




# ================================================================================
# QUESTION 9 : Déduplication Avancée (2 points)
# ================================================================================
"""
Les données suivantes contiennent des doublons de commandes (dues à des problèmes techniques).
Pour un même order_id, on peut avoir plusieurs lignes avec des montants différents.

Règle de déduplication : Garder uniquement la ligne avec le montant le plus élevé pour chaque order_id.

Utilisez les window functions pour effectuer cette déduplication.
"""

orders_with_duplicates = [
    (1, 101, 150.0, "2024-01-10"),
    (1, 101, 145.0, "2024-01-10"),  # doublon
    (1, 102, 200.0, "2024-01-15"),
    (2, 103, 300.0, "2024-01-20"),
    (2, 103, 310.0, "2024-01-20"),  # doublon avec montant plus élevé
    (2, 103, 295.0, "2024-01-20"),  # doublon
    (3, 104, 500.0, "2024-01-05"),
]

# Votre code ici :




# ================================================================================
# QUESTION 10 : Pipeline Complet d'Analyse (2 points)
# ================================================================================
"""
Écrivez une pipeline complete qui :

1. Join users_df, orders_df et products_df
2. Filtre les commandes de plus de 100€
3. Pour chaque combinaison (ville, catégorie de produit), calcule le montant total
4. Ajoute un rang par ville basé sur le montant total (du plus élevé au plus bas)
5. Ne conserve que le top 2 des catégories par ville
6. Affiche le résultat final trié par ville puis par rang

Cette pipeline doit être écrite de manière optimisée (évitez les shuffles inutiles).
"""

# Votre code ici :




# ================================================================================
# FIN DE L'EXAMEN
# ================================================================================
"""
Vérifiez que tout votre code s'exécute sans erreur avant de rendre votre copie.
Consignes de rendu :
    - Envoyer un mail avec le .py contenant vos réponses à yidhir.moudoub@outlook.com
    - Objet : [4IABD1][SPARK CORE] Réponses Contrôle Spark Core
    - Le .py doit être nommé comme suit : nom_prenom.py

Bonne chance !
"""
