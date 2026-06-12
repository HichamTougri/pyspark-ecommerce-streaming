from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit
import json

# 1. Initialisation du moteur PySpark
spark = SparkSession.builder \
    .appName("LeBonCoin_Graphes") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# 2. Schema Enforcement
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price", DoubleType(), True)
])

# 3. Ingestion du flux brut
flux_brut = spark.readStream \
    .schema(schema) \
    .json("streaming_input")

# --- NOUVEAUTÉ : La mémoire globale du réseau ---
memoire_noeuds = {}
memoire_liens = {}

# 4. Fonction de traitement avec accumulation
def extraire_graphe(batch_df, batch_id):
    global memoire_noeuds, memoire_liens
    
    if batch_df.count() > 0:
        
        # A. Création des DataFrames temporaires
        edges_users = batch_df.select(
            col("user_id").alias("src"), col("product_id").alias("dst"), col("action_type").alias("relationship")
        )
        edges_sellers = batch_df.select(
            col("seller_id").alias("src"), col("product_id").alias("dst"), lit("PROPOSE").alias("relationship")
        ).dropDuplicates()
        edges = edges_users.unionByName(edges_sellers)
        
        users = batch_df.select(col("user_id").alias("id"), lit("Utilisateur").alias("type"), col("user_city").alias("label"))
        sellers = batch_df.select(col("seller_id").alias("id"), lit("Vendeur").alias("type"), lit("Vendeur").alias("label"))
        products = batch_df.select(col("product_id").alias("id"), lit("Produit").alias("type"), col("product_cat").alias("label"))
        vertices = users.unionByName(sellers).unionByName(products).dropDuplicates(['id'])
        
        # B. Mise à jour de la mémoire avec les nouvelles données
        for row in vertices.collect():
            memoire_noeuds[row['id']] = row.asDict()
            
        for row in edges.collect():
            # Création d'un identifiant unique pour chaque interaction
            id_lien = f"{row['src']}-{row['relationship']}-{row['dst']}"
            memoire_liens[id_lien] = row.asDict()
        
        # C. Exportation du réseau complet et accumulé
        export_data = {
            "nodes": list(memoire_noeuds.values()),
            "links": list(memoire_liens.values())
        }
        
        with open("graphe_export.json", "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=4)
            
        print(f"[RESEAU] Batch {batch_id} intégré. Le réseau contient maintenant {len(memoire_noeuds)} entités et {len(memoire_liens)} interactions.")

# 5. Démarrage du flux
print("Démarrage du Pipeline de traitement Graphe (avec accumulation)...")
query = flux_brut.writeStream \
    .foreachBatch(extraire_graphe) \
    .outputMode("append") \
    .start()

query.awaitTermination()