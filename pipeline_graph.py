from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col, lit
import json
import os

#INITIALISATION DE L'ENVIRONNEMENT
# Configuration de la session Spark avec le support des graphes (GraphFrames)
spark = SparkSession.builder \
    .appName("LeBonCoin_Graphe_Temps_Reel") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
    .getOrCreate()

# DÉFINITION DU SCHÉMA ET LECTURE DU FLUX
# Structure stricte attendue pour les fichiers JSON entrants
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

# Lecture du flux continu (1 fichier max par itération) depuis le dossier source
flux_brut = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json("streaming_input")

# Ajout d'une tolérance de 10 minutes pour gérer les données arrivant en retard (Watermarking)
flux_securise = flux_brut.withWatermark("timestamp", "10 minutes")

# LOGIQUE DE TRAITEMENT PAR LOT (MICRO-BATCH)
def extraire_graphe(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f"\n[BATCH {batch_id}] Début du traitement des données reçues...")
        
        # Extraction des Nœuds
        # Récupération des valeurs uniques pour chaque type d'entité
        users = batch_df.select(col("user_id").alias("id")).distinct().collect()
        products = batch_df.select(col("product_id").alias("id")).distinct().collect()
        sellers = batch_df.select(col("seller_id").alias("id")).distinct().collect()
        
        # Formatage des nœuds pour la compatibilité avec le front-end (Vis.js)
        nodes = []
        for r in users: nodes.append({"id": r["id"], "label": r["id"], "group": "Utilisateur"})
        for r in products: nodes.append({"id": r["id"], "label": r["id"], "group": "Produit"})
        for r in sellers: nodes.append({"id": r["id"], "label": r["id"], "group": "Vendeur"})
        
        # Extraction des Arêtes
        # Interactions Utilisateur -> Produit (ex: VUE, ACHAT)
        user_prod_edges = batch_df.select(
            col("user_id").alias("from"), 
            col("product_id").alias("to"), 
            col("action_type").alias("label")
        ).distinct().collect()
        
        # Liens de propriété Vendeur -> Produit
        sel_prod_edges = batch_df.select(
            col("seller_id").alias("from"), 
            col("product_id").alias("to")
        ).distinct().collect()
        
        # Formatage des arêtes
        edges = []
        for r in user_prod_edges: edges.append({"from": r["from"], "to": r["to"], "label": r["label"]})
        for r in sel_prod_edges: edges.append({"from": r["from"], "to": r["to"], "label": "PROPOSE"})
        
        #C. Exportation pour le Dashboard
        export_data = {"nodes": nodes, "edges": edges}
        
        # Écriture des données dans un fichier JSON (écrasé à chaque batch pour la mise à jour)
        with open("graph_export.json", "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=4)
            
        print(f"[GRAPHE] Batch {batch_id} exporté avec succès ({len(nodes)} nœuds et {len(edges)} arêtes).")


# Application de notre fonction à chaque nouveau micro-batch détecté
query = flux_securise.writeStream \
    .foreachBatch(extraire_graphe) \
    .outputMode("append") \
    .start()

# Maintien du script actif pour écouter les nouveaux fichiers indéfiniment
query.awaitTermination()
