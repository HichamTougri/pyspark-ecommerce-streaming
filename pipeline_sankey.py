from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import json

# 1. Initialisation
spark = SparkSession.builder \
    .appName("LeBonCoin_Sankey") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# 2. Schéma
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

flux_brut = spark.readStream.schema(schema).json("streaming_input")

# Mémoire globale pour accumuler les volumes de flux
memoire_sankey = {}

# 3. Fonction de traitement par lot
def extraire_sankey(batch_df, batch_id):
    global memoire_sankey
    
    if batch_df.count() > 0:
        # A. Calcul des flux : Ville -> Action
        flux_ville_action = batch_df.groupBy("user_city", "action_type").count().collect()
        for row in flux_ville_action:
            cle = f"{row['user_city']} -> {row['action_type']}"
            if cle not in memoire_sankey:
                memoire_sankey[cle] = {"src": row['user_city'], "dst": row['action_type'], "weight": 0}
            memoire_sankey[cle]["weight"] += row['count']

        # B. Calcul des flux : Action -> Catégorie
        flux_action_cat = batch_df.groupBy("action_type", "product_cat").count().collect()
        for row in flux_action_cat:
            cle = f"{row['action_type']} -> {row['product_cat']}"
            if cle not in memoire_sankey:
                memoire_sankey[cle] = {"src": row['action_type'], "dst": row['product_cat'], "weight": 0}
            memoire_sankey[cle]["weight"] += row['count']
        
        # C. Formatage exact attendu par Google Charts
        export_data = [["Source", "Destination", "Volume"]]
        for data in memoire_sankey.values():
            export_data.append([data["src"], data["dst"], data["weight"]])
            
        # Exportation dans un nouveau fichier JSON
        with open("sankey_export.json", "w", encoding="utf-8") as f:
            json.dump(export_data, f, ensure_ascii=False, indent=4)
            
        print(f"[SANKEY] Batch {batch_id} traité. {len(export_data)-1} routes actives mises à jour.")

# 4. Démarrage du flux
query = flux_brut.writeStream \
    .foreachBatch(extraire_sankey) \
    .outputMode("append") \
    .start()

query.awaitTermination()
