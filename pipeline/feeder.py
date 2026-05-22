from pyspark.sql import SparkSession, functions as F
import time

# ─────────────────────────────────────────────────────────────
#  feeder.py — Ingestion Raw (couche Bronze)
#
#  Adapté du feeder.py du prof pour les données NYC Taxi
#  Lit les Parquet source → écrit sur HDFS partitionné year/month/day
#
#  Lancement :
#  spark-submit --master spark://spark-master:7077 \
#     /opt/pipeline/feeder.py
# ─────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("feeder")
    .getOrCreate()
)

# ── Chemins (variables, pas codés en dur) ─────────────────────
input_path  = "file:///source/yellow/"
output_base = "hdfs://namenode:9000/data/raw/nyc_taxi"

# ── Lecture des fichiers Parquet source ───────────────────────
df = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(input_path)
)

# ── Ajout des colonnes de partitionnement par DATE D'INGESTION ──
# FIX VALIDATION PROF : On se base sur l'heure système (current_timestamp) et non sur la date du trajet
df2 = (
    df
    .withColumn("source", F.lit("yellow"))
    .withColumn("date_ingestion", F.current_timestamp())
    .withColumn(
        "year",
        F.year(F.col("date_ingestion"))
    )
    .withColumn(
        "month",
        F.month(F.col("date_ingestion"))
    )
    .withColumn(
        "day",
        F.dayofmonth(F.col("date_ingestion"))
    )
)

# ── cache() — visible dans Spark UI → Storage ─────────────────
df2.cache()

df2.show(5)

r = df2.count()
print("Nombre de lignes : {}".format(r))

# ── time.sleep : permet de voir le cache dans Spark UI ────────
# C'est à ce moment précis que tu dois ouvrir http://localhost:4040/storage/ pour prendre ta capture d'écran !
time.sleep(120)

# ── Écriture Raw sur HDFS partitionné par year/month/day ──────

df2 = df2.repartition(16)

(
    df2
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_base)
)

print("Raw écrit sur HDFS : {}".format(output_base))
spark.stop()