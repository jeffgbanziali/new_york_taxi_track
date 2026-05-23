from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("zone_feeder").getOrCreate()

input_path = "file:///source/zone/taxi_zone_lookup.csv"
output_path = "hdfs://namenode:9000/data/raw/zone"

print("Lecture du fichier CSV des zones...")
df = spark.read.option("header", True).csv(input_path)

df_partitioned = (
    df
    .withColumn("ingestion_time", F.current_timestamp())
    .withColumn("year", F.year("ingestion_time"))
    .withColumn("month", F.month("ingestion_time"))
    .withColumn("day", F.dayofmonth("ingestion_time"))
    .drop("ingestion_time")
)

print("Ecriture du referentiel des zones au format Parquet sur HDFS...")
(
    df_partitioned.write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_path)
)

print("Zones ecrites avec succes sur HDFS dans l'etagere du jour.")
spark.stop()