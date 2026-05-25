from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
import time

spark = (
    SparkSession.builder
    .appName("processor")
    .enableHiveSupport() 
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") 
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .getOrCreate()
)

taxi_path    = "hdfs://namenode:9000/data/raw/nyc_taxi"
weather_path = "hdfs://namenode:9000/data/raw/weather"
zones_path   = "hdfs://namenode:9000/data/raw/zone"

# Lecture des données Parquet depuis HDFS
print("Lecture Raw Taxi...")
df = spark.read.parquet(taxi_path)

print("Lecture Raw Weather...")
df_weather = spark.read.parquet(weather_path)

print("Lecture Zones...")
df_zones = spark.read.parquet(zones_path)

# Nettoyage et conversion des types (Data Cleaning)
df2 = (
    df
    .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
    .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
    .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
    .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
    .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
    .filter(F.col("total_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("tpep_pickup_datetime").isNotNull())
    .dropDuplicates(["tpep_pickup_datetime", "PULocationID", "total_amount"])
)

# Calculs de nouvelles colonnes 
df2 = (
    df2
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_weekday", F.dayofweek("tpep_pickup_datetime"))
    .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn(
        "trip_duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60
    )
    .filter(F.col("trip_duration_min").between(1, 180))
)

# Jointure avec les Zones de Taxi
df_zones_pu = (
    df_zones
    .selectExpr("LocationID as PULocationID", "Borough as PU_Borough", "Zone as PU_Zone")
)

df_zones_do = (
    df_zones
    .selectExpr("LocationID as DOLocationID", "Borough as DO_Borough", "Zone as DO_Zone")
)

df2 = (
    df2
    .join(F.broadcast(df_zones_pu), "PULocationID", "left")
    .join(F.broadcast(df_zones_do), "DOLocationID", "left")
)

# Jointure  avec la Météo
df_weather_join = df_weather.select(
    "date", "hour",
    "temperature", "precipitation",
    "windspeed", "weather_desc"
)

df2 = (
    df2
    .withColumn("pickup_date_str", F.col("pickup_date").cast("string"))
    .join(
        F.broadcast(df_weather_join),
        (F.col("pickup_date_str") == df_weather_join["date"]) &
        (F.col("pickup_hour") == df_weather_join["hour"]),
        "left"
    )
    .drop("pickup_date_str", "date", "hour")
)

# Fonctions d'analyse de fenêtrage 
window_spec = Window.partitionBy("PU_Borough").orderBy(F.desc("total_amount"))
df2 = df2.withColumn("rank_in_borough", F.rank().over(window_spec))

# Persistance en mémoire (Pour la Spark UI)
df2.persist()
print("Rows Silver:", df2.count())


# Partitionnement par la DATE D'INGESTION 
# On demande à Spark de regarder l'horloge de l'ordinateur 
df2 = (
    df2
    .withColumn("silver_ingestion", F.current_timestamp())
    .withColumn("year", F.year(F.col("silver_ingestion")))
    .withColumn("month", F.month(F.col("silver_ingestion")))
    .withColumn("day", F.dayofmonth(F.col("silver_ingestion")))
    .drop("silver_ingestion")
)

# Écriture dans le Data Warehouse (Table Silver Hive)
(
    df2
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("default.nyc_taxi_silver")
)

print("Silver OK → Table Hive créée et rangée dans l'étagère du jour.")

spark.stop()