from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
import time

# ─────────────────────────────
# Spark Session
# ─────────────────────────────
spark = (
    SparkSession.builder
    .appName("processor")
    .enableHiveSupport() 
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") 
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
    .getOrCreate()
)

# ─────────────────────────────
# Paths HDFS
# ─────────────────────────────
taxi_path    = "hdfs://namenode:9000/data/raw/nyc_taxi"
weather_path = "hdfs://namenode:9000/data/raw/weather"
zones_path   = "hdfs://namenode:9000/data/raw/zone"

# ─────────────────────────────
# Read data
# ─────────────────────────────
print("Lecture Raw Taxi...")
df = spark.read.parquet(taxi_path)

print("Lecture Raw Weather...")
df_weather = spark.read.parquet(weather_path)

print("Lecture Zones...")
df_zones = spark.read.parquet(zones_path)

# ─────────────────────────────
# Cast & clean taxi
# ─────────────────────────────
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

# ─────────────────────────────
# Feature engineering
# ─────────────────────────────
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

# ─────────────────────────────
# Zones join (IMPORTANT FIX)
# ─────────────────────────────
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

# ─────────────────────────────
# Weather join
# ─────────────────────────────
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

# ─────────────────────────────
# Ranking
# ─────────────────────────────
window_spec = Window.partitionBy("PU_Borough").orderBy(F.desc("total_amount"))
df2 = df2.withColumn("rank_in_borough", F.rank().over(window_spec))

# ─────────────────────────────
# Persist
# ─────────────────────────────
df2.persist()
print("Rows Silver:", df2.count())

time.sleep(60)

# ─────────────────────────────
# Fix partition columns (IMPORTANT)
# ─────────────────────────────
df2 = (
    df2
    .withColumn("year", F.col("pickup_year"))
    .withColumn("month", F.col("pickup_month"))
    .withColumn("day", F.dayofmonth("pickup_date"))
)

# ─────────────────────────────
# Write Hive
# ─────────────────────────────
(
    df2
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("year", "month", "day")
    .saveAsTable("default.nyc_taxi_silver")
)

print("Silver OK → Hive table created")

spark.stop()