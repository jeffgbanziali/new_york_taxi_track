
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import date, timedelta
import urllib.request
import json
import time

spark = (
    SparkSession.builder
    .appName("api_collector")
    .getOrCreate()
)

LATITUDE    = "40.7143"    # New York City
LONGITUDE   = "-74.006"
START_DATE  = "2024-01-01"
END_DATE    = "2024-03-31"
OUTPUT_PATH = "hdfs://namenode:9000/data/raw/weather"

API_URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude={lat}"
    "&longitude={lon}"
    "&start_date={start}"
    "&end_date={end}"
    "&hourly=temperature_2m,precipitation,windspeed_10m,weathercode"
    "&timezone=America%2FNew_York"
).format(
    lat=LATITUDE,
    lon=LONGITUDE,
    start=START_DATE,
    end=END_DATE
)

print("Appel API Open-Meteo...")
print("URL : {}".format(API_URL))

try:
    req = urllib.request.urlopen(API_URL, timeout=30)
    raw_data = json.loads(req.read().decode("utf-8"))
    print("Reponse API recue !")
except Exception as e:
    print("Erreur API : {}".format(e))
    spark.stop()
    exit(1)

# ── Transformation en liste de dictionnaires ───────
hourly = raw_data.get("hourly", {})
timestamps    = hourly.get("time", [])
temperatures  = hourly.get("temperature_2m", [])
precipitations = hourly.get("precipitation", [])
windspeeds    = hourly.get("windspeed_10m", [])
weathercodes  = hourly.get("weathercode", [])

print("Nombre d'heures recues : {}".format(len(timestamps)))

# ── Création d'une liste de rows ────────
rows = []
for i in range(len(timestamps)):
    ts = timestamps[i]
    dt_parts = ts.split("T")
    date_part = dt_parts[0]          
    time_part = dt_parts[1]          
    hour = int(time_part.split(":")[0])
    year  = int(date_part.split("-")[0])
    month = int(date_part.split("-")[1])
    day   = int(date_part.split("-")[2])

    rows.append((
        ts,
        date_part,
        hour,
        year,
        month,
        day,
        float(temperatures[i])   if temperatures[i]   is not None else 0.0,
        float(precipitations[i]) if precipitations[i] is not None else 0.0,
        float(windspeeds[i])     if windspeeds[i]     is not None else 0.0,
        int(weathercodes[i])     if weathercodes[i]   is not None else 0,
    ))

# ── Schéma du DataFrame météo ──────
schema = StructType([
    StructField("datetime",      StringType(),  True),
    StructField("date",          StringType(),  True),
    StructField("hour",          IntegerType(), True),
    StructField("year",          IntegerType(), True),
    StructField("month",         IntegerType(), True),
    StructField("day",           IntegerType(), True),
    StructField("temperature",   DoubleType(),  True),
    StructField("precipitation", DoubleType(),  True),
    StructField("windspeed",     DoubleType(),  True),
    StructField("weathercode",   IntegerType(), True),
])

# ── Création du DataFrame Spark ──────
df_weather = spark.createDataFrame(rows, schema=schema)

# ── Ajout colonne description météo ─────
df_weather = df_weather.withColumn(
    "weather_desc",
    F.when(F.col("weathercode") == 0,  "Ciel clair")
     .when(F.col("weathercode") <= 3,  "Nuageux")
     .when(F.col("weathercode") <= 49, "Brouillard")
     .when(F.col("weathercode") <= 69, "Pluie")
     .when(F.col("weathercode") <= 79, "Neige")
     .when(F.col("weathercode") <= 99, "Orage")
     .otherwise("Inconnu")
)

df_weather.cache()
df_weather.show(10)
print("Lignes météo : {}".format(df_weather.count()))


(
    df_weather
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(OUTPUT_PATH)
)

print("Meteo ecrite sur HDFS : {}".format(OUTPUT_PATH))
spark.stop()