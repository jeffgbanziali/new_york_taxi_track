from pyspark.sql import SparkSession, functions as F
import time


spark = (
    SparkSession.builder
    .appName("feeder")
    .getOrCreate()
)

input_path  = "file:///source/yellow/"
output_base = "hdfs://namenode:9000/data/raw/nyc_taxi"

df = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(input_path)
)

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

df2.cache()

df2.show(5)

r = df2.count()
print("Nombre de lignes : {}".format(r))




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