# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("zone_feeder").getOrCreate()

input_path = "file:///source/zone/taxi_zone_lookup.csv"
output_path = "hdfs://namenode:9000/data/raw/zone"

df = spark.read.option("header", True).csv(input_path)

df.write.mode("overwrite").parquet(output_path)

print("Zones écrites sur HDFS")
spark.stop()