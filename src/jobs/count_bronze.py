"""Prints Bronze row counts, split into batch rows and stream rows."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import os

builder = (SparkSession.builder.master("local[*]")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

for table in ("yellow_trips", "green_trips"):
    df = spark.read.format("delta").load(f"{os.environ['LAKEHOUSE_PATH']}/bronze/{table}")
    total = df.count()
    streamed = df.filter("source_file like 'stream_%'").count()
    print(f"{table}: total={total:,}  batch={total - streamed:,}  stream={streamed:,}")
spark.stop()