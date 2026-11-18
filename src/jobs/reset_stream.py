"""
Removes everything the streaming job wrote (Bronze stream rows, their quarantine rows
and the checkpoints), so the stream can be rebuilt cleanly from Kafka.
Batch rows are never touched. Safe ONLY before Silver has been built from these rows.
"""
import os
import shutil

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

L = os.environ["LAKEHOUSE_PATH"]
builder = (SparkSession.builder.master("local[*]")
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Delete only the stream rows
for table in ("yellow_trips", "green_trips"):
    path = f"{L}/bronze/{table}"
    if DeltaTable.isDeltaTable(spark, path):
        DeltaTable.forPath(spark, path).delete("source_file like 'stream_%'")
quarantine = f"{L}/quarantine/quarantine_bronze"
if DeltaTable.isDeltaTable(spark, quarantine):
    DeltaTable.forPath(spark, quarantine).delete("source_file like 'stream_%'")

# Forget which Kafka offsets were processed
shutil.rmtree(f"{L}/_checkpoints", ignore_errors=True)
print("stream data and checkpoints removed")
spark.stop()