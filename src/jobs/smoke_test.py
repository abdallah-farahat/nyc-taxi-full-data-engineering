"""
Smoke test: proves that Spark, Delta Lake and our folder mounts all work.
Run it with:  docker compose run --rm spark python src/jobs/smoke_test.py
"""

import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Read paths from environment variables (set in .env). No hardcoded paths in code.
RAW_DATA_PATH = os.environ["RAW_DATA_PATH"]
LAKEHOUSE_PATH = os.environ["LAKEHOUSE_PATH"]

# Build a Spark session in local mode: local[*] uses every CPU core available.
builder = (
    SparkSession.builder.appName("smoke_test")
    .master("local[*]")
    .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
    # These two lines switch Delta Lake on
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")   # hide the noisy INFO logs

print("Spark version:", spark.version)

# TEST 1: write a tiny Delta table to the lakehouse and read it back
test_path = f"{LAKEHOUSE_PATH}/_smoke_test"
spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "value"]) \
     .write.format("delta").mode("overwrite").save(test_path)
rows = spark.read.format("delta").load(test_path).count()
print("TEST 1 (Delta write and read): rows =", rows, "(expected 3)")

# TEST 2: read one real Parquet file from the raw data mount
green = spark.read.parquet(f"{RAW_DATA_PATH}/green/green_tripdata_2025-01.parquet")
print("TEST 2 (raw Parquet read): rows =", green.count(), "(expected 48,326)")

spark.stop()