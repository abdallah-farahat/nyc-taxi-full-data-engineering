"""
Gate 1 test: proves that bad files go to quarantine_bronze instead of crashing the job.
It uses a separate test folder and a separate test lakehouse, so real data is untouched.
Run with:  docker compose run --rm spark python -u src/jobs/gate1_test.py
"""

import os
import shutil

TEST_ROOT = "/data/lakehouse/_gate1_test"

# Point the job at the test folders BEFORE importing it
# (bronze_batch reads these variables when it is imported).
os.environ["RAW_DATA_PATH"] = f"{TEST_ROOT}/raw"
os.environ["LAKEHOUSE_PATH"] = f"{TEST_ROOT}/lakehouse"

# Start from a clean test area every time
shutil.rmtree(TEST_ROOT, ignore_errors=True)
os.makedirs(f"{TEST_ROOT}/raw/yellow")

import bronze_batch                    # noqa: E402  (imported after setting the variables)
from pyspark.sql import functions as F  # noqa: E402

spark = bronze_batch.build_spark()
yellow_dir = f"{TEST_ROOT}/raw/yellow"

# A tiny file that has ALL mandatory columns (should pass Gate 1)
good = spark.range(2).select(
    F.lit(1).alias("VendorID"),
    F.current_timestamp().alias("tpep_pickup_datetime"),
    F.current_timestamp().alias("tpep_dropoff_datetime"),
    F.lit(1).alias("passenger_count"),
    F.lit(1.5).alias("trip_distance"),
    F.lit(100).alias("PULocationID"),
    F.lit(200).alias("DOLocationID"),
    F.lit(1).alias("payment_type"),
    F.lit(10.0).alias("fare_amount"),
    F.lit(12.0).alias("total_amount"),
)

# File 1: good file                          -> expected: loaded into Bronze
good.coalesce(1).write.parquet(f"{yellow_dir}/yellow_tripdata_2025-01.parquet")
# File 2: mandatory column total_amount gone -> expected: quarantined
good.drop("total_amount").coalesce(1).write.parquet(
    f"{yellow_dir}/yellow_tripdata_2025-02.parquet")
# File 3: not a Parquet file at all           -> expected: quarantined
with open(f"{yellow_dir}/yellow_tripdata_2025-03.parquet", "w") as f:
    f.write("this is not a parquet file")

# Run the real job on the test folders. It must finish WITHOUT crashing.
bronze_batch.main()

# Show the result: what is in quarantine and what reached Bronze
spark = bronze_batch.build_spark()
print("\n=== quarantine_bronze ===")
(spark.read.format("delta").load(bronze_batch.QUARANTINE_PATH)
      .select("source_file", "taxi_type", "failure_reason").show(truncate=False))
bronze_rows = (spark.read.format("delta")
               .load(f"{bronze_batch.BRONZE_PATH}/yellow_trips").count())
print("Bronze rows (expected 2):", bronze_rows)
spark.stop()