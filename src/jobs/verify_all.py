"""
Deep verification of the whole pipeline (read-only).
Compares every layer with its source and prints [PASS] or [FAIL] for each check.
Run with:  docker compose run --rm spark python -u src/jobs/verify_all.py
"""

import os
import sys

import pyarrow.parquet as pq
import yaml
from confluent_kafka import Consumer, TopicPartition
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import audit

L = os.environ["LAKEHOUSE_PATH"]
RAW = os.environ["RAW_DATA_PATH"]
STREAM_MONTH = os.getenv("STREAM_MONTH", "2026-09")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}" + (f"   ({detail})" if detail else ""))


def info(text):
    print(f"[INFO] {text}")


builder = (SparkSession.builder.master("local[4]")
           .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "8g"))
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.sql.shuffle.partitions", "24"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def delta(path):
    return spark.read.format("delta").load(f"{L}/{path}")


def exists(path):
    return DeltaTable.isDeltaTable(spark, f"{L}/{path}")


def sql_rows(query):
    conn = audit.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    finally:
        conn.close()


def kafka_count(topic):
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "verify-probe"})
    partitions = consumer.list_topics(topic, timeout=15).topics[topic].partitions
    total = 0
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        total += high - low
    consumer.close()
    return total


with open("/app/config/sources.yml") as f:
    sources = yaml.safe_load(f)["sources"]

# ------------------------------------------------------------------ A. Bronze
print("\n=== A. BRONZE (raw layer, batch + stream) ===")
bronze_total = 0
for taxi, src in sources.items():
    bronze = delta(f"bronze/{src['table']}")
    batch = bronze.filter("source_file not like 'stream_%'").count()
    stream = bronze.filter("source_file like 'stream_%'").count()
    bronze_total += batch + stream
    file_rows = pq.ParquetFile(
        f"{RAW}/{taxi}/{taxi}_tripdata_{STREAM_MONTH}.parquet").metadata.num_rows
    check(f"{taxi}: batch rows = profiling number", batch == src["expected_rows"],
          f"{batch:,} vs {src['expected_rows']:,}")
    check(f"{taxi}: stream rows = rows of the {STREAM_MONTH} source file", stream == file_rows,
          f"{stream:,} vs {file_rows:,}")
    check(f"{taxi}: Kafka messages = rows of the {STREAM_MONTH} source file",
          kafka_count(f"taxi.{taxi}.trips") == file_rows)
    check(f"{taxi}: no gaps in source_file names (20 months of batch + 1 stream)",
          bronze.select("source_file").distinct().count() == 20 + 1,
          f"{bronze.select('source_file').distinct().count()} distinct source files")
if exists("quarantine/quarantine_bronze"):
    info(f"quarantine_bronze rows: {delta('quarantine/quarantine_bronze').count():,}")
else:
    info("quarantine_bronze does not exist yet (no file or message failed Gate 1)")

# ------------------------------------------------------------------ B. Silver
print("\n=== B. SILVER (clean, unified layer) ===")
silver = delta("silver/trips")
silver_n = silver.count()
q_silver_n = delta("quarantine/quarantine_silver").count() if exists("quarantine/quarantine_silver") else 0
duplicates = bronze_total - silver_n - q_silver_n
check("Bronze = Silver + quarantine_silver + exact duplicates",
      0 <= duplicates <= 100,
      f"{bronze_total:,} = {silver_n:,} + {q_silver_n:,} + {duplicates:,} duplicates")
required = {"trip_id", "taxi_type", "pickup_datetime", "dropoff_datetime", "pu_location_id",
            "do_location_id", "payment_type", "total_amount", "pickup_month",
            "data_quality_flags", "is_flex_fare", "ingestion_timestamp", "trip_duration_min"}
check("Silver has all required columns", required.issubset(set(silver.columns)),
      f"missing: {sorted(required - set(silver.columns))}")
check("Silver does not contain the dead column ehail_fee", "ehail_fee" not in silver.columns)
check("Silver has no NULL trip_id or pickup/dropoff time",
      silver.filter("trip_id is null or pickup_datetime is null or dropoff_datetime is null").count() == 0)
check("Silver taxi types are only yellow and green",
      {r[0] for r in silver.select("taxi_type").distinct().collect()} == {"yellow", "green"})
leaks = silver.filter("trip_distance > 100 or fare_amount > 1000 or speed_mph > 100 "
                      "or passenger_count > 6 or pickup_datetime < '2025-01-01' "
                      "or pickup_datetime >= '2026-10-01' or dropoff_datetime < pickup_datetime").count()
check("No row that breaks a HARD rule is in Silver", leaks == 0, f"{leaks} rows leaked")

# ------------------------------------------------------------------ C. Gold
print("\n=== C. GOLD (star schema in the lakehouse) ===")
fact = delta("gold/fact_trips")
fact_n = fact.count()
q_gold_n = delta("quarantine/quarantine_gold").count() if exists("quarantine/quarantine_gold") else 0
check("Silver = Fact + quarantine_gold", silver_n == fact_n + q_gold_n,
      f"{silver_n:,} = {fact_n:,} + {q_gold_n:,}")
for name, expected in [("dim_date", 730), ("dim_location", 265), ("dim_payment", 8), ("dim_taxi_type", 2)]:
    n = delta(f"gold/{name}").count()
    check(f"{name} has {expected} rows", n == expected, f"{n}")
check("Fact has no NULL key", fact.filter(
    "pickup_date_key is null or taxi_type_key is null or pu_location_key is null "
    "or do_location_key is null or payment_key is null").count() == 0)
daily_n = delta("gold/agg_trips_daily").agg(F.sum("trip_count")).first()[0]
hourly_n = delta("gold/agg_trips_hourly").agg(F.sum("trip_count")).first()[0]
check("agg_trips_daily trips = fact rows", daily_n == fact_n, f"{daily_n:,} vs {fact_n:,}")
check("agg_trips_hourly trips = fact rows", hourly_n == fact_n, f"{hourly_n:,} vs {fact_n:,}")

# ------------------------------------------------------------------ D. Audit
print("\n=== D. AUDIT TABLES (SQL Server) ===")
stuck = sql_rows("Select run_id, pipeline_name, started_at From audit.pipeline_runs Where status = 'RUNNING'")
check("No pipeline run is stuck in RUNNING", len(stuck) == 0, f"{len(stuck)} stuck")
for row in stuck:
    info(f"stuck run: {row[1]} started {row[2]} ({row[0]})")
latest = sql_rows("""
    Select r.pipeline_name, r.status From audit.pipeline_runs r
    Where r.started_at = (Select Max(started_at) From audit.pipeline_runs
                          Where pipeline_name = r.pipeline_name)""")
for name, status in latest:
    if not name.startswith("audit_connection"):
        check(f"Latest run of {name} succeeded", status == "SUCCESS", status)
bad_rows = sql_rows("""
    Select Count(*) From audit.step_logs
    Where layer_name In ('bronze', 'gold') And status = 'SUCCESS' And rows_read Is Not Null
      And rows_read <> rows_accepted + rows_quarantined""")[0][0]
check("Bronze and Gold steps: rows_read = accepted + quarantined", bad_rows == 0, f"{bad_rows} steps differ")
bad_silver = sql_rows("""
    Select Count(*) From audit.step_logs
    Where layer_name = 'silver' And status = 'SUCCESS' And rows_read Is Not Null
      And rows_read < rows_accepted + rows_quarantined""")[0][0]
check("Silver steps: rows_read >= accepted + quarantined", bad_silver == 0, f"{bad_silver} steps differ")
marks = {r[0] for r in sql_rows("Select source_table From audit.watermarks")}
check("Watermarks exist for Bronze (2 tables) and Silver",
      {"bronze.yellow_trips", "bronze.green_trips", "silver.trips"}.issubset(marks), f"{sorted(marks)}")

# ------------------------------------------------------------------ E. SQL Server serving layer
print("\n=== E. SERVING LAYER (SQL Server) versus the lakehouse ===")
for name in ("dim_date", "dim_location", "dim_payment", "dim_taxi_type", "agg_trips_daily", "agg_trips_hourly"):
    lake = delta(f"gold/{name}").count()
    sql = sql_rows(f"Select Count(*) From gold.{name}")[0][0]
    check(f"gold.{name}: SQL Server rows = lakehouse rows", lake == sql, f"{sql:,} vs {lake:,}")
sql_trips = sql_rows("Select Sum(trip_count) From gold.agg_trips_daily")[0][0]
check("SQL Server: total trips in agg_trips_daily = fact rows", sql_trips == fact_n, f"{sql_trips:,}")
lake_money = delta("gold/agg_trips_daily").agg(F.sum("total_sum")).first()[0]
sql_money = sql_rows("Select Sum(total_sum) From gold.agg_trips_daily")[0][0]
check("SQL Server: total money = lakehouse total money (difference under 1 dollar)",
      abs(lake_money - sql_money) < 1.0, f"{sql_money:,.2f} vs {lake_money:,.2f}")

# ------------------------------------------------------------------ summary
failed = results.count(False)
print(f"\nRESULT: {results.count(True)} passed, {failed} failed, out of {len(results)} checks")
spark.stop()
sys.exit(1 if failed else 0)