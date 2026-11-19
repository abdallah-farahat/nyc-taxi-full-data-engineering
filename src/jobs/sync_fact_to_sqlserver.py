"""
Loads gold.fact_trips (trip level, about 83 million rows) from the lakehouse into SQL Server.

Month by month, so it is RESUMABLE and INCREMENTAL:
  - a month whose row count already matches the lakehouse is skipped
  - a month that is missing or incomplete is deleted from SQL Server and loaded again
Optional: MONTHS=2025-01,2025-02 loads only those months (use it for a first small test).

The 64-character trip_id is stored in SQL Server as trip_key: its first 128 bits
(32 hex characters) as 16 bytes. This keeps the columnstore small. The full trip_id
stays in the lakehouse.

Run with:  docker compose run --rm spark python -u src/jobs/sync_fact_to_sqlserver.py
"""

import logging
import os
import time

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import audit

LAKEHOUSE_PATH = os.environ["LAKEHOUSE_PATH"]
# useBulkCopyForBatchInsert makes the JDBC driver use the fast bulk-copy API
JDBC_URL = (f"jdbc:sqlserver://{os.environ['SQLSERVER_HOST']}:{os.environ['SQLSERVER_PORT']};"
            f"databaseName={os.environ['SQLSERVER_DB']};encrypt=true;trustServerCertificate=true;"
            f"useBulkCopyForBatchInsert=true")
ONLY_MONTHS = [m for m in os.getenv("MONTHS", "").split(",") if m]

# Columns copied as they are (trip_id is NOT here: it becomes trip_key below)
COLUMNS = [
    "pickup_date_key", "pickup_hour", "dropoff_date_key", "taxi_type_key",
    "pu_location_key", "do_location_key", "payment_key", "vendor_id", "ratecode_id",
    "trip_type", "request_source", "pickup_datetime", "dropoff_datetime", "pickup_month",
    "passenger_count", "trip_distance", "trip_duration_min", "speed_mph", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
    "congestion_surcharge", "airport_fee", "cbd_congestion_fee", "total_amount",
    "total_amount_diff", "is_flex_fare", "is_store_and_forward", "is_tip_recorded",
    "has_negative_amount", "data_quality_flags",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync_fact")


def build_spark():
    builder = (
        SparkSession.builder.appName("sync_fact")
        .master("local[6]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "8g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def sql_run(query, params=()):
    """Run one statement in SQL Server and return all rows (empty list for DML)."""
    conn = audit.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall() if cursor.description else []
        conn.commit()
        return rows
    finally:
        conn.close()

def sql_maintenance(statement):
    """Run an index maintenance statement outside a transaction (autocommit)."""
    conn = audit.get_connection()
    try:
        conn.autocommit(True)
        conn.cursor().execute(statement)
    finally:
        conn.close()

def main():
    spark = build_spark()
    run_id = audit.start_run("sync_fact_to_sqlserver")
    fact = spark.read.format("delta").load(f"{LAKEHOUSE_PATH}/gold/fact_trips")
    lake = {r["pickup_month"]: r["count"] for r in fact.groupBy("pickup_month").count().collect()}
    in_sql = dict(sql_run("Select pickup_month, Count(*) From gold.fact_trips Group By pickup_month"))
    months = sorted(m for m in lake if not ONLY_MONTHS or m in ONLY_MONTHS)
    log.info("event=run_started run_id=%s months=%d", run_id, len(months))

    deleted_something = False
    try:
        for month in months:
            started = time.time()
            if in_sql.get(month) == lake[month]:
                log.info("event=month_skipped month=%s reason=already_loaded rows=%d", month, lake[month])
                audit.log_step(run_id, "serving", f"fact:{month}", lake[month],
                               0, 0, 0, "SKIPPED")
                continue
            if in_sql.get(month):
                sql_run("Delete From gold.fact_trips Where pickup_month = %s", (month,))
                deleted_something = True
            (fact.filter(f"pickup_month = '{month}'")
                 .select(F.unhex(F.substring("trip_id", 1, 32)).alias("trip_key"), *COLUMNS)
                 .coalesce(2)
                 .write.format("jdbc")
                 .option("url", JDBC_URL)
                 .option("dbtable", "gold.fact_trips")
                 .option("user", os.environ["SQLSERVER_USER"])
                 .option("password", os.environ["SQLSERVER_PASSWORD"])
                 .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                 .option("batchsize", "50000")
                 .mode("append").save())
            loaded = sql_run("Select Count(*) From gold.fact_trips Where pickup_month = %s", (month,))[0][0]
            seconds = round(time.time() - started, 1)
            ok = loaded == lake[month]
            log.info("event=month_loaded month=%s lake_rows=%d sql_rows=%d result=%s seconds=%.1f",
                     month, lake[month], loaded, "SUCCESS" if ok else "FAILED", seconds)
            audit.log_step(run_id, "serving", f"fact:{month}", lake[month], loaded,
                           lake[month] - loaded, seconds, "SUCCESS" if ok else "FAILED",
                           None if ok else "row count mismatch")
            if not ok:
                raise RuntimeError(f"row count mismatch for {month}")
            # JDBC inserts first land in an uncompressed delta store. Compressing after every
            # month keeps the table (and your disk) small during the whole load.
            sql_maintenance("Alter Index cci_fact_trips On gold.fact_trips "
                            "Reorganize With (Compress_All_Row_Groups = On)")
        if deleted_something:
            # Deleted rows stay hidden in the columnstore until it is reorganized
            sql_maintenance("Alter Index cci_fact_trips On gold.fact_trips "
                            "Reorganize With (Compress_All_Row_Groups = On)")
    except Exception:
        audit.finish_run(run_id, "FAILED")
        raise
    audit.finish_run(run_id, "SUCCESS")
    log.info("event=run_finished run_id=%s status=SUCCESS", run_id)
    spark.stop()


if __name__ == "__main__":
    main()