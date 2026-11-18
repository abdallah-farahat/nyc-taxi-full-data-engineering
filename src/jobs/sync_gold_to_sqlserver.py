"""
Serving layer sync: copies the SMALL Gold tables (dimensions and aggregates) from the
lakehouse into SQL Server (schema gold). The big fact_trips table (83 million rows)
stays in the lakehouse on purpose: it would cost disk, time and money in SQL Server.

Full refresh on every run (these tables are small): delete, then insert.
Run with:  docker compose run --rm spark python -u src/jobs/sync_gold_to_sqlserver.py
"""

import logging
import os
import time

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from common import audit

LAKEHOUSE_PATH = os.environ["LAKEHOUSE_PATH"]
GOLD_PATH = f"{LAKEHOUSE_PATH}/gold"
JDBC_URL = (f"jdbc:sqlserver://{os.environ['SQLSERVER_HOST']}:{os.environ['SQLSERVER_PORT']};"
            f"databaseName={os.environ['SQLSERVER_DB']};encrypt=true;trustServerCertificate=true")

# Load order: dimensions first, because the aggregates have foreign keys to them.
# Each entry: (table name, select expressions). "year" and "month" get safer names.
TABLES = [
    ("dim_date", ["date_key", "full_date", "year AS calendar_year", "quarter AS quarter_number",
                  "month AS month_number", "month_name", "year_month", "day AS day_of_month",
                  "day_of_week", "day_name", "is_weekend", "week_of_year"]),
    ("dim_location", ["*"]),
    ("dim_payment", ["*"]),
    ("dim_taxi_type", ["*"]),
    ("agg_trips_daily", ["*"]),
    ("agg_trips_hourly", ["*"]),
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sync_gold")


def build_spark():
    builder = (
        SparkSession.builder.appName("sync_gold")
        .master("local[4]")
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


def sql_scalar(query):
    conn = audit.get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]
    finally:
        conn.close()


def main():
    spark = build_spark()
    run_id = audit.start_run("sync_gold_to_sqlserver")
    log.info("event=run_started run_id=%s", run_id)
    try:
        # Step 1: empty the tables, children (aggregates) first because of the foreign keys
        conn = audit.get_connection()
        cursor = conn.cursor()
        for name, _ in reversed(TABLES):
            cursor.execute(f"Delete From gold.{name}")
        conn.commit()
        conn.close()

        # Step 2: load them, parents (dimensions) first
        for name, expressions in TABLES:
            started = time.time()
            df = spark.read.format("delta").load(f"{GOLD_PATH}/{name}").selectExpr(*expressions)
            expected = df.count()
            (df.write.format("jdbc")
               .option("url", JDBC_URL)
               .option("dbtable", f"gold.{name}")
               .option("user", os.environ["SQLSERVER_USER"])
               .option("password", os.environ["SQLSERVER_PASSWORD"])
               .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
               .option("batchsize", "20000")
               .mode("append").save())
            loaded = sql_scalar(f"Select count(*) From gold.{name}")
            seconds = round(time.time() - started, 2)
            status = "SUCCESS" if loaded == expected else "FAILED"
            log.info("event=table_synced table=%s lake_rows=%d sql_rows=%d result=%s seconds=%.1f",
                     name, expected, loaded, status, seconds)
            audit.log_step(run_id, "serving", f"sync:{name}", expected, loaded,
                           expected - loaded, seconds, status,
                           None if status == "SUCCESS" else "row count mismatch")
            if status == "FAILED":
                raise RuntimeError(f"row count mismatch in {name}")
    except Exception:
        audit.finish_run(run_id, "FAILED")
        raise
    audit.finish_run(run_id, "SUCCESS")
    log.info("event=run_finished run_id=%s status=SUCCESS", run_id)
    spark.stop()


if __name__ == "__main__":
    main()