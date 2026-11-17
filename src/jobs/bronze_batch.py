"""
Bronze batch job: Gate 1 (file checks) + load into Bronze Delta tables.

For every monthly file listed by config/sources.yml:
  1. skip it if it is already in Bronze (incremental loading)
  2. Gate 1: can it be read? are the mandatory columns there? is it not empty?
  3. pass -> append to the Bronze table (raw columns + 3 metadata columns)
     fail -> one row in quarantine_bronze (the job continues with other files)
  4. write the result of the step to audit.step_logs

Run with:  docker compose run --rm spark python -u src/jobs/bronze_batch.py
"""

import logging
import os
import re
import time
from pathlib import Path

import yaml
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from common import audit

# ---- Settings (all come from environment variables or the config file) ----
RAW_PATH = Path(os.environ["RAW_DATA_PATH"])
LAKEHOUSE_PATH = os.environ["LAKEHOUSE_PATH"]
CONFIG_PATH = os.getenv("SOURCES_CONFIG", "/app/config/sources.yml")

BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"
QUARANTINE_PATH = f"{LAKEHOUSE_PATH}/quarantine/quarantine_bronze"

# Simple structured logging: every message is "event=... key=value ..."
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bronze_batch")


def build_spark():
    """Create a Spark session with Delta Lake enabled."""
    builder = (
        SparkSession.builder.appName("bronze_batch")
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Keep timestamps exactly as they are in the files: read them as normal
        # timestamps and use UTC so Spark never shifts the clock.
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def list_batch_files(folder, first_month, last_month):
    """Return the monthly files of one folder that are inside the batch range, sorted."""
    files = []
    for path in sorted((RAW_PATH / folder).glob("*.parquet")):
        match = re.search(r"(\d{4}-\d{2})\.parquet$", path.name)
        # Month strings like "2025-01" compare correctly as plain text
        if match and first_month <= match.group(1) <= last_month:
            files.append(path)
    return files


def already_loaded_files(spark, table_path):
    """Names of the files that are already inside the Bronze table (empty set if no table yet)."""
    if not DeltaTable.isDeltaTable(spark, table_path):
        return set()
    rows = spark.read.format("delta").load(table_path).select("source_file").distinct().collect()
    return {row["source_file"] for row in rows}


def gate1(spark, path, required_columns):
    """
    Gate 1: file-level checks. Returns (passed, reason, dataframe, row_count).
    Bronze must stay RAW, so we check only the file itself, not the values inside.
    """
    # Check 1: the file can be read at all
    try:
        df = spark.read.parquet(str(path))
        columns = df.columns          # reads only the file footer
    except Exception as error:
        return False, f"unreadable file: {type(error).__name__}", None, None

    # Check 2: every mandatory column is present
    missing = [c for c in required_columns if c not in columns]
    if missing:
        return False, f"missing mandatory columns: {missing}", None, None

    # Check 3: the file is not empty (count uses the footer, so it is fast)
    rows = df.count()
    if rows == 0:
        return False, "file has no rows", None, 0

    return True, None, df, rows


def quarantine_file(spark, taxi_type, file_name, reason, run_id):
    """Record a rejected file in quarantine_bronze."""
    schema = StructType([
        StructField("source_file", StringType()),
        StructField("taxi_type", StringType()),
        StructField("failure_reason", StringType()),
        StructField("run_id", StringType()),
    ])
    (spark.createDataFrame([(file_name, taxi_type, reason, run_id)], schema)
          .withColumn("quarantined_at", F.current_timestamp())
          .write.format("delta").mode("append").save(QUARANTINE_PATH))


def write_bronze(df, taxi_type, file_name, table_path):
    """Append one file to its Bronze table with the 3 metadata columns."""
    (df.withColumn("taxi_type", F.lit(taxi_type))
       .withColumn("source_file", F.lit(file_name))
       .withColumn("ingestion_timestamp", F.current_timestamp())
       .write.format("delta")
       .mode("append")
       # mergeSchema: a new column (request_source from 2026-06) is added to the table
       .option("mergeSchema", "true")
       # one partition per file: cheap to find loaded files and safe to reload one file
       .partitionBy("source_file")
       .save(table_path))


def process_file(spark, run_id, taxi_type, source, path, loaded_files):
    """Run the whole flow for one file and log the result to the audit table."""
    name = path.name
    table_path = f"{BRONZE_PATH}/{source['table']}"

    # Incremental loading: skip files that are already in Bronze
    if name in loaded_files:
        log.info("event=file_skipped file=%s reason=already_loaded", name)
        audit.log_step(run_id, "bronze", name, None, None, None, 0, "SKIPPED")
        return 0

    started = time.time()
    try:
        passed, reason, df, rows = gate1(spark, path, source["required_columns"])

        if not passed:
            quarantine_file(spark, taxi_type, name, reason, run_id)
            seconds = round(time.time() - started, 2)
            log.warning("event=file_quarantined file=%s reason=%s", name, reason)
            audit.log_step(run_id, "bronze", name, rows, 0, rows, seconds, "FAILED", reason)
            return 0

        write_bronze(df, taxi_type, name, table_path)
        seconds = round(time.time() - started, 2)
        log.info("event=file_loaded file=%s rows=%d seconds=%.1f", name, rows, seconds)
        audit.log_step(run_id, "bronze", name, rows, rows, 0, seconds, "SUCCESS")
        return rows

    except Exception as error:
        # An unexpected error is NOT the file's fault: log it and stop the run
        seconds = round(time.time() - started, 2)
        log.error("event=file_error file=%s error=%s", name, error)
        audit.log_step(run_id, "bronze", name, None, None, None, seconds,
                       "FAILED", str(error)[:1000])
        raise


def main():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    first = config["batch_months"]["first"]
    last = config["batch_months"]["last"]

    spark = build_spark()
    run_id = audit.start_run("bronze_batch")
    log.info("event=run_started run_id=%s months=%s..%s", run_id, first, last)

    try:
        for taxi_type, source in config["sources"].items():
            table_path = f"{BRONZE_PATH}/{source['table']}"
            loaded_files = already_loaded_files(spark, table_path)
            for path in list_batch_files(source["folder"], first, last):
                process_file(spark, run_id, taxi_type, source, path, loaded_files)
    except Exception:
        audit.finish_run(run_id, "FAILED")
        raise

    audit.finish_run(run_id, "SUCCESS")

    # Final reconciliation: Bronze totals must match the numbers from our profiling
    for taxi_type, source in config["sources"].items():
        total = (spark.read.format("delta")
                 .load(f"{BRONZE_PATH}/{source['table']}").count())
        verdict = "OK" if total == source["expected_rows"] else "MISMATCH"
        log.info("event=reconciliation table=%s rows=%d expected=%d result=%s",
                 source["table"], total, source["expected_rows"], verdict)

    log.info("event=run_finished run_id=%s status=SUCCESS", run_id)
    spark.stop()


if __name__ == "__main__":
    main()