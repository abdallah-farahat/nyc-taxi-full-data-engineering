"""
Streaming Bronze job: Kafka -> Gate 1 -> Bronze (Delta).

For every micro-batch of messages:
  - Gate 1: the message must be valid JSON with pickup and dropoff datetimes
  - pass -> appended to the same Bronze table the batch job uses
  - fail -> quarantine_bronze (raw message, partition and offset are kept)
  - the result is written to audit.step_logs

Usage:
  python -u src/jobs/stream_bronze.py           # continuous (stop with Ctrl+C)
  python -u src/jobs/stream_bronze.py --once    # process what is in Kafka now, then stop
"""

import argparse
import logging
import os
import time

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from common import audit

LAKEHOUSE_PATH = os.environ["LAKEHOUSE_PATH"]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
STREAM_MONTH = os.getenv("STREAM_MONTH", "2026-09")
CONFIG_PATH = os.getenv("SOURCES_CONFIG", "/app/config/sources.yml")
MAX_OFFSETS = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "300000"))   # messages per micro-batch

BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"
QUARANTINE_PATH = f"{LAKEHOUSE_PATH}/quarantine/quarantine_bronze"
CHECKPOINT_ROOT = f"{LAKEHOUSE_PATH}/_checkpoints"
METADATA_COLUMNS = {"taxi_type", "source_file", "ingestion_timestamp"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("stream_bronze")


def build_spark():
    """Spark session with Delta Lake and the Kafka connector."""
    builder = (
        SparkSession.builder.appName("stream_bronze")
        .master("local[*]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")   # small value: fast micro-batches
    )
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def make_batch_handler(spark, run_id, taxi_type, source, schema):
    """Create the function Spark calls for every micro-batch of one taxi type."""
    table_path = f"{BRONZE_PATH}/{source['table']}"
    topic = f"taxi.{taxi_type}.trips"
    source_file = f"stream_{taxi_type}_{STREAM_MONTH}"
    pickup = next(c for c in source["required_columns"] if c.endswith("pickup_datetime"))
    dropoff = next(c for c in source["required_columns"] if c.endswith("dropoff_datetime"))

    def handle(batch_df, batch_id):
        started = time.time()
        # Parse the JSON text of each message with the Bronze schema
        parsed = (batch_df.select(
                      F.col("value").cast("string").alias("raw_message"),
                      F.col("partition").alias("kafka_partition"),
                      F.col("offset").alias("kafka_offset"))
                  .withColumn("data", F.from_json("raw_message", schema))
                  .persist())

        read_count = parsed.count()
        if read_count == 0:
            parsed.unpersist()
            return

        # Gate 1: parsable JSON and both datetimes present
        is_valid = (F.col(f"data.{pickup}").isNotNull()
                    & F.col(f"data.{dropoff}").isNotNull())
        good = parsed.filter(is_valid)
        bad = parsed.filter(~is_valid)
        accepted = good.count()
        quarantined = read_count - accepted

        if accepted:
            (good.select("data.*")
                 .withColumn("taxi_type", F.lit(taxi_type))
                 .withColumn("source_file", F.lit(source_file))
                 .withColumn("ingestion_timestamp", F.current_timestamp())
                 .write.format("delta").mode("append")
                 .option("mergeSchema", "true")
                 .partitionBy("source_file")
                 .save(table_path))

        if quarantined:
            (bad.select("raw_message", "kafka_partition", "kafka_offset")
                .withColumn("source_file", F.lit(source_file))
                .withColumn("taxi_type", F.lit(taxi_type))
                .withColumn("failure_reason", F.lit(
                    "Gate 1 failed: invalid JSON or missing pickup/dropoff datetime"))
                .withColumn("run_id", F.lit(run_id))
                .withColumn("quarantined_at", F.current_timestamp())
                .write.format("delta").mode("append")
                .option("mergeSchema", "true")
                .save(QUARANTINE_PATH))

        seconds = round(time.time() - started, 2)
        log.info("event=micro_batch topic=%s batch=%d read=%d accepted=%d quarantined=%d seconds=%.1f",
                 topic, batch_id, read_count, accepted, quarantined, seconds)
        audit.log_step(run_id, "bronze", f"stream:{topic}:batch{batch_id}",
                       read_count, accepted, quarantined, seconds, "SUCCESS")
        parsed.unpersist()

    return handle


def main(once):
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)

    spark = build_spark()
    run_id = audit.start_run("stream_bronze")
    log.info("event=stream_started run_id=%s once=%s", run_id, once)

    queries = []
    for taxi_type, source in config["sources"].items():
        table_path = f"{BRONZE_PATH}/{source['table']}"
        # The message schema = the Bronze table schema without our 3 metadata columns
        full_schema = spark.read.format("delta").load(table_path).schema
        schema = StructType([f for f in full_schema.fields if f.name not in METADATA_COLUMNS])

        stream = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                  .option("subscribe", f"taxi.{taxi_type}.trips")
                  .option("startingOffsets", "earliest")
                  .option("maxOffsetsPerTrigger", MAX_OFFSETS)
                  .option("failOnDataLoss", "false")
                  .load())

        # availableNow = process everything that is in Kafka, then stop
        trigger = {"availableNow": True} if once else {"processingTime": "5 seconds"}
        queries.append(
            stream.writeStream
            .foreachBatch(make_batch_handler(spark, run_id, taxi_type, source, schema))
            # The checkpoint remembers which Kafka offsets are done (no re-reading)
            .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_stream_{taxi_type}")
            .trigger(**trigger)
            .start())

    try:
        for query in queries:
            query.awaitTermination()
        audit.finish_run(run_id, "SUCCESS")
    except KeyboardInterrupt:
        for query in queries:
            query.stop()
        audit.finish_run(run_id, "SUCCESS")
    except Exception:
        audit.finish_run(run_id, "FAILED")
        raise
    log.info("event=stream_finished run_id=%s", run_id)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true",
                        help="process the messages already in Kafka, then stop")
    main(parser.parse_args().once)