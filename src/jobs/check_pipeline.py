"""
Pipeline health check (read-only).
1) messages stored in each Kafka topic
2) Bronze rows: batch part and stream part
3) reconciliation: stream rows + quarantined rows must equal the Kafka messages
Run with:  docker compose run --rm spark python -u src/jobs/check_pipeline.py
"""

import os

import yaml
from confluent_kafka import Consumer, TopicPartition
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

L = os.environ["LAKEHOUSE_PATH"]
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")


def kafka_count(topic):
    """Number of messages stored in a topic (sum over partitions)."""
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "check-probe"})
    partitions = consumer.list_topics(topic, timeout=15).topics[topic].partitions
    total = 0
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)
        total += high - low
    consumer.close()
    return total


builder = (SparkSession.builder.master("local[*]")
           .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

with open("/app/config/sources.yml") as f:
    sources = yaml.safe_load(f)["sources"]

quarantine_path = f"{L}/quarantine/quarantine_bronze"
quarantine = (spark.read.format("delta").load(quarantine_path)
              if DeltaTable.isDeltaTable(spark, quarantine_path) else None)

for taxi, source in sources.items():
    df = spark.read.format("delta").load(f"{L}/bronze/{source['table']}")
    stream = df.filter("source_file like 'stream_%'")
    batch_rows = df.filter("source_file not like 'stream_%'").count()
    stream_rows = stream.count()
    # Exact copies inside the stream part (all raw columns equal)
    raw_columns = [c for c in df.columns if c not in ("ingestion_timestamp", "source_file")]
    stream_distinct = stream.dropDuplicates(raw_columns).count()
    stream_quarantined = (quarantine.filter(f"source_file like 'stream_{taxi}%'").count()
                          if quarantine is not None else 0)
    kafka_rows = kafka_count(f"taxi.{taxi}.trips")
    difference = stream_rows + stream_quarantined - kafka_rows

    print(f"\n=== {taxi.upper()} ===")
    print(f"batch rows in Bronze        : {batch_rows:>12,}   (expected {source['expected_rows']:,})")
    print(f"messages in Kafka           : {kafka_rows:>12,}")
    print(f"stream rows in Bronze       : {stream_rows:>12,}")
    print(f"stream rows in quarantine   : {stream_quarantined:>12,}")
    print(f"exact copies in stream rows : {stream_rows - stream_distinct:>12,}")
    verdict = ("OK" if difference == 0 else
               f"DUPLICATES (+{difference:,})" if difference > 0 else
               f"MISSING ({difference:,}); the stream job has not processed everything")
    print(f"reconciliation (stream + quarantine - Kafka) = {difference:,}  ->  {verdict}")

checkpoints = f"{L}/_checkpoints"
print("\ncheckpoints:", sorted(os.listdir(checkpoints)) if os.path.isdir(checkpoints) else "none")
spark.stop()