"""
Producer: reads trips from the REST API and sends them to Kafka, one message per trip.

SAFE BY DEFAULT: if the topic already holds messages, the producer STOPS instead of
guessing where to continue (continuing from a message count can send duplicates when a
previous run was interrupted). Use RESUME=true only if you know what you are doing.

Settings (environment variables, with defaults):
  MAX_ROWS         rows per taxi type to send (0 = all)
  PAGE_SIZE        rows per API request
  ROWS_PER_SECOND  throttle (0 = as fast as possible)
  TAXI_TYPES       which taxi types to send (default: green,yellow)
  RESUME           true = continue after the messages already in the topic

Example:  docker compose run --rm -e MAX_ROWS=20000 spark python -u src/streaming/producer.py
"""

import json
import logging
import os
import time

import requests
from confluent_kafka import Consumer, Producer, TopicPartition

API_URL = os.getenv("API_URL", "http://api:8000")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "5000"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "0"))
ROWS_PER_SECOND = int(os.getenv("ROWS_PER_SECOND", "0"))
TAXI_TYPES = os.getenv("TAXI_TYPES", "green,yellow").split(",")
RESUME = os.getenv("RESUME", "false").lower() == "true"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("producer")


def messages_in_topic(topic):
    """Count the messages already stored in a topic (sum over its partitions)."""
    consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "offset-probe"})
    partitions = consumer.list_topics(topic, timeout=15).topics[topic].partitions
    total = 0
    for partition in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=15)
        total += high - low
    consumer.close()
    return total


def send_taxi_type(producer, taxi_type):
    topic = f"taxi.{taxi_type}.trips"
    existing = messages_in_topic(topic)
    # How many trips does the API hold? (limit=1 returns just the total)
    available = requests.get(f"{API_URL}/trips/{taxi_type}",
                             params={"offset": 0, "limit": 1}, timeout=60).json()["total"]
    target = available if MAX_ROWS == 0 else min(MAX_ROWS, available)

    # Already complete: nothing to do (this makes the whole pipeline safe to re-run)
    if existing == target and not RESUME:
        log.info("event=producer_skipped taxi=%s reason=topic_already_complete messages=%d",
                 taxi_type, existing)
        return
    # Partly filled: we cannot know where to continue without risking duplicates
    if existing and not RESUME:
        raise SystemExit(
            f"STOP: topic {topic} holds {existing:,} messages but should hold {target:,}. "
            f"Delete and recreate the topic (or run run_pipeline.ps1 -Reset).")
    offset = existing if RESUME else 0
    sent = 0
    started = time.time()
    log.info("event=producer_start taxi=%s topic=%s start_offset=%d", taxi_type, topic, offset)

    while True:
        # Never ask for more rows than MAX_ROWS allows
        limit = PAGE_SIZE if MAX_ROWS == 0 else min(PAGE_SIZE, MAX_ROWS - offset)
        if limit <= 0:
            break
        response = requests.get(f"{API_URL}/trips/{taxi_type}",
                                params={"offset": offset, "limit": limit}, timeout=120)
        response.raise_for_status()
        page = response.json()
        rows = page["rows"]
        if not rows:
            break                               # the API has no more trips

        for index, row in enumerate(rows):
            # The key (row number) spreads the messages over the partitions
            producer.produce(topic, key=str(offset + index), value=json.dumps(row))
            producer.poll(0)                    # serve delivery callbacks
        producer.flush()                        # wait until the whole page is safely in Kafka

        offset += len(rows)
        sent += len(rows)
        if ROWS_PER_SECOND:
            time.sleep(len(rows) / ROWS_PER_SECOND)
        if offset >= page["total"]:
            break

    seconds = max(time.time() - started, 0.001)
    log.info("event=producer_done taxi=%s sent=%d seconds=%.1f rows_per_second=%d",
             taxi_type, sent, seconds, sent / seconds)


if __name__ == "__main__":
    kafka_producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "enable.idempotence": True,             # no duplicates caused by network retries
        "linger.ms": 20,                        # small batching delay: much faster sending
        "compression.type": "lz4",
    })
    for taxi in TAXI_TYPES:
        send_taxi_type(kafka_producer, taxi)