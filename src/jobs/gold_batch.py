"""
Gold job: Silver -> Gate 3 -> Gold star schema (Delta) + aggregates for the serving layer.

  dim_date, dim_location, dim_payment, dim_taxi_type   rebuilt every run (they are small)
  fact_trips        INCREMENTAL: Silver slices newer than the watermark, insert-only MERGE on trip_id
  agg_trips_daily   rebuilt from fact_trips every run (small output)
  agg_trips_hourly  rebuilt from fact_trips every run (small output)

Gate 3: a fact row whose key is missing in its dimension (or whose trip_id is empty)
goes to quarantine_gold with the reason.

Run with:  docker compose run --rm spark python -u src/jobs/gold_batch.py
"""

import logging
import os
import time
from pathlib import Path

import yaml
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import audit

RAW_PATH = Path(os.environ["RAW_DATA_PATH"])
LAKEHOUSE_PATH = os.environ["LAKEHOUSE_PATH"]
RULES_CONFIG = os.getenv("RULES_CONFIG", "/app/config/rules.yml")
WRITER_VERSION = os.getenv("GOLD_WRITER_VERSION", "v1")

SILVER_PATH = f"{LAKEHOUSE_PATH}/silver/trips"
GOLD_PATH = f"{LAKEHOUSE_PATH}/gold"
FACT_PATH = f"{GOLD_PATH}/fact_trips"
QUARANTINE_PATH = f"{LAKEHOUSE_PATH}/quarantine/quarantine_gold"
WATERMARK_KEY = "silver.trips"

DATE_START, DATE_END = "2025-01-01", "2026-12-31"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gold_batch")


def build_spark():
    builder = (
        SparkSession.builder.appName("gold_batch")
        .master("local[6]")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "8g"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "24")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ------------------------------------------------------------------ dimensions
def build_dim_date(spark):
    days = spark.sql(f"Select explode(sequence(to_date('{DATE_START}'), to_date('{DATE_END}'), "
                     f"interval 1 day)) As full_date")
    return days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.date_format("full_date", "yyyy-MM").alias("year_month"),
        F.dayofmonth("full_date").alias("day"),
        # Spark: 1 = Sunday ... 7 = Saturday. We want 1 = Monday ... 7 = Sunday.
        (((F.dayofweek("full_date") + 5) % 7) + 1).alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        F.weekofyear("full_date").alias("week_of_year"),
    )


def build_dim_taxi_type(spark):
    return spark.createDataFrame(
        [(1, "yellow", "Yellow taxi"), (2, "green", "Green taxi")],
        ["taxi_type_key", "taxi_type", "description"])


def build_dim_payment(spark):
    return spark.createDataFrame(
        [(-1, "Not recorded", False), (0, "Flex Fare", False), (1, "Credit card", True),
         (2, "Cash", False), (3, "No charge", False), (4, "Dispute", False),
         (5, "Unknown", False), (6, "Voided trip", False)],
        ["payment_key", "payment_name", "tip_is_recorded"])


def build_dim_location(spark, rules):
    zones = spark.read.csv(str(RAW_PATH / rules["lookup_file"]), header=True, inferSchema=True)
    return zones.select(F.col("LocationID").cast("int").alias("location_key"),
                        F.col("Borough").alias("borough"),
                        F.col("Zone").alias("zone"),
                        F.col("service_zone").alias("service_zone"))


def write_dim(df, name):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
      .save(f"{GOLD_PATH}/{name}")


# ------------------------------------------------------------------ fact and Gate 3
def build_fact(df):
    """Turn Silver rows into fact rows: keys + measures + flags."""
    return df.select(
        "trip_id",
        F.date_format("pickup_datetime", "yyyyMMdd").cast("int").alias("pickup_date_key"),
        F.hour("pickup_datetime").alias("pickup_hour"),
        F.date_format("dropoff_datetime", "yyyyMMdd").cast("int").alias("dropoff_date_key"),
        F.when(F.col("taxi_type") == "yellow", 1)
         .when(F.col("taxi_type") == "green", 2).alias("taxi_type_key"),
        F.col("pu_location_id").alias("pu_location_key"),
        F.col("do_location_id").alias("do_location_key"),
        F.coalesce(F.col("payment_type"), F.lit(-1)).alias("payment_key"),
        "vendor_id", "ratecode_id", "trip_type", "request_source",
        "pickup_datetime", "dropoff_datetime",
        "passenger_count", "trip_distance", "trip_duration_min", "speed_mph",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "congestion_surcharge", "airport_fee",
        "cbd_congestion_fee", "total_amount", "total_amount_diff",
        "is_flex_fare", "is_store_and_forward", "is_tip_recorded",
        "has_negative_amount", "data_quality_flags",
        "pickup_month", "ingestion_timestamp",
    )


def not_in(column, values):
    """True when the key is missing from the dimension (a NULL key counts as missing)."""
    return ~F.coalesce(F.col(column).isin(values), F.lit(False))


def gate3(fact, date_keys, location_keys, payment_keys):
    """Add quarantine_reason: the list of Gate 3 rules the row breaks (empty = passes)."""
    rules = {
        "trip_id_is_null": F.col("trip_id").isNull(),
        "pickup_date_not_in_dim_date": not_in("pickup_date_key", date_keys),
        "dropoff_date_not_in_dim_date": not_in("dropoff_date_key", date_keys),
        "taxi_type_not_in_dim_taxi_type": not_in("taxi_type_key", [1, 2]),
        "pu_location_not_in_dim_location": not_in("pu_location_key", location_keys),
        "do_location_not_in_dim_location": not_in("do_location_key", location_keys),
        "payment_not_in_dim_payment": not_in("payment_key", payment_keys),
    }
    reasons = F.concat_ws(";", *[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    return fact.withColumn("quarantine_reason", reasons)


def process_slice(spark, run_id, silver, ts, mark, keys):
    started = time.time()
    # Equality on the column lets Delta skip every file that does not match
    incoming = silver.filter(F.col("ingestion_timestamp") == F.lit(ts))
    incoming_count = incoming.count()

    checked = gate3(build_fact(incoming), *keys).persist(StorageLevel.MEMORY_AND_DISK)
    is_bad = F.col("quarantine_reason") != ""
    bad_count = checked.filter(is_bad).count()
    good_count = incoming_count - bad_count

    good = checked.filter(~is_bad).drop("quarantine_reason")
    if good_count:
        if not DeltaTable.isDeltaTable(spark, FACT_PATH):
            good.write.format("delta").mode("append").partitionBy("pickup_month").save(FACT_PATH)
        else:
            # Insert-only MERGE: a trip_id that already exists is skipped (unique keys)
            months = [r[0] for r in good.select("pickup_month").distinct().collect()]
            month_list = ",".join(f"'{m}'" for m in months)
            (DeltaTable.forPath(spark, FACT_PATH).alias("t")
                .merge(good.alias("s"),
                       f"t.trip_id = s.trip_id AND t.pickup_month IN ({month_list})")
                .whenNotMatchedInsertAll().execute())
    if bad_count:
        (checked.filter(is_bad)
            .select("trip_id", "pickup_datetime", "taxi_type_key", "pu_location_key",
                    "do_location_key", "payment_key", "quarantine_reason")
            .withColumn("run_id", F.lit(run_id))
            .withColumn("quarantined_at", F.current_timestamp())
            .write.format("delta").mode("append")
            .option("txnAppId", f"quarantine_gold_{WRITER_VERSION}")
            .option("txnVersion", mark)
            .option("mergeSchema", "true").save(QUARANTINE_PATH))

    audit.set_watermark(WATERMARK_KEY, mark)
    seconds = round(time.time() - started, 2)
    log.info("event=gold_slice slice=%s read=%d accepted=%d quarantined=%d seconds=%.1f",
             str(ts)[:19], incoming_count, good_count, bad_count, seconds)
    audit.log_step(run_id, "gold", f"gold:fact:{str(ts)[:19]}", incoming_count,
                   good_count, bad_count, seconds, "SUCCESS")
    checked.unpersist()


# ------------------------------------------------------------------ aggregates
def build_aggregates(spark, run_id):
    started = time.time()
    fact = spark.read.format("delta").load(FACT_PATH)
    # Money measures use only trips WITHOUT negative amounts (refunds and disputes are
    # counted separately in the trip counts).
    positive = ~F.coalesce(F.col("has_negative_amount"), F.lit(False))

    def money(column, condition=None):
        cond = positive if condition is None else (positive & condition)
        return F.sum(F.when(cond, F.col(column)).otherwise(0.0))

    card = F.coalesce(F.col("is_tip_recorded"), F.lit(False))
    daily = fact.groupBy("pickup_date_key", "taxi_type_key", "pu_location_key", "payment_key").agg(
        F.count("*").alias("trip_count"),
        F.sum(positive.cast("int")).alias("positive_trip_count"),
        F.sum(F.col("data_quality_flags").isNotNull().cast("int")).alias("flagged_trip_count"),
        money("trip_distance").alias("distance_sum"),
        money("trip_duration_min").alias("duration_min_sum"),
        money("fare_amount").alias("fare_sum"),
        money("total_amount").alias("total_sum"),
        money("tolls_amount").alias("tolls_sum"),
        money("congestion_surcharge").alias("congestion_surcharge_sum"),
        money("cbd_congestion_fee").alias("cbd_fee_sum"),
        F.sum((positive & card).cast("int")).alias("card_trip_count"),
        money("tip_amount", card).alias("card_tip_sum"),
    )
    daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
         .save(f"{GOLD_PATH}/agg_trips_daily")

    hourly = fact.groupBy("pickup_date_key", "pickup_hour", "taxi_type_key").agg(
        F.count("*").alias("trip_count"),
        F.sum(positive.cast("int")).alias("positive_trip_count"),
        money("total_amount").alias("total_sum"),
        money("trip_distance").alias("distance_sum"),
        money("trip_duration_min").alias("duration_min_sum"),
    )
    hourly.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
          .save(f"{GOLD_PATH}/agg_trips_hourly")

    seconds = round(time.time() - started, 2)
    log.info("event=aggregates_done seconds=%.1f", seconds)
    audit.log_step(run_id, "gold", "gold:aggregates", None, None, None, seconds, "SUCCESS")


def main():
    with open(RULES_CONFIG) as f:
        rules = yaml.safe_load(f)

    spark = build_spark()
    run_id = audit.start_run("gold_batch")
    log.info("event=run_started run_id=%s", run_id)
    try:
        # Dimensions: small, rebuilt every run
        dim_date = build_dim_date(spark)
        dim_location = build_dim_location(spark, rules)
        dim_payment = build_dim_payment(spark)
        for name, df in [("dim_date", dim_date), ("dim_location", dim_location),
                         ("dim_payment", dim_payment),
                         ("dim_taxi_type", build_dim_taxi_type(spark))]:
            write_dim(df, name)
        keys = (
            [r[0] for r in dim_date.select("date_key").collect()],
            [r[0] for r in dim_location.select("location_key").collect()],
            [r[0] for r in dim_payment.select("payment_key").collect()],
        )
        log.info("event=dimensions_written")

        # Fact: incremental, one slice at a time
        silver = spark.read.format("delta").load(SILVER_PATH)
        micros = F.unix_micros("ingestion_timestamp")
        old_mark = audit.get_watermark(WATERMARK_KEY)
        new_rows = silver if old_mark is None else silver.filter(micros > old_mark)
        slices = (new_rows.select("ingestion_timestamp", micros.alias("mark"))
                          .distinct().orderBy("mark").collect())
        log.info("event=gold_start slices=%d", len(slices))
        for row in slices:
            process_slice(spark, run_id, silver, row["ingestion_timestamp"], row["mark"], keys)

        build_aggregates(spark, run_id)
    except Exception:
        audit.finish_run(run_id, "FAILED")
        raise
    audit.finish_run(run_id, "SUCCESS")
    log.info("event=run_finished run_id=%s status=SUCCESS", run_id)
    spark.stop()


if __name__ == "__main__":
    main()