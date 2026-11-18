"""
Silver job: Bronze -> Gate 2 -> Silver (+ quarantine_silver).

The job works on SLICES: one slice = the rows written to Bronze at one ingestion time
(one batch file or one streaming micro-batch). Small slices keep memory low and make the
job RESUMABLE: the watermark moves forward after every slice, so after a crash a new run
continues from the last finished slice.

For each slice:
  1. unify Yellow and Green into one schema and add calculated columns
  2. remove exact duplicates (same trip_id)
  3. Gate 2: hard rules -> quarantine_silver, soft rules -> flags in data_quality_flags
  4. good rows -> Silver (first slice creates the table, later slices use an insert-only MERGE)
  5. save the watermark and write the result to audit.step_logs

Run with:  docker compose run --rm spark python -u src/jobs/silver_batch.py
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
SOURCES_CONFIG = os.getenv("SOURCES_CONFIG", "/app/config/sources.yml")
RULES_CONFIG = os.getenv("RULES_CONFIG", "/app/config/rules.yml")
# Change this ONLY when you delete Silver and rebuild it from scratch
WRITER_VERSION = os.getenv("SILVER_WRITER_VERSION", "v1")

BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze"
SILVER_PATH = f"{LAKEHOUSE_PATH}/silver/trips"
QUARANTINE_PATH = f"{LAKEHOUSE_PATH}/quarantine/quarantine_silver"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("silver_batch")

# Columns that identify a trip. trip_id is a hash of all of them, so two rows
# with the same trip_id are exact duplicates.
BUSINESS_COLUMNS = [
    "taxi_type", "vendor_id", "pickup_datetime", "dropoff_datetime", "passenger_count",
    "trip_distance", "ratecode_id", "store_and_fwd_flag", "pu_location_id",
    "do_location_id", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge",
    "airport_fee", "cbd_congestion_fee", "trip_type", "request_source",
]


def build_spark():
    builder = (
        SparkSession.builder.appName("silver_batch")
        .master("local[6]")                               # 6 threads: lower memory pressure
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


def col_or_null(df, name, dtype):
    """The column if it exists, otherwise a NULL column of the right type."""
    return (F.col(name) if name in df.columns else F.lit(None)).cast(dtype)


def unify(df, taxi_type):
    """Give Yellow and Green the same column names and types."""
    prefix = "tpep" if taxi_type == "yellow" else "lpep"
    return df.select(
        F.col("taxi_type"),
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col(f"{prefix}_pickup_datetime").alias("pickup_datetime"),
        F.col(f"{prefix}_dropoff_datetime").alias("dropoff_datetime"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("ratecode_id"),
        F.col("store_and_fwd_flag").alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("int").alias("pu_location_id"),
        F.col("DOLocationID").cast("int").alias("do_location_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        col_or_null(df, "Airport_fee", "double").alias("airport_fee"),      # Yellow only
        F.col("cbd_congestion_fee").cast("double").alias("cbd_congestion_fee"),
        col_or_null(df, "trip_type", "int").alias("trip_type"),             # Green only
        col_or_null(df, "request_source", "string").alias("request_source"),
        F.col("source_file"),
        F.col("ingestion_timestamp"),
        # ehail_fee is dropped on purpose: it is 100% empty
    )


def add_calculated_columns(df):
    """trip_id and the calculated helper columns."""
    hashed = F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in BUSINESS_COLUMNS]), 256)
    duration_sec = F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")
    money = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
             "improvement_surcharge", "total_amount",
             F.coalesce("congestion_surcharge", F.lit(0.0)),
             F.coalesce("cbd_congestion_fee", F.lit(0.0))]
    parts_sum = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in
                    ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                     "improvement_surcharge", "congestion_surcharge",
                     "cbd_congestion_fee", "airport_fee"])
    return (df
        .withColumn("trip_id", hashed)
        .withColumn("pickup_month", F.date_format("pickup_datetime", "yyyy-MM"))
        .withColumn("duration_sec", duration_sec)
        .withColumn("trip_duration_min", F.round(F.col("duration_sec") / 60.0, 2))
        .withColumn("speed_mph", F.when(F.col("duration_sec") > 0,
                    F.round(F.col("trip_distance") / (F.col("duration_sec") / 3600.0), 2)))
        .withColumn("is_flex_fare", F.coalesce(F.col("payment_type") == 0, F.lit(False)))
        .withColumn("is_store_and_forward",
                    F.when(F.col("store_and_fwd_flag") == "Y", True)
                     .when(F.col("store_and_fwd_flag") == "N", False))
        .withColumn("is_tip_recorded", F.coalesce(F.col("payment_type") == 1, F.lit(False)))
        .withColumn("has_negative_amount", F.least(*money) < 0)
        .withColumn("total_amount_diff",
                    F.round(F.col("total_amount") - parts_sum, 2)))


def gate2(df, rules, zone_ids, airport_ids):
    """Add quarantine_reason (hard rules) and data_quality_flags (soft rules)."""
    hard, soft = rules["hard_limits"], rules["soft_limits"]
    codes = rules["allowed_codes"]
    start = F.lit(rules["project_range"]["start"]).cast("timestamp")
    end = F.lit(rules["project_range"]["end_exclusive"]).cast("timestamp")
    cbd_start = F.lit(rules["cbd_fee_start_date"]).cast("timestamp")
    c = F.col

    hard_rules = {
        "pickup_is_null": c("pickup_datetime").isNull(),
        "dropoff_is_null": c("dropoff_datetime").isNull(),
        "dropoff_before_pickup": c("duration_sec") < 0,
        "pickup_outside_project_range":
            (c("pickup_datetime") < start) | (c("pickup_datetime") >= end),
        "duration_over_max": c("duration_sec") > hard["max_duration_hours"] * 3600,
        "passenger_count_over_max": c("passenger_count") > hard["max_passenger_count"],
        "distance_over_max": c("trip_distance") > hard["max_trip_distance_miles"],
        "fare_over_max": c("fare_amount") > hard["max_fare_amount"],
        "speed_over_max": c("speed_mph") > hard["max_speed_mph"],
        "total_amount_is_null": c("total_amount").isNull(),
    }
    file_month = F.regexp_extract("source_file", r"(\d{4}-\d{2})", 1)
    soft_rules = {
        "dropoff_equals_pickup": c("duration_sec") == 0,
        "duration_over_3h": c("duration_sec") > soft["long_duration_hours"] * 3600,
        "duration_under_1min": (c("duration_sec") > 0) & (c("duration_sec") < soft["short_duration_seconds"]),
        "passenger_count_zero": c("passenger_count") == 0,
        "unexpected_null_passenger_count":
            c("passenger_count").isNull() & ~c("is_flex_fare") & c("payment_type").isNotNull(),
        "distance_zero": c("trip_distance") == 0,
        "distance_over_50": c("trip_distance") > soft["long_distance_miles"],
        "fare_zero": c("fare_amount") == 0,
        "fare_over_300": c("fare_amount") > soft["high_fare_amount"],
        "speed_over_80": c("speed_mph") > soft["high_speed_mph"],
        "pickup_month_differs_from_file": c("pickup_month") != file_month,
        "negative_amount": c("has_negative_amount"),
        "cash_with_tip": (c("payment_type") == 2) & (c("tip_amount") > 0),
        "cbd_fee_before_start": (c("cbd_congestion_fee") > 0) & (c("pickup_datetime") < cbd_start),
        "airport_fee_outside_airport_zone":
            (c("airport_fee") > 0) & ~c("pu_location_id").isin(airport_ids),
        "code_outside_allowed_list":
            ~c("vendor_id").isin(codes["vendor_id"]) | ~c("ratecode_id").isin(codes["ratecode_id"])
            | ~c("payment_type").isin(codes["payment_type"]) | ~c("trip_type").isin(codes["trip_type"]),
        "zone_not_in_lookup":
            ~c("pu_location_id").isin(zone_ids) | ~c("do_location_id").isin(zone_ids),
    }
    # concat_ws skips NULLs: a rule that is not broken adds nothing to the text
    reasons = F.concat_ws(";", *[F.when(cond, F.lit(name)) for name, cond in hard_rules.items()])
    flags = F.concat_ws(",", *[F.when(cond, F.lit(name)) for name, cond in soft_rules.items()])
    return (df
        .withColumn("quarantine_reason", reasons)
        .withColumn("data_quality_flags", F.when(flags != "", flags))
        .drop("duration_sec"))


def load_zone_lists(spark, rules):
    """Read the zone lookup: all valid zone ids and the airport zone ids."""
    zones = spark.read.csv(str(RAW_PATH / rules["lookup_file"]), header=True, inferSchema=True)
    zone_ids = [r["LocationID"] for r in zones.select("LocationID").collect()]
    airport_ids = [r["LocationID"] for r in
                   zones.filter(F.col("service_zone") == "Airports").select("LocationID").collect()]
    return zone_ids, airport_ids


def process_slice(spark, run_id, taxi_type, rules, zone_ids, airport_ids,
                  bronze, watermark_key, ts, mark):
    """Process the Bronze rows written at ONE ingestion time (ts)."""
    started = time.time()
    # Equality on the plain column lets Delta skip every file that does not match
    incoming = bronze.filter(F.col("ingestion_timestamp") == F.lit(ts))
    incoming_count = incoming.count()

    prepared = gate2(add_calculated_columns(unify(incoming, taxi_type)),
                     rules, zone_ids, airport_ids)
    deduped = prepared.dropDuplicates(["trip_id"]).persist(StorageLevel.MEMORY_AND_DISK)
    is_bad = F.col("quarantine_reason") != ""
    stats = deduped.agg(F.count("*"), F.sum(is_bad.cast("int"))).first()
    deduped_count, bad_count = stats[0], int(stats[1] or 0)
    good_count = deduped_count - bad_count
    duplicates = incoming_count - deduped_count

    good = deduped.filter(~is_bad).drop("quarantine_reason")
    bad = (deduped.filter(is_bad)
                  .withColumn("run_id", F.lit(run_id))
                  .withColumn("quarantined_at", F.current_timestamp()))

    if good_count:
        if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
            good.write.format("delta").mode("append").partitionBy("pickup_month").save(SILVER_PATH)
        else:
            # Insert-only MERGE: a row whose trip_id already exists is skipped.
            # The month list lets Delta read only the partitions that matter.
            months = [r[0] for r in good.select("pickup_month").distinct().collect()]
            month_list = ",".join(f"'{m}'" for m in months)
            (DeltaTable.forPath(spark, SILVER_PATH).alias("t")
                .merge(good.alias("s"),
                       f"t.trip_id = s.trip_id AND t.pickup_month IN ({month_list})")
                .whenNotMatchedInsertAll().execute())
    if bad_count:
        (bad.write.format("delta").mode("append")
            # Idempotent: if this slice is repeated after a crash, the second write is ignored
            .option("txnAppId", f"quarantine_silver_{taxi_type}_{WRITER_VERSION}")
            .option("txnVersion", mark)
            .option("mergeSchema", "true").save(QUARANTINE_PATH))

    # The slice is finished: move the watermark forward
    audit.set_watermark(watermark_key, mark)
    seconds = round(time.time() - started, 2)
    label = f"silver:{taxi_type}:{str(ts)[:19]}"
    log.info("event=silver_slice taxi=%s slice=%s read=%d duplicates_removed=%d accepted=%d "
             "quarantined=%d seconds=%.1f", taxi_type, str(ts)[:19], incoming_count,
             duplicates, good_count, bad_count, seconds)
    audit.log_step(run_id, "silver", label, incoming_count, good_count, bad_count,
                   seconds, "SUCCESS")
    deduped.unpersist()


def process_taxi_type(spark, run_id, taxi_type, source, rules, zone_ids, airport_ids):
    watermark_key = f"bronze.{source['table']}"
    bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/{source['table']}")
    micros = F.unix_micros("ingestion_timestamp")

    # Which Bronze rows are new? (ingested after the stored watermark)
    old_mark = audit.get_watermark(watermark_key)
    new_rows = bronze if old_mark is None else bronze.filter(micros > old_mark)

    # The distinct ingestion times, oldest first. Each one is one slice.
    slices = (new_rows.select("ingestion_timestamp", micros.alias("mark"))
                      .distinct().orderBy("mark").collect())
    if not slices:
        log.info("event=silver_skipped taxi=%s reason=no_new_rows", taxi_type)
        audit.log_step(run_id, "silver", f"silver:{taxi_type}", 0, 0, 0, 0, "SKIPPED")
        return
    log.info("event=silver_start taxi=%s slices=%d", taxi_type, len(slices))
    for row in slices:
        process_slice(spark, run_id, taxi_type, rules, zone_ids, airport_ids,
                      bronze, watermark_key, row["ingestion_timestamp"], row["mark"])


def main():
    with open(SOURCES_CONFIG) as f:
        sources = yaml.safe_load(f)["sources"]
    with open(RULES_CONFIG) as f:
        rules = yaml.safe_load(f)

    spark = build_spark()
    zone_ids, airport_ids = load_zone_lists(spark, rules)
    run_id = audit.start_run("silver_batch")
    log.info("event=run_started run_id=%s airport_zones=%s", run_id, airport_ids)
    try:
        for taxi_type, source in sources.items():
            process_taxi_type(spark, run_id, taxi_type, source, rules, zone_ids, airport_ids)
    except Exception:
        audit.finish_run(run_id, "FAILED")
        raise
    audit.finish_run(run_id, "SUCCESS")
    log.info("event=run_finished run_id=%s status=SUCCESS", run_id)
    spark.stop()


if __name__ == "__main__":
    main()