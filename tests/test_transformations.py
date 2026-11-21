"""
Unit tests for the transformation and quality-gate functions.
Each test builds a few tiny rows, runs the REAL function and checks the result.
Run with:  python -m pytest tests -v
"""

from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField,
                               StructType, TimestampType)

import bronze_batch
import gold_batch
import silver_batch

RULES = yaml.safe_load((Path(__file__).resolve().parents[1] / "config" / "rules.yml").read_text())
ZONES = list(range(1, 266))
AIRPORTS = [132, 138]

# ---- helpers: tiny Bronze-like rows -------------------------------------------------------
MONEY = ["fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
         "improvement_surcharge", "total_amount", "congestion_surcharge"]

YELLOW_SCHEMA = StructType([
    StructField("VendorID", IntegerType()), StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()), StructField("passenger_count", LongType()),
    StructField("trip_distance", DoubleType()), StructField("RatecodeID", LongType()),
    StructField("store_and_fwd_flag", StringType()), StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()), StructField("payment_type", LongType()),
    *[StructField(m, DoubleType()) for m in MONEY],
    StructField("Airport_fee", DoubleType()), StructField("cbd_congestion_fee", DoubleType()),
    StructField("request_source", StringType()), StructField("taxi_type", StringType()),
    StructField("source_file", StringType()), StructField("ingestion_timestamp", TimestampType()),
])

GREEN_SCHEMA = StructType([
    StructField("VendorID", IntegerType()), StructField("lpep_pickup_datetime", TimestampType()),
    StructField("lpep_dropoff_datetime", TimestampType()), StructField("passenger_count", LongType()),
    StructField("trip_distance", DoubleType()), StructField("RatecodeID", LongType()),
    StructField("store_and_fwd_flag", StringType()), StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()), StructField("payment_type", LongType()),
    *[StructField(m, DoubleType()) for m in MONEY],
    StructField("ehail_fee", DoubleType()), StructField("trip_type", LongType()),
    StructField("cbd_congestion_fee", DoubleType()), StructField("taxi_type", StringType()),
    StructField("source_file", StringType()), StructField("ingestion_timestamp", TimestampType()),
])

PICKUP = datetime(2025, 3, 10, 8, 0, 0)


def row(schema, **changes):
    """One valid trip, with any field replaced by the keyword arguments."""
    base = {
        "VendorID": 2, "passenger_count": 1, "trip_distance": 3.0, "RatecodeID": 1,
        "store_and_fwd_flag": "N", "PULocationID": 161, "DOLocationID": 236, "payment_type": 1,
        "fare_amount": 15.0, "extra": 1.0, "mta_tax": 0.5, "tip_amount": 3.0, "tolls_amount": 0.0,
        "improvement_surcharge": 1.0, "total_amount": 22.5, "congestion_surcharge": 2.5,
        "Airport_fee": 0.0, "cbd_congestion_fee": 0.75, "request_source": None,
        "ehail_fee": None, "trip_type": None, "taxi_type": "yellow",
        "source_file": "yellow_tripdata_2025-03.parquet",
        "ingestion_timestamp": datetime(2026, 11, 17, 13, 0, 0),
        "tpep_pickup_datetime": PICKUP, "tpep_dropoff_datetime": PICKUP + timedelta(minutes=10),
        "lpep_pickup_datetime": PICKUP, "lpep_dropoff_datetime": PICKUP + timedelta(minutes=10),
    }
    base.update(changes)
    return tuple(base[f.name] for f in schema.fields)


def silver_with_gate2(spark, rows, schema=YELLOW_SCHEMA, taxi="yellow"):
    """Run unify, calculated columns and Gate 2 on the rows. Returns the result as dictionaries."""
    df = spark.createDataFrame(rows, schema)
    result = silver_batch.gate2(silver_batch.add_calculated_columns(silver_batch.unify(df, taxi)),
                                RULES, ZONES, AIRPORTS)
    return [r.asDict() for r in result.collect()]


# ---- unify: one schema for Yellow and Green ------------------------------------------------
def test_unify_yellow_uses_common_names(spark):
    out = silver_batch.unify(spark.createDataFrame([row(YELLOW_SCHEMA)], YELLOW_SCHEMA), "yellow")
    assert {"pickup_datetime", "dropoff_datetime", "airport_fee", "trip_type"} <= set(out.columns)
    assert "tpep_pickup_datetime" not in out.columns
    assert out.collect()[0]["trip_type"] is None          # Green-only column is empty for Yellow


def test_unify_green_drops_ehail_and_adds_airport_fee(spark):
    df = spark.createDataFrame([row(GREEN_SCHEMA, taxi_type="green", trip_type=1)], GREEN_SCHEMA)
    out = silver_batch.unify(df, "green")
    assert "ehail_fee" not in out.columns                 # 100% empty column is dropped
    assert "airport_fee" in out.columns and out.collect()[0]["airport_fee"] is None
    assert out.collect()[0]["trip_type"] == 1


# ---- calculated columns ---------------------------------------------------------------------
def test_duration_and_speed(spark):
    r = silver_with_gate2(spark, [row(YELLOW_SCHEMA, trip_distance=5.0)])[0]   # 5 miles in 10 minutes
    assert r["trip_duration_min"] == 10.0
    assert r["speed_mph"] == 30.0


def test_trip_id_is_stable_and_unique(spark):
    rows = silver_with_gate2(spark, [row(YELLOW_SCHEMA), row(YELLOW_SCHEMA),
                                     row(YELLOW_SCHEMA, fare_amount=99.0)])
    ids = [r["trip_id"] for r in rows]
    assert ids[0] == ids[1] and ids[0] != ids[2]


def test_flex_fare_and_helper_flags(spark):
    r = silver_with_gate2(spark, [row(YELLOW_SCHEMA, payment_type=0, passenger_count=None,
                                      RatecodeID=None, store_and_fwd_flag=None,
                                      congestion_surcharge=None, Airport_fee=None)])[0]
    assert r["is_flex_fare"] is True
    assert r["quarantine_reason"] == ""                   # empty columns on Flex Fare are normal
    assert r["data_quality_flags"] is None                # and they are not flagged
    card = silver_with_gate2(spark, [row(YELLOW_SCHEMA, payment_type=1, store_and_fwd_flag="Y")])[0]
    assert card["is_tip_recorded"] is True and card["is_store_and_forward"] is True


# ---- Gate 2: hard rules go to quarantine -----------------------------------------------------
@pytest.mark.parametrize("changes, reason", [
    ({"tpep_dropoff_datetime": PICKUP - timedelta(minutes=5)}, "dropoff_before_pickup"),
    ({"trip_distance": 150.0}, "distance_over_max"),
    ({"trip_distance": 50.0}, "speed_over_max"),          # 50 miles in 10 minutes = 300 mph
    ({"passenger_count": 7}, "passenger_count_over_max"),
    ({"fare_amount": 1500.0}, "fare_over_max"),
    ({"tpep_pickup_datetime": datetime(2020, 5, 1, 8, 0, 0),
      "tpep_dropoff_datetime": datetime(2020, 5, 1, 8, 10, 0)}, "pickup_outside_project_range"),
    ({"tpep_dropoff_datetime": PICKUP + timedelta(hours=30)}, "duration_over_max"),
])
def test_gate2_hard_rules_quarantine(spark, changes, reason):
    r = silver_with_gate2(spark, [row(YELLOW_SCHEMA, **changes)])[0]
    assert reason in r["quarantine_reason"]


def test_gate2_good_row_passes(spark):
    r = silver_with_gate2(spark, [row(YELLOW_SCHEMA)])[0]
    assert r["quarantine_reason"] == "" and r["data_quality_flags"] is None


# ---- Gate 2: soft rules only flag, the row is kept ---------------------------------------------
def test_gate2_soft_rules_flag_but_keep(spark):
    zero = silver_with_gate2(spark, [row(YELLOW_SCHEMA, passenger_count=0)])[0]
    assert zero["quarantine_reason"] == "" and "passenger_count_zero" in zero["data_quality_flags"]
    cash_tip = silver_with_gate2(spark, [row(YELLOW_SCHEMA, payment_type=2, tip_amount=5.0)])[0]
    assert "cash_with_tip" in cash_tip["data_quality_flags"]


def test_negative_amounts_are_flagged_not_quarantined(spark):
    r = silver_with_gate2(spark, [row(YELLOW_SCHEMA, payment_type=4, fare_amount=-12.0,
                                      total_amount=-12.0)])[0]
    assert r["has_negative_amount"] is True
    assert r["quarantine_reason"] == "" and "negative_amount" in r["data_quality_flags"]


# ---- Gate 1: file-level checks ------------------------------------------------------------------
REQUIRED = ["a", "b"]


def test_gate1_accepts_a_good_file(spark, tmp_path):
    path = tmp_path / "good.parquet"
    spark.createDataFrame([(1, 2)], ["a", "b"]).write.parquet(str(path))
    passed, reason, df, rows = bronze_batch.gate1(spark, path, REQUIRED)
    assert passed and reason is None and rows == 1


def test_gate1_rejects_missing_mandatory_column(spark, tmp_path):
    path = tmp_path / "missing.parquet"
    spark.createDataFrame([(1,)], ["a"]).write.parquet(str(path))
    passed, reason, _, rows = bronze_batch.gate1(spark, path, REQUIRED)
    assert not passed and "missing mandatory columns" in reason and rows == 1


def test_gate1_rejects_unreadable_file(spark, tmp_path):
    path = tmp_path / "bad.parquet"
    path.write_text("this is not a parquet file")
    passed, reason, _, _ = bronze_batch.gate1(spark, path, REQUIRED)
    assert not passed and "unreadable file" in reason


def test_gate1_rejects_empty_file(spark, tmp_path):
    path = tmp_path / "empty.parquet"
    spark.createDataFrame([], StructType([StructField("a", LongType()),
                                          StructField("b", LongType())])).write.parquet(str(path))
    passed, reason, _, rows = bronze_batch.gate1(spark, path, REQUIRED)
    assert not passed and reason == "file has no rows" and rows == 0


# ---- Gate 3 and the date dimension ------------------------------------------------------------------
def fact_rows(spark, *rows):
    schema = StructType([StructField(n, LongType()) if n != "trip_id" else StructField(n, StringType())
                         for n in ["trip_id", "pickup_date_key", "dropoff_date_key", "taxi_type_key",
                                   "pu_location_key", "do_location_key", "payment_key"]])
    return spark.createDataFrame(rows, schema)


def test_gate3_accepts_valid_and_rejects_orphan_keys(spark):
    df = fact_rows(spark,
                   ("ok", 20250310, 20250310, 1, 161, 236, 1),
                   ("bad_zone", 20250310, 20250310, 1, 999, 236, 1),
                   ("bad_date", 19990101, 20250310, 1, 161, 236, 1),
                   ("bad_payment", 20250310, 20250310, 1, 161, 236, 77))
    out = {r["trip_id"]: r["quarantine_reason"] for r in
           gold_batch.gate3(df, [20250310], list(range(1, 266)), [-1, 0, 1, 2, 3, 4, 5, 6]).collect()}
    assert out["ok"] == ""
    assert "pu_location_not_in_dim_location" in out["bad_zone"]
    assert "pickup_date_not_in_dim_date" in out["bad_date"]
    assert "payment_not_in_dim_payment" in out["bad_payment"]


def test_dim_date_weekday_numbers(spark):
    rows = {r["date_key"]: r.asDict() for r in gold_batch.build_dim_date(spark).collect()}
    assert rows[20250106]["day_name"] == "Monday" and rows[20250106]["day_of_week"] == 1
    assert rows[20250105]["day_of_week"] == 7 and rows[20250105]["is_weekend"] is True
    assert len(rows) == 730