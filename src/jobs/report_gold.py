"""Gold report: reconciliation, uniqueness, referential integrity and a few sample results."""
import os

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F

L = os.environ["LAKEHOUSE_PATH"]
builder = (SparkSession.builder.master("local[6]")
           .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "8g"))
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog")
           .config("spark.sql.shuffle.partitions", "24"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def table(name):
    return spark.read.format("delta").load(f"{L}/gold/{name}")


silver = spark.read.format("delta").load(f"{L}/silver/trips").count()
fact_df = table("fact_trips")
fact = fact_df.count()
qpath = f"{L}/quarantine/quarantine_gold"
quarantine = spark.read.format("delta").load(qpath).count() if DeltaTable.isDeltaTable(spark, qpath) else 0

print(f"\nSilver rows        : {silver:,}")
print(f"Fact rows          : {fact:,}")
print(f"Gold quarantine    : {quarantine:,}")
print(f"Reconciliation (Silver - Fact - Quarantine) = {silver - fact - quarantine:,}  (must be 0)")

print("\n=== Dimensions ===")
for name in ("dim_date", "dim_location", "dim_payment", "dim_taxi_type"):
    print(f"{name:<14}: {table(name).count():,} rows")

print("\n=== Aggregates must add up to the fact ===")
daily_total = table("agg_trips_daily").agg(F.sum("trip_count")).first()[0]
hourly_total = table("agg_trips_hourly").agg(F.sum("trip_count")).first()[0]
print(f"agg_trips_daily  rows={table('agg_trips_daily').count():,}  trips={daily_total:,}  (fact {fact:,})")
print(f"agg_trips_hourly rows={table('agg_trips_hourly').count():,}  trips={hourly_total:,}  (fact {fact:,})")

print("\n=== Uniqueness of trip_id (checked month by month; a trip_id always stays in one month) ===")
months = [r[0] for r in fact_df.select("pickup_month").distinct().orderBy("pickup_month").collect()]
problems = 0
for month in months:
    part = fact_df.filter(F.col("pickup_month") == month)
    rows, distinct = part.count(), part.select("trip_id").distinct().count()
    if rows != distinct:
        problems += 1
        print(f"  DUPLICATES in {month}: {rows - distinct:,}")
print(f"months checked: {len(months)}, months with duplicates: {problems}  (must be 0)")

print("\n=== Referential integrity: fact keys without a dimension row (must all be 0) ===")
checks = [("pickup_date_key", "dim_date", "date_key"), ("pu_location_key", "dim_location", "location_key"),
          ("do_location_key", "dim_location", "location_key"), ("payment_key", "dim_payment", "payment_key"),
          ("taxi_type_key", "dim_taxi_type", "taxi_type_key")]
for fact_col, dim, dim_col in checks:
    orphans = (fact_df.select(fact_col).distinct()
               .join(table(dim).select(F.col(dim_col).alias(fact_col)), fact_col, "left_anti").count())
    print(f"  {fact_col:<18} -> {dim:<14}: {orphans}")

print("\n=== Sample: trips and average fare per month and taxi type ===")
(table("agg_trips_daily")
 .join(table("dim_date").select("date_key", "year_month"), F.col("pickup_date_key") == F.col("date_key"))
 .join(table("dim_taxi_type"), "taxi_type_key")
 .groupBy("year_month", "taxi_type")
 .agg(F.sum("trip_count").alias("trips"),
      F.round(F.sum("fare_sum") / F.sum("positive_trip_count"), 2).alias("avg_fare"))
 .orderBy("year_month", "taxi_type").show(50, truncate=False))
spark.stop()