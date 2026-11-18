"""Silver report: totals, reconciliation with Bronze, quarantine reasons and flag counts."""
import os
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

L = os.environ["LAKEHOUSE_PATH"]
builder = (SparkSession.builder.master("local[*]")
           .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog",
                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

bronze = sum(spark.read.format("delta").load(f"{L}/bronze/{t}").count()
             for t in ("yellow_trips", "green_trips"))
silver_df = spark.read.format("delta").load(f"{L}/silver/trips")
quar_df = spark.read.format("delta").load(f"{L}/quarantine/quarantine_silver")
silver, quar = silver_df.count(), quar_df.count()
print(f"\nBronze rows      : {bronze:,}")
print(f"Silver rows      : {silver:,}")
print(f"Quarantine rows  : {quar:,}")
print(f"Duplicates removed (Bronze - Silver - Quarantine): {bronze - silver - quar:,}")

print("\n=== Silver by taxi type ===")
silver_df.groupBy("taxi_type").count().show()
print("=== Quarantine reasons (a row can have several) ===")
(quar_df.select(F.explode(F.split("quarantine_reason", ";")).alias("reason"))
        .groupBy("reason").count().orderBy(F.desc("count")).show(truncate=False))
print("=== Flags in Silver (a row can have several) ===")
(silver_df.filter("data_quality_flags is not null")
          .select(F.explode(F.split("data_quality_flags", ",")).alias("flag"))
          .groupBy("flag").count().orderBy(F.desc("count")).show(30, truncate=False))
spark.stop()