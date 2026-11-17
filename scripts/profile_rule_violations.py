"""
Phase 1, Script C: count how many rows break each proposed rule,
and check the zone lookup file.

Tool: DuckDB. Counts overlap: one row can break several rules.
"""

import duckdb

RAW_PATH = "F:/datasets/tlc_raw"

# Project date range: pickups outside it are impossible for our data
RANGE_START = "2025-01-01"
RANGE_END = "2026-10-01"   # exclusive

PICKUP = {"yellow": "tpep_pickup_datetime", "green": "lpep_pickup_datetime"}
DROPOFF = {"yellow": "tpep_dropoff_datetime", "green": "lpep_dropoff_datetime"}
# The extra fee column that exists in only one taxi type
EXTRA_FEE = {"yellow": "Airport_fee", "green": "ehail_fee"}

con = duckdb.connect()
# Let DuckDB spill to disk instead of running out of memory
con.execute("Set memory_limit = '16GB'")
con.execute("Set temp_directory = 'F:/duckdb_tmp'")


def show(title, query):
    """Run a query and print the result as a simple table."""
    print(f"\n--- {title} ---")
    cursor = con.execute(query)
    print("  ".join(d[0] for d in cursor.description))
    for row in cursor.fetchall():
        print("  ".join(str(v) for v in row))


def make_views(taxi):
    con.execute(f"""
        Create Or Replace View {taxi}_trips As
        Select * From read_parquet('{RAW_PATH}/{taxi}/*.parquet',
                                   union_by_name = true)
    """)


def lookup_checks():
    """Inspect the zone lookup file."""
    con.execute(f"""
        Create Or Replace View zones As
        Select * From read_csv_auto('{RAW_PATH}/lookup/taxi_zone_lookup.csv')
    """)
    show("lookup: size", """
        Select count(*) As rows_in_file,
               count(distinct LocationID) As distinct_ids,
               min(LocationID) As min_id, max(LocationID) As max_id
        From zones
    """)
    show("lookup: service_zone values", """
        Select service_zone, count(*) As n From zones Group By 1 Order By n Desc
    """)
    show("lookup: rows that look like Unknown", """
        Select * From zones
        Where lower(Borough) In ('unknown', 'n/a')
           Or lower(Zone) In ('unknown', 'n/a', 'na')
    """)


def zone_orphans(taxi):
    """Count trips whose zone code is missing from the lookup."""
    show(f"{taxi}: zone codes not found in the lookup", f"""
        Select
            count(*) Filter (Where PULocationID Not In
                             (Select LocationID From zones)) As pu_not_in_lookup,
            count(*) Filter (Where DOLocationID Not In
                             (Select LocationID From zones)) As do_not_in_lookup
        From {taxi}_trips
    """)


def rule_counts(taxi):
    """Count rows that break each proposed rule (all in one pass over the data)."""
    pu, do, extra = PICKUP[taxi], DROPOFF[taxi], EXTRA_FEE[taxi]
    dur = f"date_diff('second', {pu}, {do})"          # duration in seconds
    speed = f"trip_distance / ({dur} / 3600.0)"       # miles per hour
    # Sum of all fee components, treating NULL fees as 0 (reconciliation only)
    parts = (f"fare_amount + extra + mta_tax + tip_amount + tolls_amount"
             f" + improvement_surcharge + coalesce(congestion_surcharge, 0)"
             f" + coalesce(cbd_congestion_fee, 0) + coalesce({extra}, 0)")

    rules = {
        "pickup_is_null": f"{pu} Is Null",
        "dropoff_is_null": f"{do} Is Null",
        "pickup_not_before_dropoff": f"{pu} >= {do}",
        "pickup_outside_project_range":
            f"({pu} < timestamp '{RANGE_START}' Or {pu} >= timestamp '{RANGE_END}')",
        "duration_over_24h": f"{dur} > 86400",
        "duration_over_3h": f"{dur} > 10800",
        "duration_under_1min": f"{dur} > 0 And {dur} < 60",
        "passenger_zero": "passenger_count = 0",
        "passenger_over_6": "passenger_count > 6",
        "distance_zero": "trip_distance = 0",
        "distance_over_50": "trip_distance > 50",
        "distance_over_100": "trip_distance > 100",
        "fare_zero": "fare_amount = 0",
        "fare_over_300": "fare_amount > 300",
        "fare_over_1000": "fare_amount > 1000",
        "speed_over_80": f"{dur} > 0 And {speed} > 80",
        "speed_over_100": f"{dur} > 0 And {speed} > 100",
        "total_is_null": "total_amount Is Null",
        "total_mismatch_over_0_05": f"abs(total_amount - ({parts})) > 0.05",
        "total_mismatch_over_1": f"abs(total_amount - ({parts})) > 1",
        "cash_with_tip": "payment_type = 2 And tip_amount > 0",
        "cbd_fee_before_2025_01_05":
            f"cbd_congestion_fee > 0 And {pu} < timestamp '2025-01-05'",
        "any_negative_amount":
            ("least(fare_amount, extra, mta_tax, tip_amount, tolls_amount,"
             " improvement_surcharge, total_amount,"
             " coalesce(congestion_surcharge, 0), coalesce(cbd_congestion_fee, 0)) < 0"),
    }
    if taxi == "yellow":
        rules["airport_fee_above_0"] = "Airport_fee > 0"
        rules["airport_fee_outside_airport_zone"] = (
            "Airport_fee > 0 And PULocationID Not In "
            "(Select LocationID From zones Where service_zone = 'Airports')")

    selects = ", ".join(f"count(*) Filter (Where {cond})" for cond in rules.values())
    result = con.execute(f"Select count(*), {selects} From {taxi}_trips").fetchone()

    print(f"\n--- {taxi}: rows breaking each rule (total rows = {result[0]:,}) ---")
    for name, n in zip(rules.keys(), result[1:]):
        print(f"{name:<40} {n:>12,}   {n / result[0] * 100:7.3f}%")


def duplicate_count(taxi):
    """Count rows that are exact copies of another row (all columns equal)."""
    show(f"{taxi}: exact duplicate rows", f"""
        Select count(*) As total_rows,
               (Select count(*) From (Select Distinct * From {taxi}_trips)) As distinct_rows,
               count(*) - (Select count(*) From (Select Distinct * From {taxi}_trips))
                   As duplicate_rows
        From {taxi}_trips
    """)


if __name__ == "__main__":
    lookup_checks()
    for taxi_type in ("yellow", "green"):
        make_views(taxi_type)
        zone_orphans(taxi_type)
        rule_counts(taxi_type)
    # Duplicates last, because this is the slowest check
    for taxi_type in ("green", "yellow"):
        duplicate_count(taxi_type)