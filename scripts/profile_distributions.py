"""
Phase 1, Script B: distributions and rule-related counts.

1) pickup month versus file month
2) percentiles of distance, fare, total, tip, duration and speed
3) passenger_count values
4) negative and zero values by payment_type

Tool: DuckDB. Percentiles use approx_quantile (fast and good enough for profiling).
"""

import duckdb

RAW_PATH = "F:/datasets/tlc_raw"
PICKUP = {"yellow": "tpep_pickup_datetime", "green": "lpep_pickup_datetime"}
DROPOFF = {"yellow": "tpep_dropoff_datetime", "green": "lpep_dropoff_datetime"}

con = duckdb.connect()


def make_view(taxi):
    """Virtual table over all monthly files. filename = true adds the file path column."""
    con.execute(f"""
        Create Or Replace View {taxi}_trips As
        Select *,
               regexp_extract(filename, '([0-9]+-[0-9]+)[.]parquet', 1) As file_month
        From read_parquet('{RAW_PATH}/{taxi}/*.parquet',
                          union_by_name = true, filename = true)
    """)


def show(title, query):
    """Run a query and print the result as a simple table."""
    print(f"\n--- {title} ---")
    cursor = con.execute(query)
    print("  ".join(d[0] for d in cursor.description))
    for row in cursor.fetchall():
        print("  ".join(str(v) for v in row))


def profile(taxi):
    pu, do = PICKUP[taxi], DROPOFF[taxi]
    print(f"\n{'=' * 60}\n{taxi.upper()}\n{'=' * 60}")

    # 1) Rows whose pickup month is different from the month of their file
    show(f"{taxi}: total rows with pickup month different from file month", f"""
        Select count(*) As mismatched_rows
        From {taxi}_trips
        Where strftime({pu}, '%Y-%m') <> file_month
    """)
    show(f"{taxi}: biggest mismatches (file_month, pickup_month)", f"""
        Select file_month, strftime({pu}, '%Y-%m') As pickup_month, count(*) As n
        From {taxi}_trips
        Where strftime({pu}, '%Y-%m') <> file_month
        Group By 1, 2
        Order By n Desc
        Limit 20
    """)

    # 2) Percentiles. Duration is in minutes, speed in miles per hour.
    # Speed is calculated only when the duration is greater than zero.
    duration_min = f"date_diff('second', {pu}, {do}) / 60.0"
    speed_mph = (f"trip_distance / (date_diff('second', {pu}, {do}) / 3600.0)")
    measures = {
        "trip_distance": ("trip_distance", "1 = 1"),
        "fare_amount": ("fare_amount", "1 = 1"),
        "total_amount": ("total_amount", "1 = 1"),
        "tip_amount": ("tip_amount", "1 = 1"),
        "duration_min": (duration_min, "1 = 1"),
        "speed_mph": (speed_mph, f"date_diff('second', {pu}, {do}) > 0"),
    }
    for name, (expr, condition) in measures.items():
        show(f"{taxi}: {name} (min, p50, p99, p99.9, max)", f"""
            Select min({expr}) As min_value,
                   approx_quantile({expr}, 0.5)   As p50,
                   approx_quantile({expr}, 0.99)  As p99,
                   approx_quantile({expr}, 0.999) As p99_9,
                   max({expr}) As max_value
            From {taxi}_trips
            Where {condition}
        """)

    # 3) passenger_count values
    show(f"{taxi}: passenger_count values", f"""
        Select passenger_count, count(*) As n
        From {taxi}_trips
        Group By 1
        Order By 1
        Limit 20
    """)

    # 4) Negative and zero values by payment_type
    show(f"{taxi}: negative and zero values by payment_type", f"""
        Select payment_type,
               count(*) As total,
               count(*) Filter (Where fare_amount < 0)    As neg_fare,
               count(*) Filter (Where total_amount < 0)   As neg_total,
               count(*) Filter (Where trip_distance < 0)  As neg_distance,
               count(*) Filter (Where fare_amount = 0)    As zero_fare,
               count(*) Filter (Where trip_distance = 0)  As zero_distance
        From {taxi}_trips
        Group By 1
        Order By 1
    """)


if __name__ == "__main__":
    for taxi_type in ("yellow", "green"):
        make_view(taxi_type)
        profile(taxi_type)