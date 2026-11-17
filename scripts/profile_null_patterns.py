"""
Phase 1, Script A2: check WHY some columns are NULL.

Questions:
  1) Are the five Yellow columns NULL on the same rows as payment_type = 0?
  2) How are those rows spread across months?
  3) How does request_source relate to payment_type and to months?
  4) Where do the NULL payment_type / cbd_congestion_fee rows of Green come from?
"""

import duckdb

RAW_PATH = "F:/datasets/tlc_raw"
con = duckdb.connect()


def make_view(taxi):
    """Virtual table over all monthly files (columns matched by name)."""
    con.execute(f"""
        Create Or Replace View {taxi}_trips As
        Select * From read_parquet('{RAW_PATH}/{taxi}/*.parquet',
                                   union_by_name = true)
    """)


def show(title, query):
    """Run a query and print the rows in a simple table."""
    print(f"\n--- {title} ---")
    cursor = con.execute(query)
    print("  ".join(d[0] for d in cursor.description))
    for row in cursor.fetchall():
        print("  ".join(str(v) for v in row))


make_view("yellow")
make_view("green")

# 1) Inside Flex Fare rows (payment_type = 0): how many NULLs per column?
#    If every number equals flex_rows, the NULLs sit on exactly these rows.
show("Yellow: NULLs INSIDE payment_type = 0", """
    Select
        count(*) As flex_rows,
        count(*) Filter (Where passenger_count Is Null)      As null_passenger,
        count(*) Filter (Where RatecodeID Is Null)           As null_ratecode,
        count(*) Filter (Where store_and_fwd_flag Is Null)   As null_flag,
        count(*) Filter (Where congestion_surcharge Is Null) As null_congestion,
        count(*) Filter (Where Airport_fee Is Null)          As null_airport
    From yellow_trips
    Where payment_type = 0
""")

# 1b) The reverse: rows that are NOT Flex Fare but still have one of these NULLs.
#     We expect 0 here.
show("Yellow: NULLs OUTSIDE payment_type = 0 (expected 0)", """
    Select count(*) As rows_with_null_outside_flex
    From yellow_trips
    Where payment_type <> 0
      And (passenger_count Is Null Or RatecodeID Is Null
           Or store_and_fwd_flag Is Null Or congestion_surcharge Is Null
           Or Airport_fee Is Null)
""")

# 2) Flex Fare rows per pickup month (also reveals strange pickup dates)
show("Yellow: payment_type = 0 rows per pickup month", """
    Select strftime(tpep_pickup_datetime, '%Y-%m') As pickup_month,
           count(*) As flex_rows
    From yellow_trips
    Where payment_type = 0
    Group By 1
    Order By 1
""")

# 3) request_source versus payment_type (only rows where request_source exists)
show("Yellow: request_source x payment_type", """
    Select request_source, payment_type, count(*) As n
    From yellow_trips
    Where request_source Is Not Null
    Group By 1, 2
    Order By n Desc
    Limit 20
""")

show("Yellow: rows with request_source per pickup month", """
    Select strftime(tpep_pickup_datetime, '%Y-%m') As pickup_month,
           count(*) As rows_with_request_source
    From yellow_trips
    Where request_source Is Not Null
    Group By 1
    Order By 1
""")

# 4) Green: NULL payment_type and NULL cbd_congestion_fee per pickup month
show("Green: NULL patterns per pickup month", """
    Select strftime(lpep_pickup_datetime, '%Y-%m') As pickup_month,
           count(*) As total_rows,
           count(*) Filter (Where payment_type Is Null)         As null_payment,
           count(*) Filter (Where cbd_congestion_fee Is Null)   As null_cbd
    From green_trips
    Group By 1
    Order By 1
""")