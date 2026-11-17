"""
Phase 1, Script A: count NULLs per column and list the values of code columns.

Tool: DuckDB. It reads the Parquet files directly, so the 82 million rows
are never loaded into memory at once.
"""

import duckdb

# Where the raw files live (forward slashes work fine on Windows)
RAW_PATH = "F:/datasets/tlc_raw"

# Columns that hold codes. We list their distinct values and how often each appears.
CODE_COLUMNS = {
    "yellow": ["VendorID", "RatecodeID", "payment_type",
               "store_and_fwd_flag", "request_source"],
    "green": ["VendorID", "RatecodeID", "payment_type", "store_and_fwd_flag",
              "trip_type", "request_source"],
}

# One in-memory DuckDB connection (no database file is created)
con = duckdb.connect()


def make_view(taxi):
    """Create a virtual table over all monthly files of one taxi type."""
    # union_by_name = true matches columns by NAME, so files that lack
    # request_source (before 2026-06) simply get NULL in that column.
    con.execute(f"""
        Create Or Replace View {taxi}_trips As
        Select * From read_parquet('{RAW_PATH}/{taxi}/*.parquet',
                                   union_by_name = true)
    """)


def null_report(taxi):
    """Print the NULL count and percentage for every column."""
    # Describe returns one row per column; the first field is the column name
    columns = [row[0] for row in con.execute(f"Describe {taxi}_trips").fetchall()]

    # Build ONE query that counts NULLs for all columns in a single pass.
    # count(*) counts all rows, count(col) counts only non-NULL rows,
    # so the difference is the number of NULLs.
    parts = ", ".join(f'count(*) - count("{c}")' for c in columns)
    result = con.execute(
        f"Select count(*), {parts} From {taxi}_trips"
    ).fetchone()

    total_rows = result[0]
    print(f"\n{'=' * 60}\n{taxi.upper()}: NULLs per column "
          f"(total rows = {total_rows:,})\n{'=' * 60}")
    for column, nulls in zip(columns, result[1:]):
        print(f"{column:<28} {nulls:>12,}   {nulls / total_rows * 100:6.2f}%")


def value_report(taxi):
    """Print each distinct value of the code columns with its row count."""
    print(f"\n{'=' * 60}\n{taxi.upper()}: values of code columns\n{'=' * 60}")
    for column in CODE_COLUMNS[taxi]:
        rows = con.execute(f"""
            Select "{column}", count(*) As n
            From {taxi}_trips
            Group By 1
            Order By n Desc
            Limit 20
        """).fetchall()
        print(f"\n{column}:")
        for value, n in rows:
            print(f"    {str(value):<12} {n:>12,}")


if __name__ == "__main__":
    for taxi_type in ("yellow", "green"):
        make_view(taxi_type)
        null_report(taxi_type)
        value_report(taxi_type)