"""
Step 2 of Phase 1: scan the Parquet footers (metadata only) of every TLC file.

Goal: find out
  1) how many rows each file has
  2) whether the columns and data types are the same in every month
"""

from pathlib import Path
import pyarrow.parquet as pq

# Where the raw files live. Later this value moves to the .env file.
RAW_PATH = Path(r"F:\datasets\tlc_raw")


def read_schema(file_path):
    """Return (row_count, {column_name: data_type}) using only the file footer."""
    pf = pq.ParquetFile(file_path)
    row_count = pf.metadata.num_rows
    # schema_arrow lists every column with its type, e.g. "passenger_count": "int64"
    columns = {field.name: str(field.type) for field in pf.schema_arrow}
    return row_count, columns


def profile_taxi_type(taxi_type):
    """Print the row count per file, then list schema differences against the first file."""
    files = sorted((RAW_PATH / taxi_type).glob("*.parquet"))
    print(f"\n{'=' * 60}\n{taxi_type.upper()}: {len(files)} files\n{'=' * 60}")

    baseline = None        # the first file's schema, used as the reference
    total_rows = 0

    for f in files:
        rows, columns = read_schema(f)
        total_rows += rows
        print(f"{f.name:<35} rows = {rows:>10,}   columns = {len(columns)}")

        if baseline is None:
            baseline = columns
            continue

        # Compare this file's schema with the baseline
        added = set(columns) - set(baseline)
        missing = set(baseline) - set(columns)
        changed = {c: (baseline[c], columns[c])
                   for c in set(columns) & set(baseline)
                   if baseline[c] != columns[c]}

        if added or missing or changed:
            print(f"    !! SCHEMA DIFFERENCE in {f.name}")
            if added:
                print(f"       new columns     : {sorted(added)}")
            if missing:
                print(f"       missing columns : {sorted(missing)}")
            for col, (old, new) in changed.items():
                print(f"       type changed    : {col}: {old} -> {new}")

    print(f"\nTOTAL {taxi_type} rows: {total_rows:,}")
    print("\nBaseline schema (first file):")
    for name, dtype in baseline.items():
        print(f"    {name:<28} {dtype}")


if __name__ == "__main__":
    for taxi in ("yellow", "green"):
        profile_taxi_type(taxi)