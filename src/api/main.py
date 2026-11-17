"""
Simulated 'live dispatch' REST API.
It serves the trips of one month (STREAM_MONTH) page by page, as JSON.
Endpoints:
  GET /health
  GET /trips/{yellow|green}?offset=0&limit=1000
"""

import math
import os
from pathlib import Path

import pyarrow.parquet as pq
from fastapi import FastAPI, HTTPException, Query

RAW_PATH = Path(os.environ["RAW_DATA_PATH"])
STREAM_MONTH = os.getenv("STREAM_MONTH", "2026-09")

app = FastAPI(title="NYC Taxi Dispatch API (simulated)")

# Each file is read once and kept in memory, so paging is fast
_cache = {}


def load_table(taxi_type):
    """Read the monthly Parquet file of one taxi type (first call only)."""
    if taxi_type not in _cache:
        path = RAW_PATH / taxi_type / f"{taxi_type}_tripdata_{STREAM_MONTH}.parquet"
        if not path.exists():
            raise HTTPException(status_code=404, detail=f"no data file for {taxi_type}")
        _cache[taxi_type] = pq.read_table(path)
    return _cache[taxi_type]


def clean(value):
    """JSON cannot hold NaN, so turn it into null."""
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


@app.get("/health")
def health():
    return {"status": "ok", "month": STREAM_MONTH}


@app.get("/trips/{taxi_type}")
def get_trips(taxi_type: str,
              offset: int = Query(0, ge=0),
              limit: int = Query(1000, ge=1, le=10000)):
    if taxi_type not in ("yellow", "green"):
        raise HTTPException(status_code=404, detail="taxi_type must be yellow or green")
    table = load_table(taxi_type)
    # Slice the requested page and convert it to a list of dictionaries
    rows = table.slice(offset, limit).to_pylist()
    rows = [{key: clean(value) for key, value in row.items()} for row in rows]
    return {
        "taxi_type": taxi_type,
        "month": STREAM_MONTH,
        "offset": offset,
        "count": len(rows),
        "total": table.num_rows,
        "rows": rows,
    }