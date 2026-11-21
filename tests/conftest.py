"""Shared test setup: makes src/ importable and gives every test one small local Spark session."""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src" / "jobs"))

# The job modules read these when they are imported (real values come from .env in Docker)
os.environ.setdefault("RAW_DATA_PATH", "/tmp/raw")
os.environ.setdefault("LAKEHOUSE_PATH", "/tmp/lakehouse")


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]").appName("unit_tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()