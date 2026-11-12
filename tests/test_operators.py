"""Small tests for every custom operator in operators/.

Each operator is a YAML file whose logic is a plain Python run() function,
so we load it and run it on a few rows where we already know the answer.
"""
import glob
import os
from datetime import date

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OPERATOR_FILES = sorted(glob.glob(os.path.join(ROOT, "operators", "*.yaml")))


def load_operator(file_name):
    """Read an operator file and return (definition, run function)."""
    with open(os.path.join(ROOT, "operators", file_name)) as f:
        spec = yaml.safe_load(f)
    namespace = {}
    exec(spec["run_function"]["code"], namespace)
    return spec, namespace["run"]


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession() or SparkSession.builder.master("local[1]").getOrCreate()


# 1. Every operator file is complete: a name, a version, an output and a run() function
@pytest.mark.parametrize("path", OPERATOR_FILES, ids=os.path.basename)
def test_operator_file_is_complete(path):
    spec, run = load_operator(os.path.basename(path))
    assert spec.get("name"), "operator has no name"
    assert spec.get("version"), "operator has no version"
    assert spec["ports"]["output"], "operator has no output"
    assert callable(run), "operator logic has no run() function"


# 2. Business Days Between skips weekends and holidays
def test_business_days_between(spark):
    _, run = load_operator("business-days-between-v1.0.0.yaml")
    rows = [
        ("Mon to Fri", date(2025, 1, 6), date(2025, 1, 10), 4),
        ("Fri to Mon, over a weekend", date(2025, 1, 10), date(2025, 1, 13), 1),
        ("Christmas week", date(2024, 12, 23), date(2024, 12, 27), 3),
    ]
    df = spark.createDataFrame([r[:3] for r in rows], "booking string, start date, end date")
    config = {"start_column": "start", "end_column": "end", "holidays": "2024-12-25,2025-01-01"}
    got = {r["booking"]: r["business_days"] for r in run(config, {"data": df}, spark)["data"].collect()}
    for booking, _, _, expected in rows:
        assert got[booking] == expected, f"{booking}: expected {expected}, got {got[booking]}"


# 3. Lead Time Bucket labels how far ahead a stay was booked
def test_lead_time_bucket(spark):
    _, run = load_operator("lead-time-bucket-v1.0.0.yaml")
    rows = [
        ("booked 4 days ahead", date(2025, 1, 1), date(2025, 1, 5), "Last minute (0-7 days)"),
        ("booked 45 days ahead", date(2025, 1, 1), date(2025, 2, 15), "1-3 months"),
        ("booked 5 months ahead", date(2025, 1, 1), date(2025, 6, 1), "3+ months"),
    ]
    df = spark.createDataFrame([r[:3] for r in rows], "booking string, created_at date, check_in date")
    config = {"start_column": "created_at", "end_column": "check_in"}
    got = {r["booking"]: r["lead_time_bucket"] for r in run(config, {"data": df}, spark)["data"].collect()}
    for booking, _, _, expected in rows:
        assert got[booking] == expected, f"{booking}: expected {expected}, got {got[booking]}"
