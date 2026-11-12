"""Small tests for custom operators.

Each operator is a YAML file whose logic is a plain Python run() function,
so we load it and run it on a few rows where we already know the answer.
Set the OPERATOR_FILE environment variable (the run_tests notebook does this)
to test one specific operator; without it we test every operator we know
the answers for.
"""
import glob
import os
from datetime import date

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Operator files live either at the project root or in an operators/ folder.
OPERATOR_DIRS = [ROOT, os.path.join(ROOT, "operators")]

OPERATOR_FILES = sorted(
    path for d in OPERATOR_DIRS for path in glob.glob(os.path.join(d, "*.yaml"))
)


def find_operator_path(file_name):
    """Return the full path of an operator file, wherever it lives."""
    for d in OPERATOR_DIRS:
        candidate = os.path.join(d, file_name)
        if os.path.exists(candidate):
            return candidate
    available = ", ".join(os.path.basename(p) for p in OPERATOR_FILES) or "none found"
    raise FileNotFoundError(
        f"{file_name} not found in {OPERATOR_DIRS}. Available operator files: {available}"
    )


def load_operator(file_name):
    """Read an operator file and return (definition, run function)."""
    with open(find_operator_path(file_name)) as f:
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


# 2. The known answers for each operator: the input schema, and for each case
# the config, a few rows (first the label, then the input values, then the
# expected result) and the column run() adds. When you add an operator file,
# add its entry here the same day.
KNOWN_OPERATORS = {
    "business-day-counter-v1.0.0.yaml": {
        "input": "booking string, start date, end date",
        "cases": [
            {
                "config": {"start_date_column": "start", "end_date_column": "end"},
                "rows": [
                    ("Mon to Fri", date(2025, 1, 6), date(2025, 1, 10), 5),
                    ("Fri to Mon, over a weekend", date(2025, 1, 10), date(2025, 1, 13), 2),
                    ("Same day", date(2025, 1, 6), date(2025, 1, 6), 1),
                    ("Weekend only", date(2025, 1, 11), date(2025, 1, 12), 0),
                    ("End before start", date(2025, 1, 10), date(2025, 1, 6), 5),
                ],
                "result_column": "business_days_count",
            },
            {
                "config": {
                    "start_date_column": "start",
                    "end_date_column": "end",
                    "exclude_us_holidays": True,
                },
                "rows": [
                    ("Christmas week, holidays excluded", date(2024, 12, 23), date(2024, 12, 27), 4),
                    ("New Year week, holidays excluded", date(2024, 12, 30), date(2025, 1, 3), 4),
                ],
                "result_column": "business_days_count",
            },
        ],
    },
    "lead-time-bucket-v1.0.0.yaml": {
        "input": "booking string, created_at date, check_in date",
        "cases": [
            {
                "config": {"start_column": "created_at", "end_column": "check_in"},
                "rows": [
                    ("booked 4 days ahead", date(2025, 1, 1), date(2025, 1, 5), "Last minute (0-7 days)"),
                    ("booked 45 days ahead", date(2025, 1, 1), date(2025, 2, 15), "1-3 months"),
                    ("booked 5 months ahead", date(2025, 1, 1), date(2025, 6, 1), "3+ months"),
                ],
                "result_column": "lead_time_bucket",
            }
        ],
    },
    "business-days-between-v1.0.0.yaml": {
        "input": "booking string, start date, end date",
        "cases": [
            {
                "config": {"start_column": "start", "end_column": "end", "holidays": "2024-12-25,2025-01-01"},
                "rows": [
                    ("Mon to Fri", date(2025, 1, 6), date(2025, 1, 10), 4),
                    ("Fri to Mon, over a weekend", date(2025, 1, 10), date(2025, 1, 13), 1),
                    ("Christmas week", date(2024, 12, 23), date(2024, 12, 27), 3),
                ],
                "result_column": "business_days",
            }
        ],
    },
}


def operators_to_test():
    """The operator named in the OPERATOR_FILE environment variable, or every
    known operator whose file exists when nobody picked one (plain pytest run)."""
    picked = os.environ.get("OPERATOR_FILE")
    if picked:
        return [picked]
    return [name for name in KNOWN_OPERATORS if any(
        os.path.exists(os.path.join(d, name)) for d in OPERATOR_DIRS
    )]


# 3. The operator under test gives the answers we already know
@pytest.mark.parametrize("file_name", operators_to_test())
def test_known_answers(spark, file_name):
    if file_name not in KNOWN_OPERATORS:
        pytest.skip(
            f"no known answers for {file_name} yet - add a KNOWN_OPERATORS "
            "entry for it in tests/test_operators.py"
        )
    _, run = load_operator(file_name)
    known = KNOWN_OPERATORS[file_name]
    for case in known["cases"]:
        df = spark.createDataFrame([r[:-1] for r in case["rows"]], known["input"])
        got = {
            r["booking"]: r[case["result_column"]]
            for r in run(case["config"], {"data": df}, spark)["data"].collect()
        }
        for row in case["rows"]:
            label, expected = row[0], row[-1]
            assert got[label] == expected, f"{label}: expected {expected}, got {got[label]}"
