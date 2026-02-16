import sys
import os
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from python_helper import (
    parse_float,
    add_ingestion_date,
    parse_int
)
from framework.engine import run_validations


# ----------------------------
# Helper Function Tests
# ----------------------------

def test_parse_float(spark):
    df = spark.createDataFrame([("10",), ("20",),("10.1",)], ["amount"])
    df = parse_float(df, "amount")
    assert df.schema["amount"].dataType.simpleString() == "double"

def test_parse_int(spark):
    df = spark.createDataFrame([("10",), ("20",),("10.1",)], ["amount"])
    df = parse_int(df, "amount")
    assert df.schema["amount"].dataType.simpleString() == "int"

def test_add_ingestion_date(spark):
    df = spark.createDataFrame([(1,), (2,)], ["col"])
    df = add_ingestion_date(df)
    assert "ingestion_date" in df.columns

# ----------------------------
# Data Quality Validation Test
# ----------------------------

def test_validation_fail_raises_exception(spark):
    df = spark.createDataFrame(
        [(1, "Alice"), (None, "Bob")],
        ["client_id", "name"]
    )

    validations = [
        {
            "name": "check_null_client_id",
            "condition": "client_id IS NULL",
            "severity": "fail"
        }
    ]

    with pytest.raises(Exception):
        run_validations(df, "clients", validations)

def test_validation_warn_does_not_raise(spark):
    df = spark.createDataFrame(
        [(1, "Alice"), (None, "Bob")],
        ["client_id", "name"]
    )

    validations = [
        {
            "name": "check_null_client_id_warn",
            "condition": "client_id IS NULL",
            "severity": "warn"
        }
    ]

    # Should NOT raise
    run_validations(df, "clients", validations)