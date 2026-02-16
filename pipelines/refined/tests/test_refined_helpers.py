import sys
import os
import pytest
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from python_helper import (
    remove_rows_with_null,
)
from framework.engine import run_validations


# ----------------------------
# Helper Function Tests
# ----------------------------

def test_remove_rows_with_null(spark):
    df = spark.createDataFrame([("10",), ("20",),("10.1",)], ["amount"])
    df = remove_rows_with_null(df, "amount")
    assert df.schema["amount"].dataType.simpleString() == "string"

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