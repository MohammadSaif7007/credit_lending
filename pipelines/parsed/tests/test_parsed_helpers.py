# pipelines/credit_lending/parsed/tests/test_parsed_helpers.py
import pytest
from pyspark.sql import SparkSession
from pipelines.credit_lending.parsed import python_helper as ph

@pytest.fixture(scope="class")
def spark(request):
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("test_parsed_helpers") \
        .getOrCreate()
    request.cls.spark = spark
    yield
    spark.stop()


@pytest.mark.usefixtures("spark")
class TestParsedHelpers:

    def test_parse_float(self):
        df = self.spark.createDataFrame([("10.5",), ("20.0",)], ["amount"])
        df2 = ph.parse_float(df, "amount")
        values = [row.amount for row in df2.collect()]
        assert all(isinstance(v, float) for v in values)
        assert values == [10.5, 20.0]

    def test_parse_int(self):
        df = self.spark.createDataFrame([("1",), ("2",)], ["transaction_id"])
        df2 = ph.parse_int(df, "transaction_id")
        values = [row.transaction_id for row in df2.collect()]
        assert all(isinstance(v, int) for v in values)
        assert values == [1, 2]

    def test_add_ingestion_date(self):
        df = self.spark.createDataFrame([("Alice",), ("Bob",)], ["name"])
        df2 = ph.add_ingestion_date(df)
        assert "ingestion_date" in df2.columns
        assert all(row.ingestion_date is not None for row in df2.collect())