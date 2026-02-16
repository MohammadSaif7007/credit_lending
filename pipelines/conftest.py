# framework/conftest.py
import pytest
from framework.spark_session import get_spark

@pytest.fixture(scope="session")
def spark():
    """Reusable Spark session for all tests"""
    spark_session = get_spark()
    yield spark_session
    spark_session.stop()