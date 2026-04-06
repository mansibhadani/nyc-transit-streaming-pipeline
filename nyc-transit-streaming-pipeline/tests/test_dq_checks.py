"""
Unit tests for the TransitDQSuite data quality checks.
"""

import pytest
from pyspark.sql import SparkSession, Row
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))
from dq_checks import TransitDQSuite


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test-dq")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def good_event(**overrides):
    base = {
        "event_id": "evt-001", "route_id": "A", "trip_id": "T1",
        "stop_id": "S1", "stop_name": "Stop", "direction": "N",
        "event_type": "ARRIVAL", "scheduled_time": 1700000000000,
        "actual_time": 1700000100000, "delay_seconds": 100,
        "vehicle_id": "VH-1", "latitude": 40.7, "longitude": -73.9,
        "ingested_at": 1700000110000, "source": "test",
    }
    base.update(overrides)
    return Row(**base)


class TestTransitDQSuite:
    def test_valid_event_passes(self, spark):
        df = spark.createDataFrame([good_event()])
        suite = TransitDQSuite()
        passing, failing, metrics = suite.run(df)
        assert passing.count() == 1
        assert failing.count() == 0
        assert metrics["pass_rate_pct"] == 100.0

    def test_null_event_id_fails(self, spark):
        df = spark.createDataFrame([good_event(event_id=None)])
        suite = TransitDQSuite()
        passing, failing, metrics = suite.run(df)
        assert passing.count() == 0
        assert failing.count() == 1

    def test_out_of_range_delay_fails(self, spark):
        df = spark.createDataFrame([good_event(delay_seconds=99999)])
        suite = TransitDQSuite()
        passing, failing, metrics = suite.run(df)
        assert failing.count() == 1

    def test_invalid_direction_fails(self, spark):
        df = spark.createDataFrame([good_event(direction="X")])
        suite = TransitDQSuite()
        passing, failing, metrics = suite.run(df)
        assert failing.count() == 1

    def test_empty_df_returns_zero_metrics(self, spark):
        df = spark.createDataFrame([], schema=spark.createDataFrame([good_event()]).schema)
        suite = TransitDQSuite()
        passing, failing, metrics = suite.run(df)
        assert metrics["total"] == 0

    def test_mixed_batch_splits_correctly(self, spark):
        events = [
            good_event(event_id="good-1"),
            good_event(event_id="good-2"),
            good_event(event_id=None),
        ]
        df = spark.createDataFrame(events)
        suite = TransitDQSuite()
        passing, failing, metrics = suite.run(df)
        assert passing.count() == 2
        assert failing.count() == 1
        assert round(metrics["pass_rate_pct"], 1) == 66.7
