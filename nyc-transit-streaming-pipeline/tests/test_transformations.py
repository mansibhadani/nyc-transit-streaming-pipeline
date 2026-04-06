"""
Unit tests for spark/transformations.py
Run: pytest tests/ -v
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    IntegerType, BooleanType, TimestampType,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("transit-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture
def sample_events(spark):
    """A small DataFrame that mirrors the parsed Kafka events schema."""
    data = [
        {
            "event_id":       "evt-001",
            "route_id":       " a ",        # intentional whitespace + lowercase
            "stop_id":        "A01N",
            "stop_name":      "  times sq  ",
            "direction":      "n",           # lowercase
            "scheduled_time": 1_700_000_000_000,
            "actual_time":    1_700_000_180_000,
            "delay_seconds":  180,
            "vehicle_id":     "train-1",
            "status":         "delayed",    # lowercase
            "ingested_at":    1_700_000_001_000,
            "kafka_topic":    "transit.delays",
            "kafka_partition": 0,
            "kafka_offset":   42,
        },
        {
            "event_id":       "evt-002",
            "route_id":       "L",
            "stop_id":        "L06N",
            "stop_name":      "union sq",
            "direction":      "N",
            "scheduled_time": 1_700_000_000_000,
            "actual_time":    None,
            "delay_seconds":  None,
            "vehicle_id":     None,
            "status":         "ON_TIME",
            "ingested_at":    1_700_000_001_000,
            "kafka_topic":    "transit.raw",
            "kafka_partition": 1,
            "kafka_offset":   10,
        },
        {
            "event_id":       "evt-001",   # duplicate of evt-001
            "route_id":       "A",
            "stop_id":        "A01N",
            "stop_name":      "Times Sq",
            "direction":      "N",
            "scheduled_time": 1_700_000_000_000,
            "actual_time":    1_700_000_180_000,
            "delay_seconds":  180,
            "vehicle_id":     "train-1",
            "status":         "DELAYED",
            "ingested_at":    1_700_000_002_000,  # later ingestion
            "kafka_topic":    "transit.delays",
            "kafka_partition": 0,
            "kafka_offset":   43,
        },
    ]
    schema = StructType([
        StructField("event_id",        StringType(), False),
        StructField("route_id",        StringType(), False),
        StructField("stop_id",         StringType(), False),
        StructField("stop_name",       StringType(), False),
        StructField("direction",       StringType(), False),
        StructField("scheduled_time",  LongType(),   False),
        StructField("actual_time",     LongType(),   True),
        StructField("delay_seconds",   IntegerType(),True),
        StructField("vehicle_id",      StringType(), True),
        StructField("status",          StringType(), False),
        StructField("ingested_at",     LongType(),   False),
        StructField("kafka_topic",     StringType(), True),
        StructField("kafka_partition", IntegerType(),True),
        StructField("kafka_offset",    LongType(),   True),
    ])
    df = spark.createDataFrame(data, schema)
    # Add timestamp columns that the real pipeline derives from epoch-ms
    df = (
        df
        .withColumn("scheduled_ts", (F.col("scheduled_time") / 1000).cast(TimestampType()))
        .withColumn("actual_ts",    (F.col("actual_time")    / 1000).cast(TimestampType()))
        .withColumn("ingested_ts",  (F.col("ingested_at")    / 1000).cast(TimestampType()))
        .withColumn("dq_failure_reason", F.lit(None).cast(StringType()))
    )
    return df


# ── Import the module under test ──────────────────────────────────────────────
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark"))
from transformations import deduplicate, normalise_strings, add_derived_columns


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestDeduplicate:
    def test_removes_duplicate_event_ids(self, spark, sample_events):
        deduped = deduplicate(sample_events)
        count = deduped.count()
        assert count == 2, f"Expected 2 unique events, got {count}"

    def test_keeps_all_columns(self, spark, sample_events):
        deduped = deduplicate(sample_events)
        assert set(deduped.columns) == set(sample_events.columns)


class TestNormaliseStrings:
    def test_route_id_uppercased_and_trimmed(self, spark, sample_events):
        result = normalise_strings(sample_events)
        routes = {r.route_id for r in result.select("route_id").collect()}
        assert " a " not in routes
        assert "A" in routes

    def test_status_uppercased(self, spark, sample_events):
        result = normalise_strings(sample_events)
        statuses = {r.status for r in result.select("status").collect()}
        assert "delayed" not in statuses
        assert "DELAYED" in statuses

    def test_direction_uppercased(self, spark, sample_events):
        result = normalise_strings(sample_events)
        directions = {r.direction for r in result.select("direction").collect()}
        assert "n" not in directions
        assert "N" in directions

    def test_stop_name_trimmed(self, spark, sample_events):
        result = normalise_strings(sample_events)
        names = {r.stop_name for r in result.select("stop_name").collect()}
        for name in names:
            assert name == name.strip(), f"stop_name '{name}' has leading/trailing whitespace"


class TestDerivedColumns:
    def test_delay_category_severe(self, spark, sample_events):
        # evt-001 has delay_seconds=180 → MODERATE
        result = add_derived_columns(normalise_strings(sample_events))
        row = result.filter(F.col("event_id") == "evt-001").first()
        assert row is not None
        assert row.delay_category == "MODERATE"

    def test_delay_category_null_is_unknown(self, spark, sample_events):
        # evt-002 has delay_seconds=None → UNKNOWN
        result = add_derived_columns(normalise_strings(sample_events))
        row = result.filter(F.col("event_id") == "evt-002").first()
        assert row is not None
        assert row.delay_category == "UNKNOWN"

    def test_hour_of_day_is_integer(self, spark, sample_events):
        result = add_derived_columns(sample_events)
        hours = [r.hour_of_day for r in result.select("hour_of_day").collect()]
        for h in hours:
            assert 0 <= h <= 23, f"hour_of_day {h} out of range"

    def test_is_peak_hour_is_boolean(self, spark, sample_events):
        result = add_derived_columns(sample_events)
        peaks = [r.is_peak_hour for r in result.select("is_peak_hour").collect()]
        for p in peaks:
            assert isinstance(p, bool)

    def test_all_expected_columns_present(self, spark, sample_events):
        result = add_derived_columns(sample_events)
        required = {"delay_category", "hour_of_day", "day_of_week",
                    "date_local", "is_peak_hour", "processing_lag_sec"}
        missing = required - set(result.columns)
        assert not missing, f"Missing columns: {missing}"


class TestDQChecks:
    """Integration-level tests for the DQ gate."""

    def test_null_event_id_is_rejected(self, spark):
        from dq_checks import run_dq_checks
        data = [{"event_id": None, "route_id": "A", "stop_id": "A01",
                 "scheduled_time": 1_700_000_000_000, "status": "ON_TIME",
                 "ingested_at": 1_700_000_001_000}]
        schema = StructType([
            StructField("event_id",       StringType(), True),
            StructField("route_id",       StringType(), False),
            StructField("stop_id",        StringType(), False),
            StructField("scheduled_time", LongType(),   False),
            StructField("status",         StringType(), False),
            StructField("ingested_at",    LongType(),   False),
        ])
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("dq_failure_reason", F.lit(None).cast(StringType()))
        # Add missing columns with defaults so checks don't error
        for col_name in ["direction", "delay_seconds", "actual_time", "vehicle_id",
                         "stop_name", "scheduled_ts", "actual_ts", "ingested_ts"]:
            df = df.withColumn(col_name, F.lit(None).cast(StringType()))
        df = df.withColumn("scheduled_ts", F.lit(None).cast(TimestampType()))

        clean_df, rejected_df = run_dq_checks(df)
        assert rejected_df.count() >= 1
        assert clean_df.count() == 0

    def test_valid_row_passes_all_checks(self, spark, sample_events):
        from dq_checks import run_dq_checks
        normalised = normalise_strings(sample_events)
        enriched   = add_derived_columns(normalised)
        # Use only evt-002 which has ON_TIME status and no nulls on critical cols
        single = enriched.filter(F.col("event_id") == "evt-002")
        clean_df, rejected_df = run_dq_checks(single)
        assert clean_df.count() == 1
        assert rejected_df.count() == 0
