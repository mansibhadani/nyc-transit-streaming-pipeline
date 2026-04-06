"""
Transformations
===============
All business-logic transformations applied to the parsed streaming
DataFrame before it reaches the data-quality gate and Snowflake sink.

Kept separate from streaming_job.py so each function is unit-testable.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Stop-name lookup (broadcast join) ────────────────────────────────────────

def _load_stop_lookup(spark: SparkSession) -> DataFrame:
    """
    Loads the MTA GTFS stops.txt as a small broadcast DataFrame.
    In production this lives in Snowflake; locally we read the CSV.
    """
    try:
        stops = (
            spark.read
            .option("header", "true")
            .csv("data/stops.txt")          # bundled in repo under data/
            .select(
                F.col("stop_id"),
                F.col("stop_name").alias("stop_name_lookup"),
                F.col("stop_lat").cast("double"),
                F.col("stop_lon").cast("double"),
            )
        )
        return F.broadcast(stops)
    except Exception:
        return None


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(df: DataFrame) -> DataFrame:
    """
    Drop exact duplicate event_ids within a watermarked micro-batch.
    Uses dropDuplicatesWithinWatermark (Spark 3.5+) for stateful dedup;
    falls back to dropDuplicates for earlier versions.
    """
    try:
        return df.dropDuplicatesWithinWatermark(["event_id"])
    except AttributeError:
        return df.dropDuplicates(["event_id"])


# ── Enrichment ────────────────────────────────────────────────────────────────

def enrich_stop_name(df: DataFrame, stop_lookup) -> DataFrame:
    """Left-join broadcast stop lookup to fill stop_name from GTFS data."""
    if stop_lookup is None:
        return df
    return (
        df.join(stop_lookup, on="stop_id", how="left")
        .withColumn(
            "stop_name",
            F.coalesce(F.col("stop_name_lookup"), F.col("stop_name")),
        )
        .drop("stop_name_lookup")
    )


# ── Derived columns ───────────────────────────────────────────────────────────

def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add columns that make downstream analytics / dbt models simpler:
    - delay_category: bucketed delay severity
    - hour_of_day / day_of_week: for time-series aggregations
    - is_peak_hour: morning (6-10) and evening (16-20) rush flag
    - processing_lag_sec: wall-clock latency from event to Spark processing
    """
    return (
        df
        .withColumn(
            "delay_category",
            F.when(F.col("delay_seconds").isNull(),            "UNKNOWN")
             .when(F.col("delay_seconds") <= 0,                "EARLY_OR_ON_TIME")
             .when(F.col("delay_seconds").between(1, 120),     "MINOR")        # <2 min
             .when(F.col("delay_seconds").between(121, 300),   "MODERATE")     # 2-5 min
             .when(F.col("delay_seconds").between(301, 600),   "SIGNIFICANT")  # 5-10 min
             .otherwise("SEVERE"),                                              # >10 min
        )
        .withColumn("hour_of_day",  F.hour("scheduled_ts"))
        .withColumn("day_of_week",  F.dayofweek("scheduled_ts"))  # 1=Sun … 7=Sat
        .withColumn("date_local",   F.to_date("scheduled_ts"))
        .withColumn(
            "is_peak_hour",
            F.col("hour_of_day").between(6, 9) | F.col("hour_of_day").between(16, 19),
        )
        .withColumn(
            "processing_lag_sec",
            (F.unix_timestamp(F.current_timestamp()) - F.col("ingested_at") / 1000).cast("int"),
        )
    )


# ── Normalise strings ─────────────────────────────────────────────────────────

def normalise_strings(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("route_id",  F.upper(F.trim(F.col("route_id"))))
        .withColumn("stop_id",   F.trim(F.col("stop_id")))
        .withColumn("stop_name", F.initcap(F.trim(F.col("stop_name"))))
        .withColumn("status",    F.upper(F.trim(F.col("status"))))
        .withColumn("direction", F.upper(F.trim(F.col("direction"))))
    )


# ── Orchestrator ──────────────────────────────────────────────────────────────

def apply_transformations(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Apply all transformations in order. Called from streaming_job.py."""
    stop_lookup = _load_stop_lookup(spark)

    df = deduplicate(df)
    df = normalise_strings(df)
    df = enrich_stop_name(df, stop_lookup)
    df = add_derived_columns(df)
    return df
