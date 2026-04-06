"""
PySpark Structured Streaming Job
=================================
Reads TransitEvent records from Kafka, deserialises Avro, applies
transformations and data-quality checks, then writes to Snowflake in
two sinks:

  1. RAW   → TRANSIT_RAW.EVENTS          (append, 1-to-1 with Kafka)
  2. AGG   → TRANSIT_CURATED.DELAY_AGG   (update, 5-min windowed aggregation)

Run locally:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
                 net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4,\
                 org.apache.spark:spark-avro_2.12:3.4.1 \
      spark/streaming_job.py

Environment variables required — see .env.example
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, TimestampType,
)

from transformations import apply_transformations
from dq_checks import run_dq_checks

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)


# ── Spark session ─────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("NYCTransitStreamingPipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        # Snowflake connector options
        .config("spark.sql.extensions",
                "net.snowflake.spark.snowflake.SnowflakeSourceRelation")
        .getOrCreate()
    )


# ── Avro schema (mirrors avro_schema.avsc) ────────────────────────────────────

TRANSIT_EVENT_SCHEMA = StructType([
    StructField("event_id",       StringType(),    False),
    StructField("route_id",       StringType(),    False),
    StructField("stop_id",        StringType(),    False),
    StructField("stop_name",      StringType(),    False),
    StructField("direction",      StringType(),    False),
    StructField("scheduled_time", LongType(),      False),
    StructField("actual_time",    LongType(),      True),
    StructField("delay_seconds",  IntegerType(),   True),
    StructField("vehicle_id",     StringType(),    True),
    StructField("status",         StringType(),    False),
    StructField("ingested_at",    LongType(),      False),
])


# ── Kafka source ──────────────────────────────────────────────────────────────

def read_kafka(spark: SparkSession):
    kafka_opts = {
        "kafka.bootstrap.servers":       os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "kafka.security.protocol":        "SASL_SSL",
        "kafka.sasl.mechanism":           "PLAIN",
        "kafka.sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{os.environ["KAFKA_API_KEY"]}" '
            f'password="{os.environ["KAFKA_API_SECRET"]}";'
        ),
        "subscribe":            "transit.raw,transit.delays,transit.alerts",
        "startingOffsets":      "latest",
        "failOnDataLoss":       "false",
        "maxOffsetsPerTrigger": "10000",
    }
    return (
        spark.readStream
        .format("kafka")
        .options(**kafka_opts)
        .load()
    )


# ── Deserialise + parse ───────────────────────────────────────────────────────

def parse_events(raw_df):
    """
    Kafka value is Avro-encoded bytes (with Confluent 5-byte magic header).
    We strip the header and use from_avro with the inline schema string.
    """
    from pyspark.sql.avro.functions import from_avro
    schema_path = os.path.join(os.path.dirname(__file__), "..", "producer", "avro_schema.avsc")
    with open(schema_path) as f:
        avro_schema_str = f.read()

    return (
        raw_df
        # strip 5-byte Confluent magic header before deserialising
        .withColumn("avro_bytes", F.expr("substring(value, 6, length(value)-5)"))
        .withColumn("event", from_avro(F.col("avro_bytes"), avro_schema_str))
        .select("event.*", "topic", "partition", "offset", "timestamp")
        # convert epoch-ms to proper timestamps
        .withColumn("scheduled_ts", (F.col("scheduled_time") / 1000).cast(TimestampType()))
        .withColumn("actual_ts",    (F.col("actual_time")    / 1000).cast(TimestampType()))
        .withColumn("ingested_ts",  (F.col("ingested_at")    / 1000).cast(TimestampType()))
        # event-time watermark — tolerate up to 10 min late arrivals
        .withWatermark("scheduled_ts", "10 minutes")
    )


# ── Snowflake writer helper ───────────────────────────────────────────────────

def _sf_options(table: str) -> dict:
    return {
        "sfURL":        os.environ["SNOWFLAKE_URL"],
        "sfUser":       os.environ["SNOWFLAKE_USER"],
        "sfPassword":   os.environ["SNOWFLAKE_PASSWORD"],
        "sfDatabase":   os.environ["SNOWFLAKE_DATABASE"],
        "sfWarehouse":  os.environ["SNOWFLAKE_WAREHOUSE"],
        "sfSchema":     "TRANSIT_RAW",
        "dbtable":      table,
    }


def write_to_snowflake(batch_df, batch_id: int, table: str):
    """foreachBatch writer — runs per micro-batch."""
    row_count = batch_df.count()
    if row_count == 0:
        log.info("Batch %d: empty, skipping write to %s", batch_id, table)
        return
    log.info("Batch %d: writing %d rows to %s", batch_id, row_count, table)
    (
        batch_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**_sf_options(table))
        .mode("append")
        .save()
    )


# ── Aggregation sink ──────────────────────────────────────────────────────────

def build_delay_aggregation(parsed_df):
    """5-minute tumbling window: delay stats per route."""
    return (
        parsed_df
        .filter(F.col("status") == "DELAYED")
        .groupBy(
            F.window("scheduled_ts", "5 minutes"),
            F.col("route_id"),
            F.col("direction"),
        )
        .agg(
            F.count("*").alias("delayed_count"),
            F.avg("delay_seconds").alias("avg_delay_sec"),
            F.max("delay_seconds").alias("max_delay_sec"),
            F.percentile_approx("delay_seconds", 0.95).alias("p95_delay_sec"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Reading from Kafka topics: transit.raw, transit.delays, transit.alerts")
    raw_df    = read_kafka(spark)
    parsed_df = parse_events(raw_df)

    # Apply business transformations (dedup, enrich, normalise)
    transformed_df = apply_transformations(spark, parsed_df)

    # Data quality gate — bad records go to dead-letter, good continue
    clean_df, rejected_df = run_dq_checks(transformed_df)

    # ── Sink 1: raw events → Snowflake ──────────────────────────────────────
    raw_query = (
        clean_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/raw_events")
        .foreachBatch(lambda df, bid: write_to_snowflake(df, bid, "EVENTS"))
        .trigger(processingTime="15 seconds")
        .start()
    )

    # ── Sink 2: 5-min delay aggregations → Snowflake ────────────────────────
    agg_df = build_delay_aggregation(clean_df)
    agg_query = (
        agg_df.writeStream
        .outputMode("update")
        .option("checkpointLocation", "/tmp/checkpoints/delay_agg")
        .foreachBatch(lambda df, bid: write_to_snowflake(df, bid, "DELAY_AGG_5MIN"))
        .trigger(processingTime="60 seconds")
        .start()
    )

    # ── Sink 3: rejected records → dead-letter topic ────────────────────────
    dlq_query = (
        rejected_df.writeStream
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/dead_letter")
        .foreachBatch(lambda df, bid: write_to_snowflake(df, bid, "DEAD_LETTER"))
        .trigger(processingTime="30 seconds")
        .start()
    )

    log.info("All streaming queries started. Awaiting termination...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
