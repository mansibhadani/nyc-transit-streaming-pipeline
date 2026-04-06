"""
Data Quality Checks
===================
Applies a suite of Great Expectations-style checks to the streaming
DataFrame inside a foreachBatch call, then splits results into:

  clean_df    → passes all checks → written to Snowflake
  rejected_df → fails any check  → written to dead-letter table

Each check logs a metric so you can track DQ pass-rates over time.
"""

import logging
from dataclasses import dataclass, field
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)


# ── Check registry ────────────────────────────────────────────────────────────

@dataclass
class DQResult:
    check_name: str
    passed: int = 0
    failed: int = 0

    @property
    def pass_rate(self) -> float:
        total = self.passed + self.failed
        return self.passed / total if total else 1.0


# ── Individual check expressions (return boolean Column) ──────────────────────

NOT_NULL_COLS = ["event_id", "route_id", "stop_id", "scheduled_time", "status", "ingested_at"]

VALID_STATUS   = ["ON_TIME", "DELAYED", "CANCELLED", "UNKNOWN"]
VALID_DIRECTION = ["N", "S"]

# Delay sanity: no train is more than 2 hours late or 30 min early
MAX_DELAY_SEC = 7_200
MIN_DELAY_SEC = -1_800


def _not_null_check(df: DataFrame) -> Tuple[DataFrame, DQResult]:
    """All critical columns must be non-null."""
    null_condition = F.lit(False)
    for col in NOT_NULL_COLS:
        null_condition = null_condition | F.col(col).isNull()

    failed_df = df.filter(null_condition).withColumn("dq_failure_reason", F.lit("null_critical_column"))
    clean_df  = df.filter(~null_condition)

    failed_count = failed_df.count()  # materialise count for metrics
    result = DQResult("not_null", df.count() - failed_count, failed_count)
    return clean_df, failed_df, result


def _valid_status_check(df: DataFrame) -> Tuple[DataFrame, DataFrame, DQResult]:
    condition = F.col("status").isin(VALID_STATUS)
    clean_df  = df.filter(condition)
    failed_df = (
        df.filter(~condition)
        .withColumn("dq_failure_reason", F.lit("invalid_status_value"))
    )
    failed_count = failed_df.count()
    result = DQResult("valid_status", df.count() - failed_count, failed_count)
    return clean_df, failed_df, result


def _valid_direction_check(df: DataFrame) -> Tuple[DataFrame, DataFrame, DQResult]:
    condition = F.col("direction").isin(VALID_DIRECTION)
    clean_df  = df.filter(condition)
    failed_df = (
        df.filter(~condition)
        .withColumn("dq_failure_reason", F.lit("invalid_direction_value"))
    )
    failed_count = failed_df.count()
    result = DQResult("valid_direction", df.count() - failed_count, failed_count)
    return clean_df, failed_df, result


def _delay_range_check(df: DataFrame) -> Tuple[DataFrame, DataFrame, DQResult]:
    """Rows with a non-null delay must fall within plausible bounds."""
    condition = (
        F.col("delay_seconds").isNull()
        | F.col("delay_seconds").between(MIN_DELAY_SEC, MAX_DELAY_SEC)
    )
    clean_df  = df.filter(condition)
    failed_df = (
        df.filter(~condition)
        .withColumn("dq_failure_reason", F.lit("delay_out_of_range"))
    )
    failed_count = failed_df.count()
    result = DQResult("delay_range", df.count() - failed_count, failed_count)
    return clean_df, failed_df, result


def _scheduled_time_check(df: DataFrame) -> Tuple[DataFrame, DataFrame, DQResult]:
    """scheduled_time must be a plausible epoch ms (after 2020-01-01)."""
    min_epoch_ms = 1_577_836_800_000  # 2020-01-01 UTC
    condition = F.col("scheduled_time") >= min_epoch_ms
    clean_df  = df.filter(condition)
    failed_df = (
        df.filter(~condition)
        .withColumn("dq_failure_reason", F.lit("scheduled_time_implausible"))
    )
    failed_count = failed_df.count()
    result = DQResult("scheduled_time", df.count() - failed_count, failed_count)
    return clean_df, failed_df, result


# ── DQ pipeline orchestrator ──────────────────────────────────────────────────

def run_dq_checks(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Run all checks sequentially. Returns (clean_df, rejected_df).
    Rejected rows carry a dq_failure_reason column for debugging.
    """
    results: list[DQResult] = []
    rejected_frames = []

    checks = [
        _not_null_check,
        _valid_status_check,
        _valid_direction_check,
        _delay_range_check,
        _scheduled_time_check,
    ]

    # Add metadata column for dead-letter tracking
    if "dq_failure_reason" not in df.columns:
        df = df.withColumn("dq_failure_reason", F.lit(None).cast("string"))

    for check_fn in checks:
        df, failed, result = check_fn(df)
        results.append(result)
        if failed is not None:
            rejected_frames.append(failed)

    # Log summary
    for r in results:
        log.info(
            "DQ | %-25s | passed=%d failed=%d pass_rate=%.2f%%",
            r.check_name, r.passed, r.failed, r.pass_rate * 100,
        )

    overall_pass = sum(r.passed for r in results)
    overall_fail = sum(r.failed for r in results)
    log.info("DQ SUMMARY | clean=%d rejected=%d", overall_pass, overall_fail)

    rejected_df = rejected_frames[0]
    for frame in rejected_frames[1:]:
        rejected_df = rejected_df.union(frame)

    return df, rejected_df
