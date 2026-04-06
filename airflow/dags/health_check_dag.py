"""
health_check_dag.py
===================
Runs every 5 minutes to verify the streaming pipeline is healthy:
  1. Checks Kafka consumer group lag (alerts if > threshold)
  2. Checks Snowflake row count freshness (alerts if no new rows in 15 min)
  3. Sends Slack notification on any failure

Requires Airflow connections:
  - snowflake_default  (Snowflake hook)
  - slack_webhook      (HTTP hook pointing to your Slack webhook URL)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.http.hooks.http import HttpHook

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
CONSUMER_GROUP          = "spark-streaming-group"
LAG_ALERT_THRESHOLD     = 50_000      # alert if consumer lag > 50k messages
FRESHNESS_MINUTES       = 15          # alert if no new Snowflake rows in 15 min
SNOWFLAKE_CONN_ID       = "snowflake_default"
SLACK_CONN_ID           = "slack_webhook"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}

# ── Helper: Slack alert ───────────────────────────────────────────────────────

def _send_slack_alert(message: str, context: dict) -> None:
    dag_id   = context["dag"].dag_id
    run_id   = context["run_id"]
    payload  = {
        "text": (
            f":rotating_light: *Pipeline Alert* | DAG: `{dag_id}` | Run: `{run_id}`\n"
            f"{message}"
        )
    }
    hook = HttpHook(method="POST", http_conn_id=SLACK_CONN_ID)
    hook.run(endpoint="", data=str(payload), headers={"Content-Type": "application/json"})


# ── Task 1: Snowflake freshness ───────────────────────────────────────────────

def check_snowflake_freshness(**context) -> None:
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql  = f"""
        select count(*) as recent_rows
        from TRANSIT_RAW.EVENTS
        where ingested_ts >= dateadd('minute', -{FRESHNESS_MINUTES}, current_timestamp())
    """
    result      = hook.get_first(sql)
    recent_rows = result[0] if result else 0

    log.info("Snowflake freshness check: %d rows in last %d min", recent_rows, FRESHNESS_MINUTES)

    if recent_rows == 0:
        msg = (
            f":snowflake: *Snowflake freshness FAILED*\n"
            f"No new rows in `TRANSIT_RAW.EVENTS` in the last {FRESHNESS_MINUTES} minutes.\n"
            f"Check the PySpark streaming job."
        )
        _send_slack_alert(msg, context)
        raise ValueError(f"No new Snowflake rows in last {FRESHNESS_MINUTES} minutes")

    log.info("Freshness check PASSED: %d rows", recent_rows)


# ── Task 2: Row count audit ───────────────────────────────────────────────────

def audit_row_counts(**context) -> None:
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql  = """
        select
            table_name,
            row_count,
            bytes / (1024 * 1024) as size_mb,
            last_altered
        from information_schema.tables
        where table_schema = 'TRANSIT_RAW'
        order by last_altered desc
    """
    rows = hook.get_records(sql)
    for table_name, row_count, size_mb, last_altered in rows:
        log.info(
            "Table: %-30s | rows=%10d | size=%.1f MB | last_altered=%s",
            table_name, row_count, size_mb, last_altered,
        )

    # Push to XCom for downstream tasks / monitoring
    context["ti"].xcom_push(key="table_stats", value=[
        {"table": r[0], "rows": r[1]} for r in rows
    ])


# ── Task 3: DQ pass-rate check ────────────────────────────────────────────────

def check_dq_pass_rate(**context) -> None:
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    sql  = """
        with recent as (
            select count(*) as total from TRANSIT_RAW.EVENTS
            where ingested_ts >= dateadd('hour', -1, current_timestamp())
        ),
        rejected as (
            select count(*) as total from TRANSIT_RAW.DEAD_LETTER
            where ingested_ts >= dateadd('hour', -1, current_timestamp())
        )
        select
            recent.total  as clean_count,
            rejected.total as rejected_count,
            round(
                recent.total / nullif(recent.total + rejected.total, 0) * 100, 2
            ) as pass_rate_pct
        from recent, rejected
    """
    result = hook.get_first(sql)
    if not result:
        return

    clean, rejected, pass_rate = result
    log.info("DQ pass rate (last 1h): %.2f%% | clean=%d rejected=%d", pass_rate, clean, rejected)

    if pass_rate < 95.0:
        msg = (
            f":warning: *DQ Pass Rate Alert*\n"
            f"Pass rate dropped to `{pass_rate:.1f}%` (threshold: 95%).\n"
            f"Clean: {clean:,} | Rejected: {rejected:,}\n"
            f"Check `TRANSIT_RAW.DEAD_LETTER` for failure reasons."
        )
        _send_slack_alert(msg, context)
        raise ValueError(f"DQ pass rate {pass_rate:.1f}% below 95% threshold")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id          = "transit_pipeline_health_check",
    description     = "Every-5-min health check for the NYC transit streaming pipeline",
    default_args    = DEFAULT_ARGS,
    start_date      = datetime(2024, 1, 1),
    schedule        = "*/5 * * * *",
    catchup         = False,
    max_active_runs = 1,
    tags            = ["transit", "monitoring", "streaming"],
) as dag:

    t_freshness = PythonOperator(
        task_id         = "check_snowflake_freshness",
        python_callable = check_snowflake_freshness,
    )

    t_audit = PythonOperator(
        task_id         = "audit_row_counts",
        python_callable = audit_row_counts,
    )

    t_dq = PythonOperator(
        task_id         = "check_dq_pass_rate",
        python_callable = check_dq_pass_rate,
    )

    t_freshness >> t_audit >> t_dq
