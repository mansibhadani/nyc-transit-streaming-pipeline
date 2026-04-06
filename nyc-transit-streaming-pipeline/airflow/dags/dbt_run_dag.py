"""
dbt_run_dag.py
==============
Runs the full dbt project on a schedule to refresh curated models:
  1. dbt test  — validate source data freshness & schema
  2. dbt run   — materialise staging views + curated tables
  3. dbt test  — validate outputs
  4. Slack notification on success or failure

Runs hourly; dbt's incremental logic means only new data is processed.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
SLACK_CONN_ID = "slack_webhook"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}


def _slack_success(**context):
    hook = HttpHook(method="POST", http_conn_id=SLACK_CONN_ID)
    hook.run(
        endpoint="",
        data=str({"text": ":white_check_mark: *dbt run succeeded* — curated models refreshed"}),
        headers={"Content-Type": "application/json"},
    )


def _slack_failure(context):
    dag_id = context["dag"].dag_id
    task   = context["task_instance"].task_id
    hook   = HttpHook(method="POST", http_conn_id=SLACK_CONN_ID)
    hook.run(
        endpoint="",
        data=str({"text": f":x: *dbt run FAILED* | DAG: `{dag_id}` | Task: `{task}`"}),
        headers={"Content-Type": "application/json"},
    )


DBT_ENV = {
    "SNOWFLAKE_ACCOUNT":   "{{ var.value.snowflake_account }}",
    "SNOWFLAKE_USER":      "{{ var.value.snowflake_user }}",
    "SNOWFLAKE_PASSWORD":  "{{ var.value.snowflake_password }}",
    "SNOWFLAKE_DATABASE":  "{{ var.value.snowflake_database }}",
    "SNOWFLAKE_WAREHOUSE": "{{ var.value.snowflake_warehouse }}",
    "SNOWFLAKE_ROLE":      "TRANSFORMER",
}

DBT_CMD = (
    f"cd {DBT_PROJECT_DIR} && "
    f"dbt {{subcommand}} --profiles-dir {DBT_PROFILES_DIR} --target prod"
)

with DAG(
    dag_id          = "transit_dbt_run",
    description     = "Hourly dbt run to refresh transit curated models in Snowflake",
    default_args    = DEFAULT_ARGS,
    start_date      = datetime(2024, 1, 1),
    schedule        = "0 * * * *",       # top of every hour
    catchup         = False,
    max_active_runs = 1,
    tags            = ["transit", "dbt", "curated"],
    on_failure_callback = _slack_failure,
) as dag:

    source_test = BashOperator(
        task_id      = "dbt_source_freshness",
        bash_command = DBT_CMD.format(subcommand="source freshness"),
        env          = DBT_ENV,
    )

    run_staging = BashOperator(
        task_id      = "dbt_run_staging",
        bash_command = DBT_CMD.format(subcommand="run --select staging"),
        env          = DBT_ENV,
    )

    test_staging = BashOperator(
        task_id      = "dbt_test_staging",
        bash_command = DBT_CMD.format(subcommand="test --select staging"),
        env          = DBT_ENV,
    )

    run_curated = BashOperator(
        task_id      = "dbt_run_curated",
        bash_command = DBT_CMD.format(subcommand="run --select curated"),
        env          = DBT_ENV,
    )

    test_curated = BashOperator(
        task_id      = "dbt_test_curated",
        bash_command = DBT_CMD.format(subcommand="test --select curated"),
        env          = DBT_ENV,
    )

    notify_success = PythonOperator(
        task_id         = "slack_success",
        python_callable = _slack_success,
    )

    source_test >> run_staging >> test_staging >> run_curated >> test_curated >> notify_success
