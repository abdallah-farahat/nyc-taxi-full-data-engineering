"""
Airflow DAGs of the NYC Taxi project.

nyc_taxi_pipeline : the whole pipeline, source to dashboard, in one run
  check_services -> bronze_batch -> producer -> stream_bronze -> silver -> gold
  -> sync_dims_and_aggregates -> sync_fact -> dbt_run -> dbt_test -> verify_all -> notify_success

Every task: 2 retries (2 minutes apart), a 3-hour timeout, a Telegram alert when it fails.
Nothing is manual: every layer loads only new data, so a re-run is safe.

telegram_alert_test : one task that always fails (to show the failure alert in the demo).
"""

import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


def send_telegram(text):
    """Send a message to Telegram. Never raises: an alert problem must not break the pipeline."""
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN", ""), os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat or "change_me" in (token, chat):
        log.warning("Telegram is not configured, message not sent: %s", text)
        return
    try:
        requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                      data={"chat_id": chat, "text": text}, timeout=15)
    except Exception as error:                                  # noqa: BLE001
        log.warning("Telegram request failed: %s", error)


def alert_on_failure(context):
    """Called by Airflow when a task fails (after the retries are used up)."""
    ti = context["task_instance"]
    send_telegram(f"FAILED  {ti.dag_id} / {ti.task_id}\n"
                  f"attempt {ti.try_number}  run {context['run_id']}\n"
                  f"log: {ti.log_url}")


def notify_success(**context):
    run = context["dag_run"]
    minutes = (datetime.now(run.start_date.tzinfo) - run.start_date).total_seconds() / 60
    send_telegram(f"SUCCESS  {run.dag_id}\nrun {run.run_id}\nduration {minutes:.1f} minutes")


default_args = {
    "owner": "abdallah",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=3),
    "on_failure_callback": alert_on_failure,
}

PY = "cd /app && /usr/local/bin/python -u"
DBT = "cd /app/dbt && /opt/dbt-venv/bin/dbt"

with DAG(
    dag_id="nyc_taxi_pipeline",
    description="Bronze, Silver, Gold, SQL Server serving layer, dbt, verification",
    start_date=datetime(2026, 11, 1),
    schedule="0 2 * * *",          # every day at 02:00 (the DAG starts paused: switch it on in the UI)
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["nyc-taxi", "data-pill"],
) as dag:

    check_services = BashOperator(
        task_id="check_services",
        bash_command=("/usr/local/bin/python -c \"import requests; "
                      "r = requests.get('http://api:8000/health', timeout=15); r.raise_for_status(); "
                      "print('API ok:', r.json())\""),
    )
    bronze_batch = BashOperator(task_id="bronze_batch", bash_command=f"{PY} src/jobs/bronze_batch.py")
    producer = BashOperator(task_id="producer_api_to_kafka", bash_command=f"{PY} src/streaming/producer.py")
    stream_bronze = BashOperator(task_id="stream_into_bronze",
                                 bash_command=f"{PY} src/jobs/stream_bronze.py --once")
    silver = BashOperator(task_id="silver_gate2", bash_command=f"{PY} src/jobs/silver_batch.py")
    gold = BashOperator(task_id="gold_gate3", bash_command=f"{PY} src/jobs/gold_batch.py")
    sync_small = BashOperator(task_id="sync_dims_and_aggregates",
                              bash_command=f"{PY} src/jobs/sync_gold_to_sqlserver.py")
    sync_fact = BashOperator(task_id="sync_fact", bash_command=f"{PY} src/jobs/sync_fact_to_sqlserver.py")
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run --profiles-dir .")
    dbt_test = BashOperator(task_id="dbt_test",
                            bash_command=f"{DBT} test --profiles-dir . --exclude source:gold.fact_trips")
    verify = BashOperator(task_id="verify_all", bash_command=f"{PY} src/jobs/verify_all.py")
    success = PythonOperator(task_id="notify_success", python_callable=notify_success)

    (check_services >> bronze_batch >> producer >> stream_bronze >> silver >> gold
     >> sync_small >> sync_fact >> dbt_run >> dbt_test >> verify >> success)

with DAG(
    dag_id="telegram_alert_test",
    description="Always fails on purpose: shows the Telegram failure alert",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    default_args={**default_args, "retries": 0},
    tags=["demo"],
) as test_dag:
    BashOperator(task_id="fail_on_purpose", bash_command="echo 'simulated failure'; exit 1")