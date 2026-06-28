# airflow/dags/ingest_market_data.py

from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import subprocess
import os

PROJECT_ROOT = "/Users/mohammedaminulfaaiz/uk-financial-lakehouse"
PYTHON = os.path.join(PROJECT_ROOT, "venv/bin/python")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_script(script_path):
    result = subprocess.run(
        [PYTHON, script_path],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Script failed:\n{result.stderr}")

def run_ftse_producer():
    run_script(f"{PROJECT_ROOT}/ingestion/producers/ftse_producer.py")

def run_fx_producer():
    run_script(f"{PROJECT_ROOT}/ingestion/producers/fx_producer.py")

def run_ftse_consumer():
    run_script(f"{PROJECT_ROOT}/ingestion/consumers/ftse_consumer.py")

def run_fx_consumer():
    run_script(f"{PROJECT_ROOT}/ingestion/consumers/fx_consumer.py")

with DAG(
    dag_id="ingest_market_data",
    default_args=default_args,
    description="Ingest FTSE and FX data from yfinance into bronze layer",
    schedule="0 8 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "market"],
) as dag:

    ftse_producer = PythonOperator(
        task_id="run_ftse_producer",
        python_callable=run_ftse_producer,
    )

    fx_producer = PythonOperator(
        task_id="run_fx_producer",
        python_callable=run_fx_producer,
    )

    ftse_consumer = PythonOperator(
        task_id="run_ftse_consumer",
        python_callable=run_ftse_consumer,
    )

    fx_consumer = PythonOperator(
        task_id="run_fx_consumer",
        python_callable=run_fx_consumer,
    )

    # Run producers first, then consumers after both producers finish
    ftse_producer >> ftse_consumer
    fx_producer >> fx_consumer
