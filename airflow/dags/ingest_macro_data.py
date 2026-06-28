# airflow/dags/ingest_macro_data.py
# DAG 2: Ingest macro economic data daily

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

def run_macro_producer():
    run_script(f"{PROJECT_ROOT}/ingestion/producers/macro_producer.py")

def run_macro_consumer():
    run_script(f"{PROJECT_ROOT}/ingestion/consumers/macro_consumer.py")

with DAG(
    dag_id="ingest_macro_data",
    default_args=default_args,
    description="Ingest UK macro economic indicators from BOE and ONS",
    schedule="0 9 * * 1-5",   # 9am Monday to Friday
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "macro"],
) as dag:

    macro_producer = PythonOperator(
        task_id="run_macro_producer",
        python_callable=run_macro_producer,
    )

    macro_consumer = PythonOperator(
        task_id="run_macro_consumer",
        python_callable=run_macro_consumer,
    )

    macro_producer >> macro_consumer
