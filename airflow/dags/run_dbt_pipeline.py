# airflow/dags/run_dbt_pipeline.py
# DAG 3: Run dbt transformations daily, then validate with Great Expectations

from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import subprocess
import os

PROJECT_ROOT = "/Users/mohammedaminulfaaiz/uk-financial-lakehouse"
DBT_DIR = os.path.join(PROJECT_ROOT, "dbt")
DBT = os.path.join(PROJECT_ROOT, "venv/bin/dbt")
PYTHON = os.path.join(PROJECT_ROOT, "venv/bin/python")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_dbt(select=None):
    cmd = [DBT, "run"]
    if select:
        cmd += ["--select", select]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=DBT_DIR
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt failed:\n{result.stderr}")

def run_dbt_bronze():
    run_dbt(select="bronze")

def run_dbt_silver():
    run_dbt(select="silver")

def run_dbt_gold():
    run_dbt(select="gold")

def run_data_quality_checks():
    script_path = f"{PROJECT_ROOT}/great_expectations/run_checkpoint.py"
    result = subprocess.run(
        [PYTHON, script_path],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"Data quality checks failed:\n{result.stdout}\n{result.stderr}")

with DAG(
    dag_id="run_dbt_pipeline",
    default_args=default_args,
    description="Run dbt bronze -> silver -> gold pipeline, then validate with Great Expectations",
    schedule="0 10 * * 1-5",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dbt", "transformation", "data-quality"],
) as dag:

    bronze = PythonOperator(
        task_id="dbt_bronze",
        python_callable=run_dbt_bronze,
    )

    silver = PythonOperator(
        task_id="dbt_silver",
        python_callable=run_dbt_silver,
    )

    gold = PythonOperator(
        task_id="dbt_gold",
        python_callable=run_dbt_gold,
    )

    data_quality = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    bronze >> silver >> gold >> data_quality
