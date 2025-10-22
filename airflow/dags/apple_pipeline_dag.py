from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, subprocess

default_args = {
    "owner": "kaancakir",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="apple_retail_pipeline",
    default_args=default_args,
    description="ETL pipeline for Apple Retail data",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    BASE_DIR = "/opt/airflow/src"

    def run_script(script_name):
        subprocess.run(["python", os.path.join(BASE_DIR, script_name)], check=True)

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=run_script,
        op_args=["extract.py"],
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=run_script,
        op_args=["transform.py"],
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=run_script,
        op_args=["load.py"],
    )

    extract_task >> transform_task >> load_task
