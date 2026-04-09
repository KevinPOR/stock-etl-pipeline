from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import os

# -------------------------
# Default args
# -------------------------
default_args = {
    'owner': 'Karim',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# -------------------------
# DAG definition
# -------------------------
dag = DAG(
    'stock_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline: Yahoo Finance -> Postgres -> Transform',
    schedule_interval='0 8 * * *',  # daily at 8 AM
    start_date=datetime(2026, 4, 8),
    catchup=False
)

# -------------------------
# Helper to run Python scripts
# -------------------------
def run_script(script_path):
    """Run a Python script using subprocess"""
    full_path = os.path.abspath(script_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Script not found: {full_path}")
    
    print(f"Running {full_path}...")
    result = subprocess.run(["python", full_path], capture_output=True, text=True)
    
    if result.returncode != 0:
        print(f"❌ Error in {script_path}")
        print(result.stderr)
        raise Exception(f"Script failed: {script_path}")
    
    print(f"✅ {script_path} completed successfully")
    print(result.stdout)

# -------------------------
# Define DAG tasks
# -------------------------
ingest_task = PythonOperator(
    task_id='ingest_yahoo_data',
    python_callable=lambda: run_script("scripts/ingestion.py"),
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=lambda: run_script("scripts/load_to_postgres.py"),
    dag=dag,
)

dim_task = PythonOperator(
    task_id='dim_tickers',
    python_callable=lambda: run_script("scripts/dim_tickers.py"),
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=lambda: run_script("scripts/transform.py"),
    dag=dag,
)

# -------------------------
# Set task dependencies
# -------------------------
ingest_task >> load_task >> dim_task >> transform_task