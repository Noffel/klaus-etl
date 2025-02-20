from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def extract_json(**context):
    """Task 1: Raw data extraction from the JSON file"""
    from extract_json import extract_json
    raw_data = extract_json('/opt/airflow/data/etl.json')
    return raw_data  # XCom auto-serializes small data

def transform_data(**context):
    """Task 2: Data transformation"""
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='extract_task')
    
    from transform_json import process_json
    dfs = process_json(raw_data)
    return dfs

def load_data(**context):
    """Task 3: Data loading to BigQuery tables"""
    ti = context['ti']
    dfs = ti.xcom_pull(task_ids='transform_task')
    
    from load_json import load_to_bq
    load_to_bq(dfs, "klaus_subscriptions")

with DAG(
    'klaus_bq_etl',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id='extract_task',
        python_callable=extract_json,
        provide_context=True
    )

    transform = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
        provide_context=True
    )

    extract >> transform >> load