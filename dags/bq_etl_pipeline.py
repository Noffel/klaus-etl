from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from metadata import get_last_processed, update_metadata

def extract_json(**context):
    last_ts = context['ti'].xcom_pull(task_ids='get_metadata')
    from extract_json import extract_json
    return extract_json('/opt/airflow/data/etl.json', last_ts)

def transform_data(**context):
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='extract_task')
    from transform_json import process_json
    return process_json(raw_data)

def load_data(**context):
    ti = context['ti']
    dfs = ti.xcom_pull(task_ids='transform_task')
    from load_json import load_to_bq
    load_to_bq(dfs, "klaus_subscriptions")

def get_metadata(**context):
    last_ts = get_last_processed('subscriptions')
    context['ti'].xcom_push(key='last_ts', value=last_ts)

def update_metadata_task(**context):
    ti = context['ti']
    dfs = ti.xcom_pull(task_ids='transform_task')
    max_ts = dfs['subscriptions']['updated_at'].max()
    update_metadata('subscriptions', max_ts)

with DAG(
    'klaus_bq_etl',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    get_meta = PythonOperator(
        task_id='get_metadata',
        python_callable=get_metadata,
        provide_context=True
    )

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

    update_meta = PythonOperator(
        task_id='update_metadata',
        python_callable=update_metadata_task,
        provide_context=True
    )

    get_meta >> extract >> transform >> load >> update_meta