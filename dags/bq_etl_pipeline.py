from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from metadata import get_last_processed, update_metadata
from etl_config import METADATA_CONFIG
import logging

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
    load_to_bq(dfs)

def get_metadata(**context):
    last_ts = get_last_processed('subscriptions')
    context['ti'].xcom_push(key='last_ts', value=last_ts)

logger = logging.getLogger(__name__)

def get_metadata(**context):
    tables_to_track = [
        table for table, config in METADATA_CONFIG['tables'].items() 
        if 'incremental_key' in config
    ]
    last_ts = {
        table: get_last_processed(table)
        for table in tables_to_track
    }
    context['ti'].xcom_push(key='last_ts', value=last_ts)
    logger.info(f"Loaded metadata for tables: {list(last_ts.keys())}")

def update_metadata_task(**context):
    ti = context['ti']
    dfs = ti.xcom_pull(task_ids='transform_task')
    
    for table_name in METADATA_CONFIG['tables']:
        config = METADATA_CONFIG['tables'][table_name]
        
        # Skip tables without incremental_key definition
        if 'incremental_key' not in config:
            continue
            
        df = dfs.get(table_name)
        if df is None or df.empty:
            print(f"Skipping metadata update for {table_name} - no data")
            continue

        # Validating if incremental_key exists in dataframe
        incremental_key = config['incremental_key']
        if incremental_key not in df.columns:
            print(f"WARNING: Skipping {table_name} - missing incremental column {incremental_key}")
            continue

        try:
            max_ts = df[incremental_key].max()
            row_count = len(df)
            update_metadata(table_name, max_ts, row_count)
            print(f"Updated metadata for {table_name} with {row_count} rows")
        except Exception as e:
            print(f"Failed to update metadata for {table_name}: {str(e)}")
            raise

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