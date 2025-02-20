from pathlib import Path

BASE_PATH = Path('/opt/airflow/data')
JSON_PATH = BASE_PATH / 'etl.json'

BQ_CONFIG = {
    'dataset_id': 'klaus_subscriptions',
    'autodetect': True,
    'write_disposition': 'WRITE_TRUNCATE'
}
