from google.cloud import bigquery
from connect_gc import client
from etl_config import METADATA_CONFIG
from datetime import datetime

def get_last_processed(table_name):
    try:
        query = f"""
            SELECT last_processed_ts 
            FROM {METADATA_CONFIG['dataset_id']}.{METADATA_CONFIG['metadata_table']}
            WHERE table_name = '{table_name}'
            ORDER BY processed_at DESC
            LIMIT 1
        """
        result = client.query(query).result()
        return next(result).last_processed_ts if result.total_rows > 0 else None
    except Exception as e:
        print(f"Metadata read error: {str(e)}")
        return None

def update_metadata(table_name, max_ts, row_count):
    try:
        query = f"""
            INSERT INTO {METADATA_CONFIG['dataset_id']}.{METADATA_CONFIG['metadata_table']}
            (table_name, last_processed_ts, processed_at, row_count)
            VALUES (
                '{table_name}', 
                TIMESTAMP('{max_ts.isoformat()}'), 
                CURRENT_TIMESTAMP(),
                {row_count}
            )
        """
        client.query(query).result()
    except Exception as e:
        print(f"Metadata update error: {str(e)}")
        raise