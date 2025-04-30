from google.cloud import bigquery
from connect_gc import client
from etl_config import METADATA_CONFIG
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

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
        if result.total_rows > 0:
            ts = next(result).last_processed_ts
            logger.info(f"Last processed timestamp for {table_name}: {ts}")
            return ts
        logger.info(f"No previous timestamp found for {table_name}")
        return None
    except Exception as e:
        logger.error(f"Metadata read error for {table_name}: {str(e)}")
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
        logger.info(f"Updated metadata for {table_name} with {row_count} rows (max_ts={max_ts})")
    except Exception as e:
        logger.error(f"Metadata update failed for {table_name}: {str(e)}")
        raise