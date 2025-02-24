from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from connect_gc import client
from transform_json import sanitize_column_name
from etl_config import BQ_SCHEMA, METADATA_CONFIG
import logging

logger = logging.getLogger(__name__)
LOAD_ORDER = ['customers', 'addresses', 'subscriptions', 'subscription_items', 'item_tiers']

PROJECT_ID = METADATA_CONFIG['project_id']
DATASET_ID = METADATA_CONFIG['dataset_id']

def _table_exists(table_name):
    full_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    try:
        client.get_table(full_id)
        return True
    except NotFound:
        return False

def _validate_schema(df, table_name):
    if table_name not in BQ_SCHEMA:
        return
    
    schema = BQ_SCHEMA[table_name]
    type_map = {
        'Int64': 'INT64',
        'int64': 'INT64',
        'float64': 'FLOAT64',
        'datetime64[ns, UTC]': 'TIMESTAMP',
        'datetime64[ns]': 'TIMESTAMP',  # Accept both timestamp types
        'object': 'STRING'
    }
    
    for col, expected_type in schema.items():
        if col not in df.columns:
            raise ValueError(f"Missing column {col} in {table_name}")
            
        actual_dtype = str(df[col].dtype)
        actual_type = type_map.get(actual_dtype, 'STRING')
        
        if actual_type != expected_type:
            raise ValueError(
                f"Schema mismatch in {table_name}.{col}\n"
                f"Expected: {expected_type}\n"
                f"Actual: {actual_dtype} → Mapped: {actual_type}"
            )

def load_to_bq(df_dict):
    try:
        for table_name in LOAD_ORDER:
            df = df_dict.get(table_name)
            if df is None or df.empty:
                logger.info(f"Skipping empty table: {table_name}")
                continue
            
            full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            logger.info(f"Starting load for {full_table_id}")
            
            _validate_schema(df, table_name)
            df.columns = [sanitize_column_name(col) for col in df.columns]

            # Table creation
            if not _table_exists(table_name):
                schema = [
                    bigquery.SchemaField(name=col, field_type=BQ_SCHEMA[table_name][col])
                    for col in BQ_SCHEMA[table_name]
                ]
                table = bigquery.Table(full_table_id, schema=schema)
                client.create_table(table)
                logger.info(f"Created table {full_table_id}")

            # Staging load
            staging_name = f"{table_name}_staging"
            staging_id = f"{PROJECT_ID}.{DATASET_ID}.{staging_name}"
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_TRUNCATE",
                schema=[
                    bigquery.SchemaField(name=col, field_type=BQ_SCHEMA[table_name][col])
                    for col in df.columns
                ]
            )
            
            logger.info(f"Loading {len(df)} rows to staging")
            load_job = client.load_table_from_dataframe(df, staging_id, job_config=job_config)
            load_job.result()

            # Merge logic
            config = METADATA_CONFIG['tables'][table_name]
            pk = config['pk']
            update_cols = [col for col in df.columns if col not in pk]
            
            merge_sql = f"""
                MERGE `{full_table_id}` T
                USING `{staging_id}` S
                ON {' AND '.join([f'T.{col} = S.{col}' for col in pk])}
                WHEN MATCHED AND T.row_hash != S.row_hash THEN
                    UPDATE SET {', '.join([f'{col} = S.{col}' for col in update_cols])}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(df.columns)}) VALUES ({', '.join([f'S.{col}' for col in df.columns])})
            """
            
            logger.info(f"Merging into {table_name}")
            merge_job = client.query(merge_sql)
            merge_job.result()

            # Cleanup
            client.delete_table(staging_id)
            logger.info(f"Staging cleaned for {table_name}")

    except Exception as e:
        logger.error(f"Load failed: {str(e)}")
        raise