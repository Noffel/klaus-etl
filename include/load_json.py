from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from connect_gc import client
from transform_json import sanitize_column_name
from etl_config import BQ_SCHEMA, METADATA_CONFIG

LOAD_ORDER = ['customers', 'addresses', 'subscriptions', 'subscription_items', 'item_tiers']

def _table_exists(dataset_id, table_name):
    try:
        client.get_table(f"{dataset_id}.{table_name}")
        return True
    except NotFound:
        return False

def _validate_schema(df, table_name):
    """Ensure DataFrame matches BigQuery schema"""
    if table_name not in BQ_SCHEMA:
        return
    
    schema = BQ_SCHEMA[table_name]
    type_map = {
        'Int64': 'INT64',
        'int64': 'INT64',
        'float64': 'FLOAT64',
        'datetime64[ns, UTC]': 'TIMESTAMP',
        'datetime64[ns]': 'DATE',  # Map datetime64 (without timezone) to DATE
        'object': 'STRING'
    }
    
    for col, expected_type in schema.items():
        if col not in df.columns:
            raise ValueError(f"Missing required column {col} in {table_name}")
            
        actual_dtype = str(df[col].dtype)
        actual_type = type_map.get(actual_dtype, 'STRING')
        
        if actual_type != expected_type:
            raise ValueError(
                f"Schema mismatch in {table_name}.{col}\n"
                f"Expected: {expected_type}\n"
                f"Actual: {actual_dtype} → Mapped: {actual_type}"
            )

def load_to_bq(df_dict, dataset_id):
    for table_name in LOAD_ORDER:
        df = df_dict.get(table_name)
        if df is None or df.empty:
            print(f"Skipping empty table: {table_name}")
            continue

        # Validate schema before processing
        _validate_schema(df, table_name)
        
        # Sanitize column names
        df.columns = [sanitize_column_name(col) for col in df.columns]
        
        # Create table if missing
        if not _table_exists(dataset_id, table_name):
            schema = [
                bigquery.SchemaField(name=col, field_type=BQ_SCHEMA[table_name][col])
                for col in BQ_SCHEMA[table_name]
            ]
            table_ref = client.dataset(dataset_id).table(table_name)
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            print(f"Created table {table_name} with schema")

        # Loading to staging
        staging_name = f"{table_name}_staging"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField(name=col, field_type=BQ_SCHEMA[table_name][col])
                for col in df.columns
            ]
        )
        staging_ref = client.dataset(dataset_id).table(staging_name)
        load_job = client.load_table_from_dataframe(
            df, staging_ref, job_config=job_config
        )
        load_job.result()

        # Execute the merge logic
        config = METADATA_CONFIG['tables'][table_name]
        pk = config['pk']
        update_cols = [col for col in df.columns if col not in pk]
        
        # Building SELECT clause based on table
        if table_name == 'subscriptions':
            select_clause = """
                SELECT 
                    * EXCEPT(created_at_date, started_at_date, updated_at_date),
                    CAST(created_at_date AS DATE) AS created_at_date,
                    CAST(started_at_date AS DATE) AS started_at_date,
                    CAST(updated_at_date AS DATE) AS updated_at_date
            """
        else:
            select_clause = "SELECT *"
        
        merge_sql = f"""
            MERGE `{dataset_id}.{table_name}` T
            USING (
                {select_clause}
                FROM `{dataset_id}.{staging_name}`
            ) S
            ON {' AND '.join([f'T.{col} = S.{col}' for col in pk])}
            WHEN MATCHED AND T.row_hash != S.row_hash THEN
                UPDATE SET {', '.join([f'{col} = S.{col}' for col in update_cols])}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(df.columns)}) VALUES ({', '.join([f'S.{col}' for col in df.columns])})
        """
        client.query(merge_sql).result()
        
        # Cleanup staging
        client.delete_table(staging_ref)
        print(f"Successfully merged {len(df)} rows into {table_name}")