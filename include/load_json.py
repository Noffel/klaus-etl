from google.cloud import bigquery
from connect_gc import client 
from transform_json import sanitize_column_name


def load_to_bq(df_dict, dataset_id):
    if not df_dict:
        raise ValueError("No DataFrames received for loading...")
    
    print(f"Received {len(df_dict)} tables to load")
    
    dataset_ref = client.dataset(dataset_id)
    
    for table_name, df in df_dict.items():
        if df.empty:
            print(f"Skipping empty table: {table_name}")
            continue        
        df.columns = [sanitize_column_name(col) for col in df.columns]
        table_ref = dataset_ref.table(table_name)
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )
        
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
                
        job.result()
        print(f"Loaded {df.shape[0]} rows to {dataset_id}.{table_name}")