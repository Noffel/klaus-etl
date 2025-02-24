import pandas as pd
import json
from flatten_json import flatten
from etl_config import BQ_SCHEMA, METADATA_CONFIG

def sanitize_column_name(name):
    """Clean column names for BigQuery compatibility"""
    sanitized = ''.join([c if c.isalnum() else '_' for c in str(name)])
    if sanitized and (sanitized[0].isdigit() or sanitized[0] == '_'):
        sanitized = 'col_' + sanitized
    return sanitized[:128]

def convert_column(series, target_type):
    """Convert Pandas series with simplified timestamp handling"""
    try:
        if target_type == 'INT64':
            return pd.to_numeric(series, errors='coerce').astype('Int64')
        elif target_type == 'FLOAT64':
            return pd.to_numeric(series, errors='coerce').astype('float64')
        elif target_type == 'TIMESTAMP':
            series = pd.to_datetime(series, unit='s', errors='coerce', utc=True)
            return series.dt.floor('us')
        elif target_type == 'STRING':
            return series.astype(str)
        else:
            return series
    except Exception as e:
        print(f"Error converting {series.name} to {target_type}: {str(e)}")
        return series

def enforce_schema(df, table_name):
    """Ensure DataFrame contains required columns"""
    if table_name not in BQ_SCHEMA:
        return df
    
    schema_cols = list(BQ_SCHEMA[table_name].keys())
    
    for col in schema_cols:
        if col not in df.columns:
            dtype = BQ_SCHEMA[table_name][col]
            null_value = pd.NA if dtype in ['INT64', 'FLOAT64'] else None
            df[col] = null_value
    
    return df.reindex(columns=schema_cols)

def _clean_subscriptions(df):
    """Remove redundant formatted date columns"""
    cols_to_drop = [
        'created_at_formatted', 'created_at_date',
        'started_at_formatted', 'started_at_date',
        'updated_at_formatted', 'updated_at_date'
    ]
    return df.drop(columns=[c for c in cols_to_drop if c in df.columns])

def process_json(raw_data):
    """Main transformation with column cleanup"""
    try:
        main_records = []
        for entry in raw_data.get('list', []):
            # Excluding nested arrays from subscription data
            sub_data = {k: v for k, v in entry['subscription'].items() 
                       if k not in ['subscription_items', 'item_tiers']}
            
            sub = flatten(sub_data, separator='_')
            cust = flatten(entry.get('customer', {}), separator='_')
            
            combined = {
                **{'subscription_' + k: v for k, v in sub.items()},
                **{'customer_' + k: v for k, v in cust.items()}
            }
            main_records.append(combined)

        main_df = pd.DataFrame(main_records)
        
        if main_df.empty:
            print("Warning: No data found after flattening JSON")
            return {table: pd.DataFrame() for table in BQ_SCHEMA.keys()}

        # Splitting into normalized tables
        entities = {
            'subscriptions': [c for c in main_df if c.startswith('subscription_')],
            'customers': [c for c in main_df 
                         if c.startswith('customer_') 
                         and not c.startswith('customer_billing_address_')],
            'addresses': [c for c in main_df 
                         if c.startswith('customer_billing_address_')]
        }

        dfs = {}
        for name, cols in entities.items():
            if cols:
                dfs[name] = main_df[cols].copy()
                dfs[name].columns = [col.split('_', 1)[1] for col in cols]
            else:
                dfs[name] = pd.DataFrame()

            # Schema enforcement
            dfs[name] = enforce_schema(dfs[name], name)
            for col, dtype in BQ_SCHEMA.get(name, {}).items():
                if col in dfs[name].columns:
                    dfs[name][col] = convert_column(dfs[name][col], dtype)

        # Cleaning subscriptions table
        if 'subscriptions' in dfs:
            dfs['subscriptions'] = _clean_subscriptions(dfs['subscriptions'])

        # Handle nested arrays in subscriptions
        def normalize_sub_items(record_path, table_name):
            try:
                df = pd.json_normalize(
                    raw_data['list'],
                    record_path=['subscription', record_path],
                    meta=[['subscription', 'id']],
                    meta_prefix='sub_'
                ).rename(columns={'sub_subscription.id': 'subscription_id'})
                
                if table_name in BQ_SCHEMA:
                    for col, dtype in BQ_SCHEMA[table_name].items():
                        if col in df.columns:
                            df[col] = convert_column(df[col], dtype)
                return enforce_schema(df, table_name)
            except (KeyError, json.JSONDecodeError) as e:
                print(f"Warning: Missing key in {record_path}: {str(e)}")
                return enforce_schema(pd.DataFrame(), table_name)

        dfs['subscription_items'] = normalize_sub_items('subscription_items', 'subscription_items')
        dfs['item_tiers'] = normalize_sub_items('item_tiers', 'item_tiers')

        # Adding relationships
        if 'subscriptions' in dfs and not dfs['subscriptions'].empty:
            dfs['subscriptions']['customer_id'] = main_df.get('customer_id', pd.NA)
        if 'addresses' in dfs and not dfs['addresses'].empty:
            dfs['addresses']['customer_id'] = main_df.get('customer_id', pd.NA)

        # Hash generation
        for table_name, df in dfs.items():
            if df.empty:
                df['row_hash'] = pd.NA
                continue
                
            config = METADATA_CONFIG['tables'].get(table_name, {})
            hash_cols = config.get('hash_columns', df.columns.tolist())
            valid_cols = [c for c in hash_cols if c in df.columns]
            
            try:
                clean_df = df[valid_cols].fillna('')
                hasher = pd.util.hash_pandas_object(clean_df, index=False)
                df['row_hash'] = hasher.astype('int64')
            except Exception as e:
                print(f"Hash error in {table_name}: {str(e)}")
                df['row_hash'] = pd.NA

        return dfs

    except Exception as e:
        print(f"Critical transformation error: {str(e)}")
        raise