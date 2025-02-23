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
    """Convert Pandas series to match BigQuery schema with precise type handling"""
    try:
        if target_type == 'INT64':
            return pd.to_numeric(series, errors='coerce').astype('Int64')
        elif target_type == 'FLOAT64':
            return pd.to_numeric(series, errors='coerce').astype('float64')
        elif target_type == 'TIMESTAMP':
            # To handle Unix timestamps in SECONDS
            series = pd.to_datetime(series, unit='s', errors='coerce', utc=True)
            return series.dt.floor('us')  # Truncate to microseconds
        elif target_type == 'DATE':
            series = pd.to_datetime(series, unit='s', errors='coerce').dt.tz_localize(None)
            return series.dt.date
        elif target_type == 'STRING':
            return series.astype(str)
        else:
            return series
    except Exception as e:
        print(f"Error converting {series.name} to {target_type}: {str(e)}")
        return series

def enforce_schema(df, table_name):
    """Ensure DataFrame contains all columns from BQ_SCHEMA with proper null handling"""
    if table_name not in BQ_SCHEMA:
        return df
    
    schema_cols = list(BQ_SCHEMA[table_name].keys())
    
    # Add missing columns with appropriate null values
    for col in schema_cols:
        if col not in df.columns:
            dtype = BQ_SCHEMA[table_name][col]
            null_value = pd.NA if dtype in ['INT64', 'FLOAT64'] else None
            df[col] = null_value
    
    # Ensure correct column order
    return df.reindex(columns=schema_cols)

def process_json(raw_data):
    """Main transformation function with robust schema enforcement"""
    try:
        # Flatten JSON structure with explicit hierarchy preservation
        main_records = []
        for entry in raw_data.get('list', []):
            # Flatten with explicit nested structure preservation
            sub = flatten(entry.get('subscription', {}), separator='_')
            cust = flatten(entry.get('customer', {}), separator='_')
            
            # Combine with explicit prefixes
            combined = {
                **{'subscription_' + k: v for k, v in sub.items()},
                **{'customer_' + k: v for k, v in cust.items()}
            }
            main_records.append(combined)

        main_df = pd.DataFrame(main_records)
        
        if main_df.empty:
            print("Warning: No data found after flattening JSON")
            return {table: pd.DataFrame() for table in BQ_SCHEMA.keys()}

        # Split into normalized tables with explicit column handling
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
                # Remove prefix after splitting
                dfs[name].columns = [col.split('_', 1)[1] for col in cols]
            else:
                dfs[name] = pd.DataFrame()

            # Enforce schema and type conversions
            dfs[name] = enforce_schema(dfs[name], name)
            for col, dtype in BQ_SCHEMA.get(name, {}).items():
                if col in dfs[name].columns:
                    dfs[name][col] = convert_column(dfs[name][col], dtype)

        # Handle nested arrays in subscriptions
        def normalize_sub_items(record_path, table_name):
            try:
                df = pd.json_normalize(
                    raw_data['list'],
                    record_path=['subscription', record_path],
                    meta=[['subscription', 'id']],
                    meta_prefix='sub_'
                ).rename(columns={'sub_subscription.id': 'subscription_id'})
                
                # Convert all columns to match schema
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

        # Add relationships
        if 'subscriptions' in dfs and not dfs['subscriptions'].empty:
            dfs['subscriptions']['customer_id'] = main_df.get('customer_id', pd.NA)
        if 'addresses' in dfs and not dfs['addresses'].empty:
            dfs['addresses']['customer_id'] = main_df.get('customer_id', pd.NA)

        # Hash generation with null handling
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