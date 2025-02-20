import pandas as pd
from flatten_json import flatten


def sanitize_column_name(name):
    # Replacing invalid characters with underscores
    sanitized = ''.join([c if c.isalnum() else '_' for c in str(name)])
    # Removing leading numbers/underscores
    if sanitized and (sanitized[0].isdigit() or sanitized[0] == '_'):
        sanitized = 'col_' + sanitized
    # Truncating to 128 characters
    return sanitized[:128]


def process_json(raw_data):
    main_records = []
    if not isinstance(raw_data, dict):
        raise TypeError(f"Expected dict, got {type(raw_data)}")

    for entry in raw_data['list']:
    # Flattening core structure
        sub = flatten(entry['subscription'], separator='.')
        cust = flatten(entry['customer'], separator='.')
        combined = {
            **{'subscription_'+k: v for k,v in sub.items()},
            **{'customer_'+k: v for k,v in cust.items()}
        }
        main_records.append(combined)
    main_df = pd.DataFrame(main_records)

    # Creating normalized tables
    entities = {
        'subscriptions': [c for c in main_df if c.startswith('subscription_')],
        'customers': [c for c in main_df if c.startswith('customer_') and 'billing_address' not in c],
        'addresses': [c for c in main_df if c.startswith('customer_billing_address')] 
    }
    
    dfs = {}
    for name, cols in entities.items():
        dfs[name] = main_df[cols].copy()
        dfs[name].columns = [c.split('_', 1)[1] for c in cols]

    # Processing the arrays separately for further normalization
    def normalize_subscription_data(record_path):
        return pd.json_normalize(
            raw_data['list'],
            record_path=['subscription', record_path],
            meta=[['subscription', 'id']],
            meta_prefix='sub_'
        ).rename(columns={'sub_subscription.id': 'subscription_id'})

    dfs['subscription_items'] = normalize_subscription_data('subscription_items')
    dfs['item_tiers'] = normalize_subscription_data('item_tiers')

    # Adding relationships
    dfs['subscriptions']['customer_id'] = main_df['customer_id']
    dfs['addresses']['customer_id'] = main_df['customer_id']

    # Converting fields such as unit price and dates
    for table in ['subscriptions', 'subscription_items', 'item_tiers']:
        if 'unit_price' in dfs[table]:
            dfs[table]['unit_price'] = dfs[table]['unit_price'] / 100
        if 'price' in dfs[table]:
            dfs[table]['price'] = dfs[table]['price'] / 100

    # Converting epoch to datetime
    def convert_epoch_to_datetime(df, columns):
        for col in columns:
            df[col] = pd.to_datetime(df[col], unit='s')
        return df
      
    subscription_time_cols = [
        'created_at', 'started_at', 'updated_at',
        'current_term_start', 'current_term_end',
        'next_billing_at', 'activated_at'
    ]

    dfs['subscriptions'] = convert_epoch_to_datetime(dfs['subscriptions'], subscription_time_cols)
 
    def add_formatted_dates(df, time_cols):
        for col in time_cols:
            if col in df.columns:
                df[f'{col}_formatted'] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S') # YYYY-MM-DD HH:MM:SS
                df[f'{col}_date'] = df[col].dt.date
        return df

    # Applying to subscriptions
    sub_time_cols = ['created_at', 'started_at', 'updated_at']
    dfs['subscriptions'] = add_formatted_dates(dfs['subscriptions'], sub_time_cols)


    # Validating the final structure
    print("=== Final Structure ===")
    print(f"Subscriptions: {len(dfs['subscriptions'])} rows")
    print(f"Customers: {len(dfs['customers'])} rows")
    print(f"Addresses: {len(dfs['addresses'])} rows")
    print(f"Subscription Items: {len(dfs['subscription_items'])} rows")
    print(f"Item Tiers: {len(dfs['item_tiers'])} rows")

    return dfs