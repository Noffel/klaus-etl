import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def extract_json(file_path, last_processed_ts_dict=None):
    with open(file_path) as f:
        data = json.load(f)
    
    initial_count = len(data['list'])
    logger.info(f"Found {initial_count} total entries in source JSON")

    if last_processed_ts_dict:
        last_ts_values = [
            ts.timestamp() if isinstance(ts, datetime) else ts
            for ts in last_processed_ts_dict.values() 
            if ts is not None
        ]
        last_ts = min(last_ts_values) if last_ts_values else None
        
        if last_ts:
            filtered = []
            for entry in data['list']:
                sub_updated = entry.get('subscription', {}).get('updated_at', 0)
                cust_updated = entry.get('customer', {}).get('updated_at', 0)
                
                if sub_updated > last_ts or cust_updated > last_ts:
                    filtered.append(entry)
            
            data['list'] = filtered
            logger.info(f"Filtered {initial_count - len(filtered)} entries using timestamp cutoff {last_ts}")

    logger.info(f"Proceeding with {len(data['list'])} entries for processing")
    return data