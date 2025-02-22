import json
from datetime import datetime

def extract_json(file_path, last_processed_ts=None):
    with open(file_path) as f:
        data = json.load(f)
    
    if last_processed_ts:
        if isinstance(last_processed_ts, datetime):
            last_ts = last_processed_ts.timestamp()
        else:
            last_ts = last_processed_ts
            
        data['list'] = [entry for entry in data['list'] 
            if entry['subscription']['updated_at'] > last_ts]
        
    print(f"Extracted {len(data['list'])} subscriptions since {last_processed_ts}")
    return data