import json

def extract_json(file_path):
    with open(file_path) as f:
        return json.load(f)  # returns raw Python dict