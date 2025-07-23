import json

def flatten_table(data_rows):
    try:
        # If data_rows is a string, parse it into a Python list
        if isinstance(data_rows, str):
            data_rows = json.loads(data_rows)
        
        if not data_rows or not isinstance(data_rows[0], dict):
            raise ValueError("Invalid data_rows format: Expected a list of dictionaries.")

        return {
            "columns": [{"value": key} for key in data_rows[0].keys()],
            "rows": data_rows
        }
    except Exception as e:
        print(f"[ERROR] flatten_table failed: {str(e)}")
        return {
            "columns": [],
            "rows": []
        }
