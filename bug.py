import os
import json

def load_config():
    # Adjust relative path: go one level above project and into `config` folder
    config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config', 'config_ge4.json'))

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at: {config_path}")

    with open(config_path, 'r') as file:
        return json.load(file)
