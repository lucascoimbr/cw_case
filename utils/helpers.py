import json
import uuid
from typing import Any
import pandas as pd
from io import StringIO
import requests

class UUIDEncoder(json.JSONEncoder):
    def default(self, obj: Any):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)

def load_data_from_json(file_path: str) -> dict:
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def load_data_from_csv_url(csv_url: str) -> pd.DataFrame:
    
    response = requests.get(csv_url)
    response.raise_for_status()  # Ensure we got a successful response

    csv_data = StringIO(response.text)

    df = pd.read_csv(csv_data)

    return df.to_dict(orient="records")

if __name__ == "__main__":

    import os
    from dotenv import load_dotenv
    load_dotenv()

    csv_url = os.getenv("CSV_URL", "")

    data = load_data_from_csv_url(csv_url)
    print(data)

