import requests
import base64
import uuid

def encode_csv_to_base64(csv_data):
    """Encode CSV data to base64."""
    return base64.b64encode(csv_data.encode()).decode()

def post_data_to_endpoint(schema_id, clear, csv_data, api_key):
    """Post data to the specified endpoint."""
    if not isinstance(schema_id, uuid.UUID):
        raise ValueError("schema_id must be a UUID.")

    url = f"http://localhost:7080/api/data/format/csv"

    headers = {
        'X-API-KEY': f'{api_key}',
        'Content-Type': 'application/json'
    }

    payload = {
        'schema-id': str(schema_id),
        'clear': 'true',
        'data': encode_csv_to_base64(csv_data)
    }
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to post data: {response.status_code}, {response.text}")

# Example usage
schema_id = uuid.UUID('c89f6e99-efd7-4bdc-8114-07572ef7e4fb')  # Replace with actual UUID
clear = True  # or False
csv_data = "animal,nos legs\ncat,4"  # Replace with actual CSV data
api_key = "A9YAgop5xXKwKnTjZHJBQCoDw2m7I93r"  # Replace with your API key

try:
    response = post_data_to_endpoint(schema_id, clear, csv_data, api_key)
    print("Response:", response)
except Exception as e:
    print("Error:", e)
