import base64
import subprocess
import uuid
import json

def encode_csv_to_base64(csv_file_path):
    """Read CSV file and encode its contents to base64."""
    with open(csv_file_path, 'r') as file:
        csv_data = file.read()
    return base64.b64encode(csv_data.encode()).decode()

def post_data_to_endpoint_with_curl(schema_id, clear, csv_file_path, api_key):
    """Post data to the specified endpoint using a curl command."""
    if not isinstance(schema_id, uuid.UUID):
        raise ValueError("schema_id must be a UUID.")

    # Construct the URL with schema_id and clear query parameter
    url = f"http://localhost:7080/api/data/format/csv/{schema_id}?clear={str(clear).lower()}"

    # The payload now only contains the base64-encoded data from the CSV file
    payload = {
        'data': encode_csv_to_base64(csv_file_path)
    }

    # Constructing curl command
    curl_command = [
        'curl', '-X', 'POST', url,
        '-H', f"X-API-KEY: {api_key}",
        '-H', "Content-Type: application/json",
        '-d', json.dumps(payload)
    ]

    try:
        result = subprocess.run(curl_command, check=True, capture_output=True, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        raise Exception(f"Failed to post data: {e.stderr}")

# Example usage
schema_id = uuid.UUID('c89f6e99-efd7-4bdc-8114-07572ef7e4fb')  # Replace with actual UUID
clear = True  # or False
csv_file_path = "sample data.csv"  # Path to your CSV file
api_key = "A9YAgop5xXKwKnTjZHJBQCoDw2m7I93r"  # Replace with your API key

try:
    response = post_data_to_endpoint_with_curl(schema_id, clear, csv_file_path, api_key)
    print("Response:", response)
except Exception as e:
    print("Error:", e)
