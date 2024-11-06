import os
import google.auth
from google.auth.transport.requests import Request
import requests

# Set up your environment and constants
AIRFLOW_HOST = os.getenv(
    "AIRFLOW_HOST",
    "https://e744c4e0fdc34ded96ed6a524a7eaffe-dot-us-east1.composer.googleusercontent.com/",
)

# Function to get the access token
def get_access_token():
    credentials, _ = google.auth.default()
    credentials.refresh(Request())
    return credentials.token

# Test the DAGs endpoint with explicit token handling
def test_get_dags():
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(f"{AIRFLOW_HOST}/api/v1/dags", headers=headers)
    print(f"Response status code: {response.status_code}")
    if response.status_code == 200:
        print("DAGs retrieved successfully:")
        print(response.json())  # Print the response JSON for further details
    else:
        print("Failed to retrieve DAGs:", response.text)

# Run the test function
if __name__ == "__main__":
    test_get_dags()
