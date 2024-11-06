
#%%
import hashlib
import json
import os
from collections import defaultdict
from typing import Any
import google.auth
from google.auth.transport.requests import AuthorizedSession
import pandas as pd

#%%
AIRFLOW_HOST = os.getenv(
    "AIRFLOW_HOST",
    "https://e744c4e0fdc34ded96ed6a524a7eaffe-dot-us-east1.composer.googleusercontent.com/",
)

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])
authed_session = AuthorizedSession(CREDENTIALS)
AIRFLOW_DAGS_ROUTE = f"{AIRFLOW_HOST}/api/v1/dags"
AIRFLOW_TASKS_ROUTE = "{}/api/v1/dags/{}/dagRuns"

#%%
def fetch_task_info(dag_name):
    # Construct the API URL for the specific DAG
    AIRFLOW_DAGS_INFO_ROUTE = f"{AIRFLOW_HOST}/api/v1/dags/{dag_name}/dagRuns"
    #url = AIRFLOW_DAGS_INFO_ROUTE.format(AIRFLOW_HOST, dag_name)
    # Fetch DAG runs
    dags_info_response = authed_session.get(AIRFLOW_DAGS_INFO_ROUTE)
    dags_info_response.raise_for_status()
    # if dags_info_response.status_code != 200:
    #     raise Exception(f"Error fetching DAG information: {dags_info_response.status_code}")

    dags_data = dags_info_response.json()["dag_runs"]

    # Prepare list for DataFrame rows
    rows = []

    for run in dags_data:
        # Fetch task instances for the specific DAG run
        task_instances_response = authed_session.get(f"{AIRFLOW_HOST}/api/v1/dags/{dag_name}/dagRuns/{run['dag_run_id']}/taskInstances")
        
        if task_instances_response.status_code == 200:
            task_instances = task_instances_response.json().get("task_instances", [])

            for task_instance in task_instances:
                task_name = task_instance["task_id"]
                creation_date = run["execution_date"]
                is_failed = task_instance["state"] == "failed"  # True if the task failed, False otherwise
                
                # Append to list
                rows.append({
                    "dag_display_name": dag_name,
                    "task_name": task_name,
                    "creation_date": creation_date,
                    "is_failed": is_failed
                })

    # Create DataFrame
    df = pd.DataFrame(rows)
    return df

# Specify the DAG name and fetch task information
dag_name = 'nimbus_client_delivery'
df = fetch_task_info(dag_name)

# Display the DataFrame
print(df)
# %%
