from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from typing import Any
import google.auth
from google.auth.transport.requests import AuthorizedSession
import pandas as pd

# Constants
AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "https://e744c4e0fdc34ded96ed6a524a7eaffe-dot-us-east1.composer.googleusercontent.com/")
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default()
authed_session = AuthorizedSession(CREDENTIALS)
AIRFLOW_DAGS_ROUTE = f"{AIRFLOW_HOST}/api/v1/dags"
AIRFLOW_DAGS_RUN_ROUTE = f"{AIRFLOW_HOST}/api/v1/dags/{{}}/dagRuns"

# Fetch all DAGs
def get_all_dags():
    response = authed_session.get(AIRFLOW_DAGS_ROUTE)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve DAGs list. Status code: {response.status_code}")
    return response.json().get("dags", [])

# Fetch DAG runs for a specific DAG
def get_dag_runs(dag_id):
    dag_runs_endpoint = AIRFLOW_DAGS_RUN_ROUTE.format(dag_id)
    response = authed_session.get(dag_runs_endpoint)
    if response.status_code != 200:
        print(f"Failed to retrieve dagRun details for DAG '{dag_id}'. Status code: {response.status_code}")
        return []
    return response.json().get("dag_runs", [])

# Fetch task statuses for a specific DAG run
def get_task_statuses(dag_id, dag_run_id):
    dag_run_tasks_endpoint = f"{AIRFLOW_HOST}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    response = authed_session.get(dag_run_tasks_endpoint)
    if response.status_code != 200:
        print(f"Failed to retrieve task instances for DAG '{dag_id}' run '{dag_run_id}'. Status code: {response.status_code}")
        return {"export_to_s3_client": "Unknown", "export_to_gs_client": "Unknown", "export_to_azure_client": "Unknown"}
    tasks_data = response.json().get("task_instances", [])
    
    # Only get statuses of specific tasks
    task_status = {task["task_id"]: task["state"] for task in tasks_data if task["task_id"] in {"export_to_s3_client", "export_to_gs_client", "export_to_azure_client"}}
    return task_status

# Aggregate DAG run details
def get_dag_run_details(dag):
    dag_id = dag["dag_id"]
    dag_runs = get_dag_runs(dag_id)
    airflow_dag_runs = []
    
    for run in dag_runs:
        run_id = run["dag_run_id"]
        task_states = get_task_statuses(dag_id, run_id)
        airflow_dag_runs.append({
            "dag_id": dag_id,
            "status": run["state"],
            "last_run_start": run.get("start_date"),
            "next_dagrun_create_after": run.get("logical_date"),
            "task_status": task_states,
            "is_active": dag.get("is_active"),
            "timetable_description": dag.get("timetable_description")
        })
    return airflow_dag_runs

# Main function to collect all details concurrently
def main():
    dag_details = get_all_dags()
    all_airflow_dag_runs = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_dag_run_details, dag) for dag in dag_details]

        for future in as_completed(futures):
            all_airflow_dag_runs.extend(future.result())

    # Create DataFrame and save to CSV
    df_dag_runs = pd.DataFrame(all_airflow_dag_runs)
    print(df_dag_runs)
    df_dag_runs.to_csv("dag_runs_details.csv", index=False)
    print("Data saved to dag_runs_details.csv")
    upload_csv_to_bigquery()
    

# Run main
if __name__ == "__main__":
    main()
