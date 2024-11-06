#%%
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from typing import Any
from datetime import date
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage, bigquery
import pandas as pd


#%%
# Constants
AIRFLOW_HOST = os.getenv("AIRFLOW_HOST", "https://e744c4e0fdc34ded96ed6a524a7eaffe-dot-us-east1.composer.googleusercontent.com/")
AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default()
authed_session = AuthorizedSession(CREDENTIALS)
AIRFLOW_DAGS_ROUTE = f"{AIRFLOW_HOST}/api/v1/dags"
AIRFLOW_DAGS_RUN_ROUTE = f"{AIRFLOW_HOST}/api/v1/dags/{{}}/dagRuns"

GCS_BUCKET_NAME = "echo_recurring_delivery_details" 
GCS_FILE_PATH = "airflow_dag_runs/"
BIGQUERY_PROJECT = "echo-data-lake"  # Replace with your actual BigQuery project ID
BIGQUERY_DATASET = "recurring_delivery_details"  # Replace with your BigQuery dataset name
BIGQUERY_TABLE = "airflow_deliveries"


#%%
# Fetch all DAGs
def get_all_dags():
    response = authed_session.get(AIRFLOW_DAGS_ROUTE)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve DAGs list. Status code: {response.status_code}")
    return response.json().get("dags", [])
print("All DAGs extracted into a list")



#%%
# Fetch DAG runs for a specific DAG
def get_dag_runs(dag_id):
    dag_runs_endpoint = AIRFLOW_DAGS_RUN_ROUTE.format(dag_id)
    response = authed_session.get(dag_runs_endpoint)
    if response.status_code != 200:
        print(f"Failed to retrieve dagRun details for DAG '{dag_id}'. Status code: {response.status_code}")
        return []
    return response.json().get("dag_runs", [])



#%%
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




#%%
# Aggregate DAG run details
def get_dag_run_details(dag):
    dag_id = dag["dag_id"]
    if not dag_id.endswith('_client_delivery'):
        return []
    dag_runs = get_dag_runs(dag_id)
    airflow_dag_runs = []
    
    for run in dag_runs:
        run_id = run["dag_run_id"]
        run_id_str = f"{dag_id}_{run.get('logical_date')}_{run.get('start_date')}_{run['state']}"
        task_states = get_task_statuses(dag_id, run_id)
        airflow_dag_runs.append({
            "run_id": run_id_str,
            "dag_id": dag_id,
            "status": run["state"],
            "last_run_start": run.get("start_date"),
            "next_dagrun_create_after": run.get("logical_date"),
            "task_status": task_states,
            "is_active": dag.get("is_active"),
            "timetable_description": dag.get("timetable_description")
        })
    return airflow_dag_runs
print("DAG run details extracted")



#%%
# Upload file to gcs
def upload_to_gcs(local_file_path, gcs_file_path):
    project_id = "Echo Data Lake"
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_file_path)
    blob.upload_from_filename(local_file_path)
    print(f"File uploaded to GCS: {gcs_file_path}")



#%%
# Upload CSV to BigQuery with incremental updates
def upload_to_bigquery(local_file_path):
    client = bigquery.Client(project=BIGQUERY_PROJECT)
    dataset_ref = client.dataset(BIGQUERY_DATASET)
    table_ref = dataset_ref.table(BIGQUERY_TABLE)

    # Load existing data into a DataFrame to deduplicate
    existing_data_query = f"SELECT * FROM `{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`"
    existing_data = client.query(existing_data_query).to_dataframe()

    # Load new data
    new_data = pd.read_csv(local_file_path)

    # Generate the run_id for new data
    new_data["run_id"] = new_data.apply(lambda row: f"{row['dag_id']}_{row['next_dagrun_create_after']}_{row['last_run_start']}_{row['status']}", axis=1)

    # Remove duplicates from new data based on run_id
    new_data = new_data[~new_data['run_id'].isin(existing_data['run_id'])]

    if not new_data.empty:
        # Append new data to BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip the header row
            autodetect=True,  # Automatically detect schema
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Append to existing table
        )

        with open(local_file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)  # Make an API request.
        
        job.result()  # Wait for the job to complete.
        print(f"Data loaded into BigQuery table {BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}")
    else:
        print("No new data to upload to BigQuery.")


#%%
# Main function to collect all details concurrently
print("Starting extraction...")
def main():
    dag_details = get_all_dags()
    all_airflow_dag_runs = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_dag_run_details, dag) for dag in dag_details]

        for future in as_completed(futures):
            result = future.result()
            if result:
                all_airflow_dag_runs.extend(result)

    # Create DataFrame and save to CSV with the date in the filename
    if all_airflow_dag_runs:
        df_dag_runs = pd.DataFrame(all_airflow_dag_runs)
        today = date.today()
        current_date = f"{today.year}_{today.month:02d}_{today.day:02d}"
        csv_file_name = f"airflow_dag_runs_{current_date}.csv"
        df_dag_runs.to_csv(csv_file_name, index=False)
        print(f"Data saved to {csv_file_name}")

        # Upload CSV file to GCS
        gcs_file_path = f"{GCS_FILE_PATH}{csv_file_name}"
        upload_to_gcs(csv_file_name, gcs_file_path)
        
        # Upload to BigQuery
        upload_to_bigquery(csv_file_name)
    else:
        print("No new DAG runs.")
#%%
# Run main
if __name__ == "__main__":
    main()


