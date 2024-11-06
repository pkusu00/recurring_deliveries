#%%
import os
import google.auth
from google.auth.transport.requests import Request
import requests
import pandas as pd

#%%
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

#%%
# Function to get all DAGs with their details
def get_all_dags():
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    response = requests.get(f"{AIRFLOW_HOST}/api/v1/dags", headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve DAGs list. Status code: {response.status_code}")
    
    dags_data = response.json().get("dags", [])
    dag_details = []
    
    for dag in dags_data:
        dag_id = dag["dag_id"]
        dag_details.append({
            "dag_id": dag_id,
            "display_name": dag.get("dag_name", dag_id),
            "is_active": dag.get("is_active"),
            "timetable_description": dag.get("timetable_description"),
        })
    
    return dag_details

#%%
# Function to get detailed information for each DAG run
def get_dag_runs(dag_id):
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    dag_runs_endpoint = f"{AIRFLOW_HOST}/api/v1/dags/{dag_id}/dagRuns"
    response = requests.get(dag_runs_endpoint, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to retrieve dagRun details for DAG '{dag_id}'. Status code: {response.status_code}")
        return []
    
    dag_runs = response.json().get("dag_runs", [])
    dag_run_details = []
    
    for run in dag_runs:
        run_id = run["dag_run_id"]
        task_states = get_task_statuses(dag_id, run_id)
        
        dag_run_details.append({
            "dag_id": dag_id,
            "status": run["state"],
            "last_run_start": run.get("start_date"),
            "next_dagrun_create_after": run.get("logical_date"),
            "task_status": task_states,
        })
    
    return dag_run_details

#%%
# Function to get task statuses for specific tasks
def get_task_statuses(dag_id, dag_run_id):
    token = get_access_token()
    headers = {
        "Authorization": f"Bearer {token}"
    }

    dag_run_tasks_endpoint = f"{AIRFLOW_HOST}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    response = requests.get(dag_run_tasks_endpoint, headers=headers)

    if response.status_code != 200:
        print(f"Failed to retrieve task instances for DAG '{dag_id}' run '{dag_run_id}'. Status code: {response.status_code}")
        return {
            "export_to_s3_client": "Unknown",
            "export_to_gs_client": "Unknown",
            "export_to_azure_client": "Unknown",
        }

    tasks_data = response.json().get("task_instances", [])
    
    # Check status for specific tasks
    task_status = {
        "export_to_s3_client": "Unknown",
        "export_to_gs_client": "Unknown",
        "export_to_azure_client": "Unknown",
    }
    
    for task in tasks_data:
        task_id = task["task_id"]
        if task_id in task_status:
            task_status[task_id] = task["state"]
    
    return task_status

#%%
# Function to save DataFrame as CSV
def save_to_csv(df, filename="dag_runs_details.csv"):
    df.to_csv(filename, index=False)
    print(f"Data saved to {filename}")

#%%
# Main function to collect and display all dag run details
def main():
    # Fetch list of all DAGs and their details
    dag_details = get_all_dags()
    all_dag_run_details = []

    # Collect detailed DAG run information for each DAG
    for dag in dag_details:
        dag_id = dag["dag_id"]
        dag_runs = get_dag_runs(dag_id)
        
        for run in dag_runs:
            run.update({
                "dag_id": dag["dag_id"],
                "is_active": dag["is_active"],
                "timetable_description": dag["timetable_description"],
            })
            all_dag_run_details.append(run)
    
    # Convert to DataFrame
    df_dag_runs = pd.DataFrame(all_dag_run_details)
    
    # Display all columns in the DataFrame
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    
    # Print the DataFrame
    print(df_dag_runs)
    
    # Save the DataFrame to CSV
    save_to_csv(df_dag_runs)

#%%
# Run the main function
if __name__ == "__main__":
    main()
