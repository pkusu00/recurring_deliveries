from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
import os

# Define the path where the CSV files will be saved locally
LOCAL_PATH = "/path/to/local/directory"  # Adjust the path accordingly

# Define the Google Cloud Storage bucket
GCS_BUCKET = "your_gcs_bucket_name"

# Function to retrieve DAG runs and save them as CSV
def retrieve_and_save_dag_runs():
    # Assuming you have the code to get the DataFrame already in place
    from your_module import main  # Import the main function from your previous code
    
    # Run the main function which returns the DataFrame
    df_dag_runs = main()  # Adjust this as needed to get the DataFrame directly
    
    # Save the DataFrame to CSV
    file_name = f"dag_runs_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    local_file_path = os.path.join(LOCAL_PATH, file_name)
    df_dag_runs.to_csv(local_file_path, index=False)
    print(f"Saved DAG run details to {local_file_path}")

# Create the DAG
@dag(
    schedule='@daily',  # Set the schedule as per your requirements
    start_date=datetime(2024, 11, 1),  # Set your start date
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
)
def dag_refresh_dag_runs():

    # Task to retrieve and save DAG runs to CSV
    retrieve_dag_runs_task = PythonOperator(
        task_id="retrieve_and_save_dag_runs",
        python_callable=retrieve_and_save_dag_runs
    )

    # Task to send the CSV file to GCS
    send_local_dag_runs_to_gcs_task = LocalFilesystemToGCSOperator(
        gcp_conn_id="echo-google-cloud",
        task_id="send_local_dag_runs_to_gcs",
        src="{{ task_instance.xcom_pull(task_ids='retrieve_and_save_dag_runs') }}",  # Use XCom to pull the file path
        dst=f"dag-runs/dag_runs_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        bucket=GCS_BUCKET
    )

    # Task to transfer the data from GCS to BigQuery
    dag_runs_from_gcs_to_bq_task = GCSToBigQueryOperator(
        task_id="dag_runs_from_gcs_to_bq",
        gcp_conn_id="echo-google-cloud",
        bucket=GCS_BUCKET,
        source_objects=[f"dag-runs/dag_runs_details_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"],
        destination_project_dataset_table="your_project.your_dataset.dag_runs",
        write_disposition="WRITE_TRUNCATE"
    )

    # Define the task dependencies
    retrieve_dag_runs_task >> send_local_dag_runs_to_gcs_task >> dag_runs_from_gcs_to_bq_task

# Instantiate the DAG
dag_instance = dag_refresh_dag_runs()
