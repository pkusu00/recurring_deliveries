#%%
from airflow.decorators import dag
from datetime import date,datetime
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from jira_export_all import retrieve_and_clean_jira_file
from jira_export_history import retrieve_and_transform_jira_history
from slack_alert import notify_slack_failure
# test:

@dag(
    schedule= '0 20 * * 3',
    start_date=datetime(2024, 6, 20),
    catchup=False,
    on_failure_callback = notify_slack_failure 
)

def dag_jira():

    current_date = date.today()

    send_local_jira_file_to_gcs_task = LocalFilesystemToGCSOperator(
        gcp_conn_id = "echo-google-cloud",
        task_id="send_local_jira_file_to_gcs",
        src= f"/Users/hichamghermani/desktop/projects/jira/cleaned/all_jira_cleaned_{current_date}.csv",
        dst= f"airflow_dag_runs/airflow_dag_runs_{current_date}.csv",
        bucket= "echo_recurring_delivery_details"
    )

    send_local_jira_history_file_to_gcs_task = LocalFilesystemToGCSOperator(
        gcp_conn_id = "echo-google-cloud",
        task_id= "send_local_jira_history_file_to_gcs",
        src= f"/Users/hichamghermani/desktop/projects/jira/cleaned/all_jira_history_transformed_{current_date}.csv",
        dst= f"jira-history/all_jira_history_transformed_{current_date}.csv",
        bucket= "echo_jira"
    )

    jira_from_gcs_to_bq_task = GCSToBigQueryOperator(
        task_id= "jira_from_gcs_to_bq",
        gcp_conn_id = "echo-google-cloud",
        bucket= "echo_jira",
        source_objects= f"jira-all/all_jira_cleaned_{current_date}.csv",
        destination_project_dataset_table= "echo-data-lake.jira.jira_cleaned_v2",
        write_disposition= "WRITE_TRUNCATE"
    )

    jira__history_from_gcs_to_bq_task = GCSToBigQueryOperator(
        task_id= "jira__history_from_gcs_to_bq",
        gcp_conn_id = "echo-google-cloud",
        bucket= "echo_jira",
        source_objects= f"jira-history/all_jira_history_transformed_{current_date}.csv",
        destination_project_dataset_table= "echo-data-lake.jira.jira_history_transformed",
        write_disposition= "WRITE_TRUNCATE"
    )

    
    retrieve_and_clean_jira_task >> retrieve_and_transform_jira_history_task >> send_local_jira_history_file_to_gcs_task >> send_local_jira_file_to_gcs_task >> jira_from_gcs_to_bq_task >> jira__history_from_gcs_to_bq_task

_ = dag_jira()