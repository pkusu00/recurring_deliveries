
import time
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime  # Added import
from slack_alert import notify_slack_failure

# DAG definition with @dag decorator
default_args = {
    'owner': 'delivery-squad',
}

with DAG(
    'airflow_delivery_details_dag_test',
    default_args=default_args,
    description='Run Python file from repo',
    schedule_interval='0 8 * * FRI',
    start_date=datetime(2024, 11, 6),
    catchup=False,
    on_failure_callback=notify_slack_failure
) as dag:

    start = DummyOperator(task_id='start')

    run_python_file = BashOperator(
        task_id='run_python_code_from_repo',
        bash_command='python /Users/pranavikusu/echo-delivery/delivery_table/upload_test.py'
    )

    end = DummyOperator(task_id='end')

    # Task dependencies
    start >> run_python_file >> end
