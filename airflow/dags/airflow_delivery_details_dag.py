from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from slack_alert import notify_slack_failure
import subprocess

# DAG definition
default_args = {
    'owner': 'delivery-squad',
}

def run_python_script():
    """Function to run the Python script."""
    # Run the Python file using subprocess
    subprocess.run(['python', '/Users/pranavikusu/recurring_deliveries/airflow/dags/airflow_delivery_details.py'], check=True)

with DAG(
    'airflow_delivery_details_dag_test',
    default_args=default_args,
    description='Run Python file from repo',
    schedule_interval='0 8 * * FRI',
    start_date=datetime(2024, 11, 6),
    catchup=False,
    on_failure_callback=notify_slack_failure
) as dag:

    run_python_file = PythonOperator(
        task_id='run_python_code_from_repo',
        python_callable=run_python_script,
    )

    # Setting the task dependencies
    run_python_file
