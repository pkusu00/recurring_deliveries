#%%
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.models import Variable
import json
import requests

def notify_slack_failure(context):

    #slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    url = "https://hooks.slack.com/services/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"
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            exec_date=context['execution_date'],
            log_url=context.get('task_instance').log_url,
        )
    
    slack_data = {"text": slack_msg}

    return requests.post(
        url,
        data= json.dumps(slack_data),
        headers= {"Content-Type": "application/json"}
    )

