from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'retries': 1
}

dag = DAG(
    'inject_nifi',
    default_args=default_args,
    description='Trigget nifi process',
    schedule_interval=timedelta(seconds=10),
    start_date=days_ago(0),
    catchup=False,
    max_active_runs=1)

def update_processor_status():
    # Define the base URL
    base_url = 'http://nifi:8081/nifi-api/processors/'

    # Make the GET request to retrieve the processor details
    response = requests.get(f'{base_url}{"7999d0bf-0188-1000-f91d-b0693da60506"}')
    response_json = response.json()
    
    # Parse the version from the response
    version = response_json['revision']['version']

    # Define the payload for the PUT request
    payload = {
        'revision': {
            'clientId': '25ff',
            'version': version
        },
        'state': 'RUNNING'
    }

    # Make the PUT request to update the processor status
    response = requests.put(f'{base_url}{"7999d0bf-0188-1000-f91d-b0693da60506"}/run-status', data=json.dumps(payload), headers={'Content-Type': 'application/json'})
    
    # Return the response
    return response.json()


load_data_task = PythonOperator(
    task_id='update_processor_status',
    python_callable=update_processor_status,
    dag=dag
)