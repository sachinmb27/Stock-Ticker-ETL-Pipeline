import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import time
import asyncio

import async_api_call
import transform_data
import insert_into_bigquery

# Default Argument
default_args = {
    'owner': 'Sachin M B',
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

# Sample API calls
urls = ['https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=TSLA&interval=5min&outputsize=full&apikey=AHEAG5EVYQQJ8FQP',
        'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=GOOG&interval=5min&outputsize=full&apikey=AHEAG5EVYQQJ8FQP',
        'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=TWTR&interval=5min&outputsize=full&apikey=AHEAG5EVYQQJ8FQP'
]

# Helper functions
def get_api_data():
    obj = async_api_call.PublishToPubSub()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(obj.main(urls))
    # asyncio.run(obj.main(urls))

def data_transformation():
    obj = transform_data.transformData()
    obj.transformation_on_data()

def load_to_bq():
    obj = insert_into_bigquery.insertIntoBigQuery()
    obj.insert_data_to_bigquery()

# DAG Definition
dag = DAG('stock_ticker_dag',
schedule_interval=None,
default_args=default_args)

# Tasks
with dag:
    # Dummy start task
    start = DummyOperator(
        task_id='start'
    )

    # Get API data
    get_data = PythonOperator(
        task_id='get_data_from_api',
        python_callable=get_api_data,
    )

    # Waits for the Cloud Function which pulls the message from Pub/Sub topic and loads to the GCP bucket
    wait_for_cf = PythonOperator(
        task_id="wait_for_cloud_function_to_trigger",
        python_callable=lambda: time.sleep(120)
    )

    # Transforms the data 
    transform_data = PythonOperator(
        task_id="transform_data_and_store",
        python_callable=data_transformation
    )

    # Inserts data into BigQuery table
    insert_into_bq = PythonOperator(
        task_id="insert_into_bigquery",
        python_callable=load_to_bq
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end'
    )

    # Task flow
    start >> get_data >> wait_for_cf >> transform_data >> insert_into_bq >> end