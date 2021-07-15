from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import services.async_api_call
import asyncio
from services.transform_data import transformation_on_data
from services.insert_into_bigquery import insert_data_to_bigquery
import time

# Default Argument
default_args = {
    'owner': 'Sachin M B',
    'start_date': datetime.now(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

# Sample API calls
urls = ['https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=TSLA&interval=5min&apikey=AHEAG5EVYQQJ8FQP',
        'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=GOOG&interval=5min&apikey=AHEAG5EVYQQJ8FQP',
        'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=TWTR&interval=5min&apikey=AHEAG5EVYQQJ8FQP'
]

# Function to start the asynchronous API calls
def get_api_data():
    obj = services.async_api_call.PublishToPubSub()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(obj.main(urls))

    # asyncio.run(obj.main(urls))

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
        python_callable=transformation_on_data
    )

    # Inserts data into BigQuery table
    insert_into_bq = PythonOperator(
        task_id="insert_into_bigquery",
        python_callable=insert_data_to_bigquery
    )

    # Dummy end task
    end = DummyOperator(
        task_id='end'
    )

    # Task flow
    start >> get_data >> wait_for_cf >> transform_data >> insert_into_bq >> end