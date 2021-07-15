# Stock-Ticker-ETL-Pipeline
 
Airflow DAG file -> `stock-ticker-dag.py`

Services that run in the DAG:
1. `async_api_call.py` -> Gets data from API and stores in GCP bucket
2. `transform_data.py` -> Transforms data from GCP bucket
3. `insert_into_bigquery.py` -> Inserts transformed data into BigQuery table
