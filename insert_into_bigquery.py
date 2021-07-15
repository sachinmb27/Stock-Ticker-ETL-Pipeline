import os
from google.cloud import bigquery, storage

client = bigquery.Client()
project = 'egen-training-mbs'
dataset_id = 'stock_data'

bucket_name = "temp-stock-ticker-dataa"

# Lists all the files in the bucket
def list_buckets():
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    return blobs

# Creates a table for each file and inserts corresponding data to the BigQuery table
def insert_data_to_bigquery():
    files = list_buckets()

    # Holds the names of the tables
    tickers = []
    for f in files:
        tickers.append(f.name.split('_')[2].split('.')[0])

    dataset_ref = bigquery.DatasetReference(project, dataset_id)

    # Schema for BigQuery table
    table_schema = [
        bigquery.SchemaField("ticker_symbol", "STRING"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("hour", "STRING"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
        ]

    job_config = bigquery.LoadJobConfig(
        schema = table_schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    # For each file in the bucket, fetches the data from GCP bucket and inserts it into corresponding BigQuery table
    for ticker in tickers:
        table_name_to_be_created = ticker
        table_id = project + '.' + dataset_id + '.' + ticker
        uri = "gs://temp-stock-ticker-dataa/transformed_data_" + ticker + ".csv"


        table = bigquery.Table(dataset_ref.table(table_name_to_be_created), schema=table_schema)
        table = client.create_table(table)

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        load_job.result()

        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))

