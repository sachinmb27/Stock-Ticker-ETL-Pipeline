import os
from google.cloud import bigquery, storage

class insertIntoBigQuery:
    def __init__(self):
        self.storage_client = storage.Client()
        self.bigquery_client = bigquery.Client()
        self.project = 'egen-training-mbs'
        self.dataset_id = 'stock_data'
        self.bucket_name = "temp-stock-ticker-dataa"

    # Lists all the files in the bucket
    def list_buckets(self) -> list:
        blobs = self.storage_client.list_blobs(self.bucket_name)
        return blobs

    # Creates a table for each file and inserts corresponding data to the BigQuery table
    def insert_data_to_bigquery(self) -> None:
        files = self.list_buckets()

        # Holds the names of the tables
        tickers = []
        for f in files:
            tickers.append(f.name.split('_')[2].split('.')[0])

        dataset_ref = bigquery.DatasetReference(self.project, self.dataset_id)

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

        # For each file in the bucket, fetches the data from GCP bucket and inserts it into corresponding BigQuery table
        for ticker in tickers:
            table_name_to_be_created = ticker
            table_id = self.project + '.' + self.dataset_id + '.' + ticker
            uri = "gs://temp-stock-ticker-dataa/transformed_data_" + ticker + ".csv"

            # Check if table already present
            # True - Appends data to the table
            # False - Creates a new table and adds data to it
            if self.bigquery_client.get_table(table_id):
                job_config = bigquery.LoadJobConfig()
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
                job_config.skip_leading_rows = 1

                job_config.source_format = bigquery.SourceFormat.CSV
                load_job = self.bigquery_client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )

                load_job.result()
                destination_table = self.bigquery_client.get_table(table_id)
                print(f"Table {destination_table} already present.")
                print(f"Loaded {destination_table.num_rows} rows.")
            
            else:
                job_config = bigquery.LoadJobConfig(
                    schema = table_schema,
                    skip_leading_rows=1,
                    source_format=bigquery.SourceFormat.CSV,
                )

                table = bigquery.Table(dataset_ref.table(table_name_to_be_created), schema=table_schema)
                table = self.bigquery_client.create_table(table)

                load_job = self.bigquery_client.load_table_from_uri(
                    uri, table_id, job_config=job_config
                )

                load_job.result()
                destination_table = self.bigquery_client.get_table(table_id)
                print(f"Created {destination_table}.")
                print(f"Loaded {destination_table.num_rows} rows.")

