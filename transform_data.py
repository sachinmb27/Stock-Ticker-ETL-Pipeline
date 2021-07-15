import os
import pandas as pd
from google.cloud import storage

origin_bucket_name = "stock-ticker-dataa"
destination_bucket_name = "temp-stock-ticker-dataa"

# Lists all the files present in the bucket
def list_buckets():
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(origin_bucket_name)
    return blobs

# Does transformation on the data
def transformation_on_data():
    files = list_buckets()
    for f in files:
        df = pd.read_csv('gs://' + origin_bucket_name +  '/' + f.name)
        ticker_name = f.name.split('_')[2].split('.')[0]
        df['Time Series'] = df['Time Series'].str.split(':').str[0]
        df = df.rename(columns={"Time Series": "Time_Series"})
        grouped_df = df.groupby(['Time_Series'], as_index=False).mean()
        grouped_df['Time_Series'] = grouped_df['Time_Series'].astype(str)
        grouped_df[['Date', 'Hour']] = grouped_df['Time_Series'].str.split(' ', 1, expand=True)
        grouped_df = grouped_df.drop(columns=['Time_Series'])
        grouped_df['Ticker'] = ticker_name
        grouped_df = grouped_df[['Ticker', 'Date', "Hour", 'Open', "High", 'Low', 'Close', 'Volume']]
        grouped_df.to_csv(ticker_name + '.csv', index=False, header=False)
        load_transformed_data_to_bucker(ticker_name + '.csv')
        os.remove(ticker_name + '.csv')

# Loads the transformed data into a bucket
def load_transformed_data_to_bucker(file_path):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(destination_bucket_name)
    blob = bucket.blob('transformed_data_' + file_path)
    blob.upload_from_file(open(file_path, 'r'))