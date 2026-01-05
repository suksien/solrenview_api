import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import logging
import utils
try:
  from google.cloud import storage
except ImportError:
  pass
import glob

# extract electrical data from web api into csv file
def extract_electrical_data(start_date, end_date):
  inverter_id = [1, 2, 6]

  headers, url = utils.setup_config()  
  response_arr = utils.get_request(url, headers, start_date, end_date)
  timestamp = utils.get_timestamp(response_arr[0], start_date)

  df_arr = []
  for id in inverter_id:
    df_arr.append(utils.get_inverter_data_arr(response_arr, id, timestamp))
  
  df = pd.concat(df_arr)
  return df
  #df.to_csv(f'data/extracted/{start_date}_{end_date}.csv')
  #df.to_csv(f'/Users/suksientie/Desktop/solren_data/{start_date}_{end_date}.csv')
  

# https://stackoverflow.com/questions/40683702/upload-csv-file-to-google-cloud-storage-using-python
def upload_csv_to_gcs(filename, bucket_name, filepath):
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob_name = f'{filepath}/{filename.split('/')[-1]}'
  blob = bucket.blob(blob_name)
  blob.upload_from_filename(filename)

  logging.info(f"File uploaded to gs://{bucket_name}/{blob_name}")

def upload_df_to_gcs(df, bucket_name, filepath, filename):
    """Uploads a pandas DataFrame to a GCS bucket as a CSV."""
    
    # 1. Convert DataFrame to a CSV string buffer
    csv = df.to_csv(index=False)
    
    # 2. Initialize GCS client and get the bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = f'{filepath}/{filename}'
    blob = bucket.blob(blob_name)
    
    # 3. Upload the CSV data from the buffer
    blob.upload_from_string(csv, content_type='text/csv')
    
    logging.info(f"DataFrame uploaded to gs://{bucket_name}/{blob_name}")

def main():
  # logging
  logging.basicConfig(filename='logs.txt', level=logging.INFO, filemode='a')

  # simulate daily extraction
  from datetime import date, timedelta

  d1 = date(2025, 12, 24)
  d2 = d1 + timedelta(days=1)

  ndays = (date.today() - d1).days
  logging.info(f"=== Downloading {ndays} days of data from {d1} to {date.today()} ===")

  start_date_arr = [d1.strftime('%Y-%m-%d')]
  end_date_arr = [d2.strftime('%Y-%m-%d')]

  ndays = 2
  for i in range(ndays - 1):
    d1 += timedelta(days=1)
    d2 += timedelta(days=1)
    start_date_arr.append(d1.strftime('%Y-%m-%d'))
    end_date_arr.append(d2.strftime('%Y-%m-%d'))

  bucket_name = 'uni_toledo'
  filepath = 'data/waiting_to_load'
  
  # electrical data
  for i in range(len(start_date_arr)):
    start_date, end_date = start_date_arr[i], end_date_arr[i]
    df = extract_electrical_data(start_date, end_date)
    
    filename = f'{start_date}_{end_date}.csv'
    upload_df_to_gcs(df, bucket_name , filepath, filename)
  
  # met data
  met_file = glob.glob('data/waiting_to_load/MET*')[0]
  upload_csv_to_gcs(met_file, bucket_name, filepath)

if __name__ == "__main__":
  main()