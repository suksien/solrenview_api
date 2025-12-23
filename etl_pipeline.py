import requests
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import utils
try:
  from google.cloud import storage
except ImportError:
  pass

# TODO: extract weather data from sftp server into csv file

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
  #df.to_csv(f'data/extracted/{start_date}_{end_date}.csv')
  df.to_csv(f'/Users/suksientie/Desktop/solren_data/{start_date}_{end_date}.csv')

# https://stackoverflow.com/questions/40683702/upload-csv-file-to-google-cloud-storage-using-python
'''
def load_csv_to_bucket(csv_file, bucket_name = "solrenview_data"):
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(f'data/extracted/{start_date}_{end_date}.csv')
  blob.upload_from_filename(csv_file)
'''

# simulate daily extraction
from datetime import date, timedelta

d1, d2 = date(2025, 10, 14), date(2025, 10, 15)

start_date_arr = [d1.strftime('%Y-%m-%d')]
end_date_arr = [d2.strftime('%Y-%m-%d')]

for i in range(5):
  d1 += timedelta(days=1)
  d2 += timedelta(days=1)
  start_date_arr.append(d1.strftime('%Y-%m-%d'))
  end_date_arr.append(d2.strftime('%Y-%m-%d'))

for i in range(len(start_date_arr)):
  start_date, end_date = start_date_arr[i], end_date_arr[i]
  extract_electrical_data(start_date, end_date)