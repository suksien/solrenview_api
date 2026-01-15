import pandas as pd
import logging
import utils
import glob
from datetime import date, timedelta
try:
  from google.cloud import storage
except ImportError:
  pass

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

def get_start_date():
  f = open('start_date.txt', 'r')
  last_yr, last_mo, last_dt = [int(num) for num in f.readline().split('-')]
  f.close()

  return date(last_yr, last_mo, last_dt)

def write_next_start_date(date_str):
  f = open('start_date.txt', 'w')
  f.write(date_str)
  f.close()

def get_date_arrays(start_date_obj, end_date_obj, ndays):
    start_date_arr = [start_date_obj.strftime('%Y-%m-%d')]
    end_date_arr = [end_date_obj.strftime('%Y-%m-%d')]

    for _ in range(ndays):
      start_date_obj += timedelta(days=1)
      end_date_obj += timedelta(days=1)
      start_date_arr.append(start_date_obj.strftime('%Y-%m-%d'))
      end_date_arr.append(end_date_obj.strftime('%Y-%m-%d'))

    return start_date_arr, end_date_arr

##########################
def main_mock_daily():
  # logging
  logging.basicConfig(filename='logs.txt', level=logging.INFO, filemode='a')

  d1 = date(2025, 12, 24)
  d2 = d1 + timedelta(days=1)
  ndays = (date.today() - d1).days
  logging.info(f"=== Downloading {ndays} days of data from {d1} to {date.today()} ===")

  ndays = 1
  start_date_arr, end_date_arr = get_date_arrays(d1, d2, ndays - 1)

  bucket_name = 'uni_toledo'
  filepath = 'data/waiting_to_load'
  
  # electrical data
  for i in range(len(start_date_arr)):
    start_date, end_date = start_date_arr[i], end_date_arr[i]
    df = extract_electrical_data(start_date, end_date)
    
    filename = f'{start_date}.csv'
    #upload_df_to_gcs(df, bucket_name, filepath, filename)
  
  return df

  # met data
  #met_file = glob.glob('data/waiting_to_load/MET*')[0]
  #upload_csv_to_gcs(met_file, bucket_name, filepath)

def main():
  # logging
  logging.basicConfig(filename='logs.txt', level=logging.INFO, filemode='a')

  d1 = get_start_date()
  d2 = d1 + timedelta(days=1)
  ndays = (date.today() - d1).days
  if ndays < 0:
    raise ValueError(f"Negative ndays ({ndays}) is not allowed")
  
  logging.info(f"=== Downloading {ndays} days of data from {d1} to {date.today()} ===")
  ndays = 1
  start_date_arr, end_date_arr = get_date_arrays(d1, d2, ndays - 1)
  write_next_start_date(end_date_arr[-1])

  bucket_name = 'uni_toledo'
  filepath = 'data/waiting_to_load'
  
  # electrical data
  for i in range(len(start_date_arr)):
    start_date, end_date = start_date_arr[i], end_date_arr[i]
    df = extract_electrical_data(start_date, end_date)
    
    filename = f'{start_date}.csv'
    upload_df_to_gcs(df, bucket_name, filepath, filename)

  # met data
  met_files = glob.glob('MET*txt')
  if len(met_files) > 0:
    logging.info(f"Uploading {met_files[-1]} met file")
    # upload_csv_to_gcs(met_files[-1], bucket_name, filepath)

if __name__ == "__main__":
  main()
