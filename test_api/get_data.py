import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime

def get_data(url):
  out = requests.get(url)
  data = out.json()
  items = data['items']
  df = pd.DataFrame(items)
  df.drop(columns=['@id', 'eaRegionName', 'floodArea'], inplace=True)
  return df

def save_dataframe_to_storage(df, bucket_name):
    csv_string = df.to_csv(index=False)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    blob = bucket.blob(f'{time_now}.csv')
    blob.upload_from_string(csv_string, content_type='text/csv')

'''
TODO:
- dump csv content into BigQuery
- only add data to BigQuery if the severity level of the flood has changed, otherwise
throw away the duplicate data
'''