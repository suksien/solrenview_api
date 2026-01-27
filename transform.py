import pandas as pd
import numpy as np
from datetime import date
from google.cloud import storage, bigquery
from alert import send_email

storage_client = storage.Client()
bucket_name = 'uni_toledo'
bucket = storage_client.bucket(bucket_name)

def get_electrical_and_met_files():
  all_blobs = storage_client.list_blobs('uni_toledo', prefix='data/waiting_to_load')
  all_files = [blob.name for blob in all_blobs if blob.name.endswith('csv') or blob.name.endswith('txt')]

  met_file = []
  for file in all_files:
    if file.split('/')[-1].startswith('MET'):
        met_file.append(file)

  # handle missing met file
  met_file = met_file[-1] if len(met_file) > 0 else None
  
  elec_files = [file for file in all_files if not file.split('/')[-1].startswith('MET')]

  return (elec_files, met_file)

def select_files_for_staging(elec_files, met_file):
  blob = bucket.blob(met_file)
  with blob.open('r') as file:
    met_df = pd.read_csv(file, skiprows=[0, 2, 3])

  last_date_met = pd.to_datetime(met_df.tail(1)['TIMESTAMP'])

  # append elec files whose start date < last date of met file
  staging_files = []
  for file in elec_files:
      start_date_file = file.split('/')[-1].split('.csv')[0]
      compare_dates = pd.to_datetime(start_date_file) <= last_date_met
      if compare_dates.item():
          staging_files.append(file)

  return staging_files
   
def concat_all_electrical_files(elec_files):
  all_data = []

  for file in elec_files:
    tmp_df = resample_electrical_15min(file)
    tmp_df['date'] = pd.to_datetime(tmp_df['timestamp']).dt.date
    tmp_df['component_id'] = 'I' + tmp_df['inverter'].astype(str) + '_S' + tmp_df['str'].astype(str)
    tmp_df['sample_description'] = np.where(tmp_df['inverter'] == 6, 'QED', 'Cure')
    tmp_df['energy'] = tmp_df['voltage'] * tmp_df['current']
    all_data.append(tmp_df)

  df = pd.concat(all_data)
  return df

def resample_electrical_15min(file):
    blob = bucket.blob(file)
    with blob.open('r') as file:
      df = pd.read_csv(file)

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    inverter_arr = list(set(df['inverter']))
    string_arr = list(set(df['str']))
    data = []

    for inverter in inverter_arr:
        df_inverter = df[df['inverter'] == inverter]

        for string in string_arr:
            df_string = df_inverter[df_inverter['str'] == string]
            df2 = df_string.resample('15Min', on='timestamp')['current', 'voltage'].mean()
            df2['inverter'] = inverter
            df2['str'] = string
           
            data.append(df2.reset_index())

    new_df = pd.concat(data)
    return new_df

def resample_met_15min(filename):
    blob = bucket.blob(filename)
    with blob.open('r') as file:
      met_df = pd.read_csv(file, skiprows=[0, 2, 3])

    met_df['timestamp'] = pd.to_datetime(met_df['TIMESTAMP'])
    met_df = met_df.resample('15Min', on='timestamp')['GHI_TS_Avg', 
                                                      'POA_TS_Avg', 
                                                      'AirTemp_Avg', 
                                                      'RH_Avg', 
                                                      'Panel1_Temp_Avg'].mean().reset_index()
    met_df.rename(columns={'GHI_TS_Avg': 'irradiance_ghi', 
                           'POA_TS_Avg': 'irradiance_poa',
                           'AirTemp_Avg': 'temperature', 
                           'RH_Avg': 'relative_humidity', 
                           'Panel1_Temp_Avg': 'tmod'}, inplace=True)

    return met_df

def load_df_to_bigquery(df, table_id):
  client = bigquery.Client()
  job_config = bigquery.LoadJobConfig()
  job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
  job.result()
  if job.output_rows == len(df):
      print(f"Loaded {job.output_rows} rows into {table_id}.")
      return 'success'
  else:
     return None

def move_files(files_list, directory):
  for file in files_list:
    blob = bucket.blob(file)
    dest_filename = f'data/{directory}/{file.split('/')[-1]}'
    bucket.copy_blob(blob, bucket, dest_filename)
    print(f".... Copied {blob.name} to {dest_filename}")
    blob.delete()
    print(f"Original file {blob.name} deleted.")

# copied from extract.py -- TODO: extract this out to util.py
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
    
    print(f"DataFrame uploaded to gs://{bucket_name}/{blob_name}")

def load_to_warehouse_with_retries(merged_df, elec_df, met_df, staging_files, retry_ntimes=2):
  while retry_ntimes >= 0:
    load_result = load_df_to_bigquery(merged_df, "solren-view-etl.UT_15min.master")

    if load_result == 'success':
      del elec_df
      del met_df
      print("Moving files to Loaded directory")
      move_files(staging_files, 'loaded')
      return "Successfully loaded to BQ"
    else:
      print("Data failed to load to BigQuery")
      print("Retrying...")
      retry_ntimes -= 1  

  print("Moving files to Error directory")
  move_files(staging_files, 'error')

  today_str = date.today().strftime('%Y-%m-%d')
  upload_df_to_gcs(merged_df, bucket_name, 'data/error', f'merged_{today_str}.csv')
  upload_df_to_gcs(elec_df, bucket_name, 'data/error', f'elec_{today_str}.csv')
  upload_df_to_gcs(met_df, bucket_name, 'data/error', f'met_{today_str}.csv')
  return "Error loading to BQ - files routed to DLQ"

def main():
  elec_files, met_file = get_electrical_and_met_files()
  
  if len(elec_files) > 0 and met_file is not None:
    staging_files = select_files_for_staging(elec_files, met_file)
    
    if len(staging_files) > 0:
      elec_df = concat_all_electrical_files(staging_files)
      met_df = resample_met_15min(met_file)
      merged_df = pd.merge(elec_df, met_df, how='left', on='timestamp')

      load_result = load_to_warehouse_with_retries(merged_df, elec_df, met_df, staging_files)

    else:
      load_result = "No file is being staged"
      print(load_result)
  else:
      load_result = "No electrical or met file can be found"
      print(load_result)

  today_str = date.today().strftime('%Y-%m-%d')
  send_email('hahasuksien@yahoo.com.my', f'Transform job results for {today_str}', load_result)

if __name__ == '__main__':
   main()