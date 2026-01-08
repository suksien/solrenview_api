import pandas as pd
import numpy as np
from google.cloud import storage, bigquery

storage_client = storage.Client()
bucket = storage_client.bucket('uni_toledo')

def get_electrical_and_met_files():
  all_blobs = storage_client.list_blobs('uni_toledo', prefix='data/waiting_to_load')
  all_files =[blob.name for blob in all_blobs if blob.name.endswith('csv') or blob.name.endswith('txt')]

  met_file = [file for file in all_files if file.split('/')[-1].startswith('MET')][0]
  elec_files = [file for file in all_files if not file.split('/')[-1].startswith('MET')]

  return (elec_files, met_file)

def select_files_for_staging(elec_files, met_file):
  blob = bucket.blob(met_file)
  with blob.open('r') as file:
    met_df = pd.read_csv(file, skiprows=[0, 2, 3])

  last_date_met = pd.to_datetime(met_df.tail(1)['TIMESTAMP'])
  # arr = [file.split('/')[-1].split('_')[0] for file in elec_files]
  # arr.sort()
  # last_date_elec = pd.to_datetime(arr[-1])

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

def main():
  elec_files, met_file = get_electrical_and_met_files()
  staging_files = select_files_for_staging(elec_files, met_file)

  print(staging_files)

  elec_df = concat_all_electrical_files(staging_files)
  met_df = resample_met_15min(met_file)
  merged_df = pd.merge(elec_df, met_df, how='left', on='timestamp')

  del elec_df
  del met_df
  load_result = load_df_to_bigquery(merged_df, "solren-view-etl.UT_15min.master")
  
  if load_result == 'success':
    print("Moving files to Loaded directory")

    for file in staging_files:
        
        blob = bucket.blob(file)
        dest_filename = f'data/loaded/{file.split('/')[-1]}'
        bucket.copy_blob(blob, bucket, dest_filename)
        print(f".... Copied {blob.name} to {dest_filename}")
        blob.delete()
        print(f"Original file {blob.name} deleted.")

if __name__ == '__main__':
   main()