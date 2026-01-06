import pandas as pd
import numpy as np
import shutil
from google.cloud import storage, bigquery

# staging_dir = 'data/waiting_to_load/'
# loaded_dir = 'data/loaded/'
# staging_files = []

storage_client = storage.Client()
bucket = storage_client.bucket('uni_toledo')

def get_electrical_and_met_files():
  #all_files = glob.glob(staging_dir + '*')
  all_blobs = storage_client.list_blobs('uni_toledo', prefix='data/waiting_to_load')
  all_files =[blob.name for blob in all_blobs if blob.name.endswith('csv') or blob.name.endswith('txt')]

  met_file = [file for file in all_files if file.split('/')[-1].startswith('MET')][0]
  elec_files = [file for file in all_files if not file.split('/')[-1].startswith('MET')]

  # TODO: select electrical file within the last date from met file
  # append these files to `staging_file` array
  # append met file ONLY if its last date < latest electrical file 
  return (elec_files, met_file)

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
  # print(f'Loaded {table.num_rows} rows and {len(table.schema)} to {table_id}')

def main():
  elec_files, met_file = get_electrical_and_met_files()
  elec_df = concat_all_electrical_files(elec_files)
  met_df = resample_met_15min(met_file)
  merged_df = pd.merge(elec_df, met_df, how='left', on='timestamp')

  load_df_to_bigquery(merged_df, "solren-view-etl.UT_15min.master")

  # all_files = glob.glob(staging_dir + '*')
  
  # for f in all_files:
  #   shutil.move(f, loaded_dir)

if __name__ == '__main__':
   main()