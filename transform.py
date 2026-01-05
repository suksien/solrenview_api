import pandas as pd
import glob
import numpy as np
from datetime import date
import shutil

staging_dir = 'data/waiting_to_load/'
loaded_dir = 'data/loaded/'

def electrical_and_met_files():
  all_files = glob.glob(staging_dir + '*')
  met_file = [file for file in all_files if file.split('/')[-1].startswith('MET')][0]
  elec_files = [file for file in all_files if not file.split('/')[-1].startswith('MET')]
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
    met_df = pd.read_csv(filename, skiprows=[0, 2, 3])
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

def main():
  elec_files, met_file = electrical_and_met_files()
  elec_df = concat_all_electrical_files(elec_files)
  met_df = resample_met_15min(met_file)
  merged_df = pd.merge(elec_df, met_df, how='left', on='timestamp')
  today = date.today()
  merged_df.to_csv(loaded_dir + f'processed_{today.strftime('%Y-%m-%d')}.csv', index=False)

  all_files = glob.glob(staging_dir + '*')
  for f in all_files:
    shutil.move(f, loaded_dir)

if __name__ == '__main__':
   main()