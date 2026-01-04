import pandas as pd
import glob

def hack():
  all_files = glob.glob('waiting_to_load/*')
  df = concat_all_electrical_files(all_files)

  met_file = [file for file in all_files if file.split('/')[-1].startswith('MET')][0]
  update_electrical_with_met(df, met_file)
  df.to_csv('processed.csv')

def concat_all_electrical_files(all_files):
  pass


def update_electrical_with_met(df, met_file):
  met_df = pd.read_csv(met_file, skiprows=[0, 2, 3])
