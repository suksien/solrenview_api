import requests
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
import utils

# extract weather data from sftp server into csv file


# extract electrical data from web api into csv file
def extract_electrical_data(start_date, end_date):
  inverter_id = [1, 2, 6]

  headers, url = utils.setup_config()  
  response_arr = utils.get_request(url, headers, start_date, end_date)
  timestamp = utils.get_timestamp(response_arr[0], start_date)

  df_arr = []
  for id in inverter_id:
    df_arr.append(utils.get_inverter_data_arr(response_arr, 1, timestamp))
  
  df = pd.DataFrame().append(df_arr)
  df.to_csv(f'data_{start_date}_{end_date}.csv')

# load csv file to cloud storage
extract_electrical_data('2025-10-25', '2025-10-26')