import requests
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np

##### connections #####
def setup_config():
  headers = {
  'Cookie': 'siteId=6073; PHPSESSID=0ooqna8h758bs6c4jl9gl6reu7; _ga=GA1.2.792316693.1761186174; _gid=GA1.2.2038752395.1761186174; _gat=1; _ga_L4S8LG26JM=GS2.2.s1761186174$o1$g1$t1761186879$j60$l0$h0',
  'Referer': 'https://www.solrenview.com/srvp/FSC/SRV/analyticsT.php?siteId=6073&CurrentTab=3',
  'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36'
  }

  url = 'https://www.solrenview.com/srvp/FSC/SRV/getAnalyticsInfo.php'

  return (headers, url)

def construct_params(start_date, end_date, data_interval=300):

  end_interval = f'{start_date} 04:' + '%02d' % (int(data_interval/60)) + ':00'

  common_params = {
    'siteId': '6073',
    'queryNo': '4',
    'startIntervalTs': f'{start_date} 04:00:00',
    'endIntervalTs': f'{end_interval}',
    'endDuration': f'{end_date} 00:00:00',
    'dataInterval': data_interval,
    'hasRg': '0',
    'maxLimit': '360000',
    'gauge': '0 ',
    'dataIntervalStr': 'Minute'
  }

  # inverter 1 and 2
  params12 = {
    'deviceindices': '0^35346^Inverter 1  [1013821839182 PVI 60TL]^3;0^35347^Inverter 2  [1013821839191 PVI 60TL]^3',
    'parList': 'vdc;vdc2;vdc3;vdc4;idc;idc2;idc3;idc4',
    'deviceType': '0^35346^Inverter 1 [1013821839182 PVI 60TL]^3;0^35347^Inverter 2 [1013821839191 PVI 60TL]^3',
  }
  params12.update(common_params)

  params16 = {
    'deviceindices': '0^35346^Inverter 1  [1013821839182 PVI 60TL]^3;0^35351^Inverter 6  [1013821824186 PVI 60TL]^3',
    'parList': 'vdc;vdc2;vdc3;vdc4;idc;idc2;idc3;idc4',
    'deviceType': '0^35346^Inverter 1 [1013821839182 PVI 60TL]^3;0^35351^Inverter 6 [1013821824186 PVI 60TL]^3',
  }
  params16.update(common_params)

  '''
  params12 = {
    'siteId': '6073',
    'queryNo': '4',
    'startIntervalTs': f'{start_date} 04:00:00',
    'endIntervalTs': f'{end_interval}',
    'endDuration': f'{end_date} 00:00:00',
    'dataInterval': data_interval,
    'deviceindices': '0^35346^Inverter 1  [1013821839182 PVI 60TL]^3;0^35347^Inverter 2  [1013821839191 PVI 60TL]^3',
    'parList': 'vdc;vdc2;vdc3;vdc4;idc;idc2;idc3;idc4',
    'deviceType': '0^35346^Inverter 1 [1013821839182 PVI 60TL]^3;0^35347^Inverter 2 [1013821839191 PVI 60TL]^3',
    'hasRg': '0',
    'maxLimit': '360000',
    'gauge': '0 ',
    'dataIntervalStr': 'Minute'
  }

  params16 = {
    'siteId': '6073',
    'queryNo': '4',
    'startIntervalTs': f'{start_date} 04:00:00',
    'endIntervalTs': f'{start_date} 04:05:00',
    'endDuration': f'{end_date} 00:00:00',
    'dataInterval': 300,
    'deviceindices': '0^35346^Inverter 1  [1013821839182 PVI 60TL]^3;0^35351^Inverter 6  [1013821824186 PVI 60TL]^3',
    'parList': 'vdc;vdc2;vdc3;vdc4;idc;idc2;idc3;idc4',
    'deviceType': '0^35346^Inverter 1 [1013821839182 PVI 60TL]^3;0^35351^Inverter 6 [1013821824186 PVI 60TL]^3',
    'hasRg': '0',
    'maxLimit': '360000',
    'gauge': '0 ',
    'dataIntervalStr': 'Minute'
    }
    '''
  
  return (params12, params16)

def get_request(url, headers, start_date, end_date, data_interval=300):
  params12, params16 = construct_params(start_date, end_date, data_interval=data_interval)
  response12 = requests.get(url, params=params12, headers=headers)
  status = "request OK" if response12.status_code == 200 else response12.status_code
  print(status)
  
  response16 = requests.get(url, params=params16, headers=headers)
  status = "request OK" if response16.status_code == 200 else response16.status_code
  print(status)

  response_arr = [response12, response16]

  return response_arr

##### transform #####
def get_timestamp(response, start_date):
  cat = response.json()['categories']

  time_labels = cat[0]['category']
  timestamp = []
  for label in time_labels:
    str = start_date + ' ' + label['label']
    timestamp.append(str.upper())

  timestamp = pd.to_datetime(timestamp)
  return timestamp

def explore_response_shape(resp):
  # response_arr[0].json() returns a dict
  # keys for the dict are ['chart', 'categories', 'dataset', 'styles']
  # 'dataset' value is a list of length 12 (6 data from inverter 1 & 2 and 6 data from inverter 1 & 6)
  # each elem from 'dataset' is a dict with keys ['seriesname', 'renderas', 'parentyaxis', 'data']
  # 'seriesname' is similar to the column name of the 'data'

  # usage: 
  #   utils.explore_response_shape(response_arr[0]) 
  #   utils.explore_response_shape(response_arr[1])
  arr = resp.json()['dataset']
  for dict_obj in arr:
    print(dict_obj['seriesname'], len(dict_obj['data']), type(dict_obj['data']))

def get_inverter_data_arr(response_arr, inverter_num):
  if inverter_num == 1:
    start_index, end_index = 0, 6
    response_index = 0
  elif inverter_num == 2:
    start_index, end_index = 6, 12
    response_index = 0
  elif inverter_num == 6:
    start_index, end_index = 6, 12
    response_index = 1
  
  arr = response_arr[response_index].json()['dataset'][start_index : end_index]

  
  data = {
    'config': 0,
    'voltage': 0,
    'current': 0,
  }
  
  volt_data = []
  curr_data = []

  voltage1 = arr[0]['data']
  current1 = arr[3]['data']
  
  for i in range(len(voltage1)):
    volt_data.append(voltage1['value'])
    curr_data.append(current1['value'])



  # config, voltage, current
  # inv1-str1, .., ...
  # inv1-str1, ..., ...
  # inv1-str2, ..., ...

  return data
  # todo: unpack arr to extract I and V

####### TBD

def get_inv_data_arr(response_arr):
  inv1_iv_data_arr = response_arr[0].json()['dataset'][0:6]
  inv2_iv_data_arr = response_arr[0].json()['dataset'][6:]
  inv6_iv_data_arr = response_arr[1].json()['dataset'][6:]

  return [inv1_iv_data_arr, inv2_iv_data_arr, inv6_iv_data_arr]
  
def get_electrical_data(all_inv_data_arr, timestamp):
  all_volt_data = []
  all_curr_data = []
  all_timestamp = []

  for i_inv, inv_data_arr in enumerate(all_inv_data_arr):
    for i, data_dict in enumerate(inv_data_arr):
      if i < 3:
        arr = [float(dict['value']) if dict['value'] != '' else 0 for dict in data_dict['data']]
        all_volt_data.extend(arr)
        all_timestamp.extend(timestamp)
      else:
        arr = [float(dict['value']) if dict['value'] != '' else 0 for dict in data_dict['data']]
        all_curr_data.extend(arr)

  return all_volt_data, all_curr_data, all_timestamp

def get_equipment(all_inv_data_arr):
  all_equipment = []
  for i_inv in range(len(all_inv_data_arr)):
    for i, data_dict in enumerate(inv_data_arr[0:3]):
      equipment_code = f'inv{i_inv + 1}_str{(i % 3) + 1}'
      all_equipment.extend([equipment_code] * len(data_dict['data']))

  return all_equipment

def transform_data(response_arr, timestamp):
  all_inv_data_arr = get_inv_data_arr(response_arr)
  all_volt_data, all_curr_data, all_timestamp = get_electrical_data(response_arr, all_inv_data_arr, timestamp)
  all_equipment = get_equipment(all_inv_data_arr)

  data = {'timestamp': all_timestamp, 'equipment_code': all_equipment, 'current': all_curr_data, 'voltage': all_volt_data}
  df = pd.DataFrame(data=data)