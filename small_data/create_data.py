import pandas as pd
import os

directory = '../crash_anal'

data = pd.DataFrame

for foldername in os.listdir(directory):
    for region_folder in os.listdir(directory + '/' + foldername):
        if '500m' in region_folder:
            for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
                new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
                data = pd.concat([data, new_data]).groupby("crash_timestamp", as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
data = data.dropna()
data.to_parquet('500m_data.parquet')

data = pd.DataFrame

for foldername in os.listdir(directory):
    for region_folder in os.listdir(directory + '/' + foldername):
        if '1km' in region_folder:
            for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
                new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
                data = pd.concat([data, new_data]).groupby("crash_timestamp", as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
data = data.dropna()
data.to_parquet('1km_data.parquet')

data = pd.DataFrame

for foldername in os.listdir(directory):
    for region_folder in os.listdir(directory + '/' + foldername):
        if '2km' in region_folder:
            for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
                new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
                data = pd.concat([data, new_data]).groupby("crash_timestamp", as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
data = data.dropna()
data.to_parquet('2km_data.parquet')

data = pd.DataFrame

for foldername in os.listdir(directory):
    for region_folder in os.listdir(directory + '/' + foldername):
        if '5km' in region_folder:
            for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
                new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
                data = pd.concat([data, new_data]).groupby("crash_timestamp", as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
data = data.dropna()
data.to_parquet('5km_data.parquet')

