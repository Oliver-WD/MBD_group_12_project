import pandas as pd
import os

directory = '../crash_analysis_500m_b'

data = pd.read_parquet("../crash_analysis_500m_b/year=2016/part-00070-d7900872-885f-48ba-80f2-8793c2f2b1b4.c000.snappy.parquet")

for year in os.listdir(directory):
    for filename in os.listdir(directory + '/' + year):
        new_data = pd.read_parquet(os.path.join(directory + '/' + year +'/' + filename))
        data = pd.concat([data, new_data]).groupby(["crash_timestamp", "link_id"], as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
data = data.dropna()
data.to_parquet('500mb_data_link.parquet')

# data = pd.read_parquet("../crash_anal/LINK_ID=4329472/region_size=size_500m/part-00006-eb5dbaa4-69fb-4b3c-922a-15d93c616589.c000.snappy.parquet")
#
# for foldername in os.listdir(directory):
#     link_id = foldername.split('=')[1]
#     for region_folder in os.listdir(directory + '/' + foldername):
#         if '500m' in region_folder:
#             for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
#                 new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
#                 new_data["LINK_ID"] = link_id
#                 data = pd.concat([data, new_data]).groupby(["crash_timestamp", "LINK_ID"], as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
# data = data.dropna()
# data.to_parquet('500m_data_link.parquet')
#
# data = pd.read_parquet("../crash_anal/LINK_ID=4329472/region_size=size_1km/part-00025-eb5dbaa4-69fb-4b3c-922a-15d93c616589.c000.snappy.parquet")
#
# for foldername in os.listdir(directory):
#     link_id = foldername.split('=')[1]
#     for region_folder in os.listdir(directory + '/' + foldername):
#         if '1km' in region_folder:
#             for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
#                 new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
#                 new_data["LINK_ID"] = link_id
#                 data = pd.concat([data, new_data]).groupby(["crash_timestamp", "LINK_ID"], as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
# data = data.dropna()
# data.to_parquet('1km_data.parquet')
#
# data = pd.read_parquet("../crash_anal/LINK_ID=4329472/region_size=size_2km/part-00044-eb5dbaa4-69fb-4b3c-922a-15d93c616589.c000.snappy.parquet")
#
# for foldername in os.listdir(directory):
#     link_id = foldername.split('=')[1]
#     for region_folder in os.listdir(directory + '/' + foldername):
#         if '2km' in region_folder:
#             for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
#                 new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
#                 new_data["LINK_ID"] = link_id
#                 data = pd.concat([data, new_data]).groupby(["crash_timestamp", "LINK_ID"], as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
# data = data.dropna()
# data.to_parquet('2km_data.parquet')
#
# data = pd.read_parquet("../crash_anal/LINK_ID=4329472/region_size=size_5km/part-00063-eb5dbaa4-69fb-4b3c-922a-15d93c616589.c000.snappy.parquet")
#
# for foldername in os.listdir(directory):
#     link_id = foldername.split('=')[1]
#     for region_folder in os.listdir(directory + '/' + foldername):
#         if '5km' in region_folder:
#             for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
#                 new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
#                 new_data["LINK_ID"] = link_id
#                 data = pd.concat([data, new_data]).groupby(["crash_timestamp", "LINK_ID"], as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
# data = data.dropna()
# data.to_parquet('5km_data.parquet')

# data = pd.read_parquet("crash_average_speeds/part-00000-e8a35280-0ce9-400a-b230-c0cc478564a7-c000.snappy.parquet")
#
# for filename in os.listdir('crash_average_speeds'):
#     new_data = pd.read_parquet('crash_average_speeds/' + filename)
#     data = pd.concat([data, new_data]).groupby("DATA_AS_OF", as_index=False).agg({'avg(SPEED)':'mean'})
# data = data.dropna()
# data.to_parquet('average_speeds_data.parquet')
#
# for filename in os.listdir('average_speeds_links'):
#     new_data = pd.read_parquet('average_speeds_links/' + filename)
#     data = pd.concat([data, new_data]).groupby(["DATA_AS_OF", "LINK_ID"], as_index=False).agg({'avg(SPEED)':'mean'})
# data = data.dropna()
# data.to_parquet('average_speeds_linkdata.parquet')
