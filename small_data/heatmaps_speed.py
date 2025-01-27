import polyline
import pandas as pd
import folium
from folium.plugins import HeatMap
import os
import re


# Use this to create the parquet files, every link joined on the crash
# directory = '../crash_anal'
#
# data = pd.read_parquet('../crash_anal/LINK_ID=4329472/region_size=size_500m')
#
# for foldername in os.listdir(directory):
#     link_id = foldername.split('=')[1]
#     for region_folder in os.listdir(directory + '/' + foldername):
#         if '500m' in region_folder:
#             for filename in os.listdir(directory + '/' + foldername + '/' + region_folder):
#                 new_data = pd.read_parquet(directory + '/' + foldername + '/' + region_folder + '/' + filename)
#                 new_data["LINK_ID"] = link_id
#                 data = pd.concat([data, new_data]).groupby("crash_timestamp", as_index=False).agg({'LATITUDE':'first', 'LONGITUDE':'first', 'LINK_ID':'first', 'speed_5min_after':'mean', 'speed_15min_before':'mean', 'speed_1hr_after':'mean', 'speed_15min_after':'mean'})
# data = data.dropna()
# data.to_parquet('links_500m.parquet')

data = pd.read_parquet('links_500m.parquet')
poly = pd.read_parquet('links_to_poly.parquet')


data = data.merge(poly, left_on='LINK_ID', right_on='LINK_ID', how='inner')
data["ENCODED_POLY_LINE"] = data["ENCODED_POLY_LINE"].apply(lambda x: re.sub(r"\\\\+", "", x))
data["ENCODED_POLY_LINE"] = data["ENCODED_POLY_LINE"].apply(lambda x: re.sub(r"B\\|", "B|", x))
data.to_parquet("test.parquet")
print(data)
data["POLY_LINE"] = data["ENCODED_POLY_LINE"].apply(lambda x: polyline.decode(x))



data["weight"] = data["5min_diff"]
heatmap = folium.map(location=(40.7462026,-73.9305064))
heatmap_data = data[["LATITUDE", "LONGITUDE", "weight"]].values.toList
HeatMap(heatmap_data).add_to(heatmap)

# Save the map as an HTML file or display it
heatmap.save("heatmap.html")
