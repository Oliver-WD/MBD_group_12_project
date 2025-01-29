import itertools
from datetime import timedelta
import pandas as pd
import os
import folium
from folium.plugins import HeatMap
import geopy.distance
import aspose.words as aw

# Use this to create the parquet files, every link joined on the crash
# data = pd.read_parquet("speed_links/part-00000-36e08329-ea97-4358-a9b9-9f5fd7c585ed-c000.snappy.parquet")

# for foldername in os.listdir('speed_links'):
#     new_data = pd.read_parquet('speed_links/'+foldername)
#     data = pd.concat([data, new_data]).groupby("DATA_AS_OF", as_index=False).agg({'LINK_ID': 'first', 'SPEED': 'first', 'TRAVEL_TIME': 'first', 'DATA_AS_OF': 'first', 'LINK_POINTS': 'first'})
# data.to_parquet('speed_links.parquet')

data = pd.read_parquet('speed_links.parquet')
crash_data = pd.read_parquet('500mb_data_link.parquet')
crash_data = crash_data[crash_data["speed_5min_after"] != 0]
crash = crash_data.sample(1)
crash_time = crash.iloc[0]["crash_timestamp"]
crash_coords = (crash.iloc[0]["LATITUDE"], crash.iloc[0]["LONGITUDE"])

data = data[data["DATA_AS_OF"] >= crash_time]
data = data[data["DATA_AS_OF"] <= crash_time + timedelta(hours=1)]

# Weight modifier. Makes it so that 0mph glows bright red in the heatmap and high speeds a soft blue
weight_15min_before = crash.iloc[0]["speed_15min_before"]

weight_15min_before = (65-float(crash.iloc[0]["speed_15min_before"]))/65 if float(crash.iloc[0]["speed_15min_before"]) < 65 else 0
weight_5min = (65-float(crash.iloc[0]["speed_5min_after"]))/65 if float(crash.iloc[0]["speed_5min_after"]) < 65 else 0
weight_15min = (65-float(crash.iloc[0]["speed_15min_after"]))/65 if float(crash.iloc[0]["speed_15min_after"]) < 65 else 0
weight_1hr = (65-float(crash.iloc[0]["speed_1hr_after"]))/65 if float(crash.iloc[0]["speed_1hr_after"]) < 65 else 0


# Function to process LINK_POINTS column
def process_link_points(link_points):
    # Split the string by spaces and group into coordinate pairs
    points = link_points.strip().split(' ')
    coordinates = []
    for point in points:
        if ',' in point:
            try:
                lat, lon = map(float, point.split(','))
                coordinates.append([lat, lon])
            except:
                continue
    return coordinates


# Apply the processing function to the LINK_POINTS column
data["LINK_POINTS"] = data["LINK_POINTS"].apply(process_link_points)
print(data.iloc[0]["LINK_POINTS"])

map_15_before = folium.Map(location=crash_coords, zoom_start=14)
map_5_after = folium.Map(location=crash_coords, zoom_start=14)
map_15_after = folium.Map(location=crash_coords, zoom_start=14)
map_1h_after = folium.Map(location=crash_coords, zoom_start=14)

# Prepare heatmap data
# heatmap_points = [[point[0], point[1]] for points in data["LINK_POINTS"] for point in points]
# print(heatmap_points[0])

# Create a heatmap for every link instance
heatmap_range_data = []
prev_weight = 0
index = 0
for _, row in data.iterrows():
    link_points = row["LINK_POINTS"]
    for link_point in link_points:
        test_point = (link_point[0], link_point[1])
        print(test_point)
        # Only consider the data if it is in 1km of the crash site
        if geopy.distance.geodesic(test_point, crash_coords).km <= 1:
            heatmap_range_data.append([link_point[0], link_point[1]])

for point in heatmap_range_data:
    print(point[0], point[1])
    HeatMap([[point[0], point[1], weight_15min_before]]).add_to(map_15_before)
    HeatMap([[point[0], point[1], weight_5min]]).add_to(map_5_after)
    HeatMap([[point[0], point[1], weight_15min]]).add_to(map_15_after)
    HeatMap([[point[0], point[1], weight_1hr]]).add_to(map_1h_after)

print(weight_15min_before, weight_5min, weight_15min, weight_1hr)
folium.Marker(location=crash_coords).add_to(map_15_before)
folium.Marker(location=crash_coords).add_to(map_5_after)
folium.Marker(location=crash_coords).add_to(map_15_after)
folium.Marker(location=crash_coords).add_to(map_1h_after)
map_15_before.save("heatmaps/heatmap_15_before.html")
map_5_after.save("heatmaps/heatmap_5_after.html")
map_15_after.save("heatmaps/heatmap_15_after.html")
map_1h_after.save("heatmaps/heatmap_1h_after.html")




        # if current_weight != prev_weight and prev_weight != 0:
        #     print(current_weight, prev_weight)
        #     HeatMap(heatmap_range_data).add_to(fmap)
        #
        #     # Save the map as an HTML file or display it
        #     folium.Marker(location=crash_coords, popup=point[3]).add_to(fmap)
        #     # Save the heatmap as an html file
        #     fmap.save("heatmaps/heatmap"+str(index)+".html")
        #     print(index)
        #     index += 1
        #     heatmap_range_data = []
        # prev_weight = current_weight