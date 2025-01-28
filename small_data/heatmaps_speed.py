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
crash_data = pd.read_parquet('500m_data.parquet')
crash_data = crash_data[crash_data["speed_5min_after"] != 0]
crash = crash_data.sample(1)
crash_time = crash.iloc[0]["crash_timestamp"]
crash_coords = (crash.iloc[0]["LATITUDE"], crash.iloc[0]["LONGITUDE"])

data = data[data["DATA_AS_OF"] >= crash_time]
data = data[data["DATA_AS_OF"] <= crash_time + timedelta(hours=1)]

# Weight modifier. Makes it so that 0mph glows bright red in the heatmap and high speeds a soft blue
data["weight"] = data["SPEED"].apply(lambda x: (65-float(x))/65 if float(x) < 65 else 0)


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

fmap = folium.Map(location=crash_coords, zoom_start=16)
# Prepare heatmap data
heatmap_data = [[point[0], point[1], weight, timestamp] for points, weight, timestamp in zip(data["LINK_POINTS"], data["weight"], data["DATA_AS_OF"]) for point in points]

# Create a heatmap for every link instance
heatmap_range_data = []
prev_weight = 0
index = 0
for point in heatmap_data:
    test_point = (point[0], point[1])
    current_weight = point[2]
    # Only consider the data if it is in 1km of the crash site
    if geopy.distance.geodesic(test_point, crash_coords).km <= 1:
        heatmap_range_data.append([point[0],point[1], point[2]])
        if current_weight != prev_weight and prev_weight != 0:
            print(current_weight, prev_weight)
            HeatMap(heatmap_range_data).add_to(fmap)

            # Save the map as an HTML file or display it
            folium.Marker(location=crash_coords, popup=point[3]).add_to(fmap)
            # Save the heatmap as an html file
            fmap.save("heatmaps/heatmap"+str(index)+".html")
            print(index)
            index += 1
            heatmap_range_data = []
        prev_weight = current_weight