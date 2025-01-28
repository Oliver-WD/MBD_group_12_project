import polyline
import pandas as pd
import folium
from folium.plugins import HeatMap
import os
import re


# Use this to create the parquet files, every link joined on the crash
# data = pd.read_parquet("speed_links/part-00000-36e08329-ea97-4358-a9b9-9f5fd7c585ed-c000.snappy.parquet")
#
# for foldername in os.listdir('speed_links'):
#     new_data = pd.read_parquet('speed_links/'+foldername)
#     data = pd.concat([data, new_data]).groupby("DATA_AS_OF", as_index=False).agg({'LINK_ID': 'first', 'SPEED': 'first', 'TRAVEL_TIME': 'first', 'DATA_AS_OF': 'first', 'LINK_POINTS': 'first'})
# data.to_parquet('speed_links.parquet')

data = pd.read_parquet('speed_links.parquet')

print(data)



data["weight"] = data["SPEED"].apply(lambda x: float(x)/65 if float(x) < 65 else 0)

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
    return coordinates

# Apply the processing function to the LINK_POINTS column
data["LINK_POINTS"] = data["LINK_POINTS"].apply(process_link_points)

fmap = folium.Map(location=(40.7462026,-73.9305064), zoom_start=11)
time_index = [t for t in data["DATA_AS_OF"]]

# Prepare heatmap data
heatmap_data = [[point, weight] for points, weight in zip(data["LINK_POINTS"], data["weight"]) for point in points]

heatmap = folium.plugins.HeatMapWithTime(heatmap_data, index=time_index)
HeatMap(heatmap_data).add_to(fmap)

# Save the map as an HTML file or display it
heatmap.save("heatmap.html")
