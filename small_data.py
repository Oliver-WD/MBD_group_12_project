import pandas as pd

# load in the data as Pandas dataframe
data = pd.read_csv("/home/s2457512/motorvehicle_collision.gz", compression="gzip")

# Go from a dataframe with two columns containing strings of crash date and crash time, to one column with a datetime
data["datetime"] = data["./"] + " " + data["CRASH TIME"]
data["datetime"] = pd.to_datetime(data["datetime"])
