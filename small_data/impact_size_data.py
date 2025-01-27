import pandas as pd

fiveh_m = pd.read_parquet('500m_data.parquet').set_index("crash_timestamp")
one_km = pd.read_parquet('1km_data.parquet').set_index("crash_timestamp")
two_km = pd.read_parquet('2km_data.parquet').set_index("crash_timestamp")
five_km = pd.read_parquet('5km_data.parquet').set_index("crash_timestamp")

one_km = one_km[["speed_5min_after", "speed_15min_before", "speed_15min_after", "speed_1hr_after"]]
two_km = two_km[["speed_5min_after", "speed_15min_before", "speed_15min_after", "speed_1hr_after"]]
five_km = five_km[["speed_5min_after", "speed_15min_before", "speed_15min_after", "speed_1hr_after"]]

data = fiveh_m.join(one_km, how="inner", lsuffix="5m", rsuffix="1km")
data = data.join(two_km, how="inner", rsuffix="2km")
data = data.join(five_km, how="inner", rsuffix="5km")

print(data)
data.to_parquet('impact.parquet')