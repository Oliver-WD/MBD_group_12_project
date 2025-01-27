import os
from datetime import timedelta

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import dates as mdates

normal_data = pd.read_parquet('average_speeds.parquet')

print(normal_data)

normal_data["timestamp"] = pd.to_datetime(normal_data["DATA_AS_OF"]).dt.time
normal_data["timestamp"] = pd.to_datetime(normal_data["timestamp"], format="%H:%M:%S")
normal_data.groupby(["timestamp"]).mean()
normal_data.set_index("timestamp", inplace=True)

normal_data = normal_data.resample('5min').mean()

normal_data["5min_diff"] = round(normal_data["avg(SPEED)"] - normal_data["avg(SPEED)"].shift(4), 2)
normal_data["15min_diff"] = round(normal_data["avg(SPEED)"] - normal_data["avg(SPEED)"].shift(6), 2)
normal_data["1h_diff"] = round(normal_data["avg(SPEED)"] - normal_data["avg(SPEED)"].shift(15), 2)
#
# print(data)
#
# ax = data.plot(y=["5min_diff", "15min_diff", "1h_diff"], kind='line')
# ax.grid(True)
# ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
# ax.set_ylabel('Average speed (mph)')
#
# plt.savefig("temporal_speeds.png")
# plt.show()

crash_data = pd.read_parquet('500m_data.parquet')

crash_data["crash_timestamp"] = pd.to_datetime(crash_data["crash_timestamp"])
crash_data.set_index(crash_data["crash_timestamp"])
crash_data = crash_data.dropna()
crash_data["timestamp"] = pd.to_datetime(crash_data["crash_timestamp"]).dt.time
crash_data["timestamp"] = pd.to_datetime(crash_data["timestamp"], format="%H:%M:%S")
crash_data.set_index("timestamp", inplace=True)

crash_data["5min_diff"] = round(crash_data["speed_5min_after"] - crash_data["speed_15min_before"], 2)
crash_data["15min_diff"] = round(crash_data["speed_15min_after"] - crash_data["speed_15min_before"], 2)
crash_data["1hr_diff"] = round(crash_data["speed_1hr_after"] - crash_data["speed_15min_before"], 2)


crash_data = crash_data[["5min_diff", "15min_diff","1hr_diff"]]

# Resample the data by hour and calculate the mean
crash_data = crash_data.resample('5min').mean()

data = pd.DataFrame(round(normal_data["5min_diff"]- crash_data["5min_diff"], 2))

data = data.sort_values(by=["5min_diff"])

print(data)

ax = data.plot.hist(column="5min_diff")
# plt.savefig("500meter_hour_temporal.png")
plt.show()


