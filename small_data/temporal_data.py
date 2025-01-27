import os
from datetime import timedelta

import pandas as pd
import scipy.stats as stats
from matplotlib import pyplot as plt
from matplotlib import dates as mdates

# load the average speed
normal_data = pd.read_parquet('average_speeds.parquet')

# set all dates to the same date (so that resample only considers time, not date)
normal_data["timestamp"] = pd.to_datetime(normal_data["DATA_AS_OF"]).dt.time
normal_data["timestamp"] = pd.to_datetime(normal_data["timestamp"], format="%H:%M:%S")
normal_data.groupby(["timestamp"]).mean()
normal_data.set_index("timestamp", inplace=True)

normal_data = normal_data.resample('5min').mean()

# calculate difference in average speed in the same intervals as the crash data
normal_data["5min_diff"] = round(normal_data["avg(SPEED)"] - normal_data["avg(SPEED)"].shift(4), 2)
normal_data["15min_diff"] = round(normal_data["avg(SPEED)"] - normal_data["avg(SPEED)"].shift(6), 2)
normal_data["1hr_diff"] = round(normal_data["avg(SPEED)"] - normal_data["avg(SPEED)"].shift(15), 2)
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

# load the crash data (500m)
crash_data = pd.read_parquet('5km_data.parquet')

crash_data["crash_timestamp"] = pd.to_datetime(crash_data["crash_timestamp"])
crash_data.set_index(crash_data["crash_timestamp"])
crash_data = crash_data.dropna()
crash_data["timestamp"] = pd.to_datetime(crash_data["crash_timestamp"]).dt.time
crash_data["timestamp"] = pd.to_datetime(crash_data["timestamp"], format="%H:%M:%S")
crash_data.set_index("timestamp", inplace=True)


# calculate difference in average speed before and after a crash
crash_data["5min_diff"] = round(crash_data["speed_5min_after"] - crash_data["speed_15min_before"], 2)
crash_data["15min_diff"] = round(crash_data["speed_15min_after"] - crash_data["speed_15min_before"], 2)
crash_data["1hr_diff"] = round(crash_data["speed_1hr_after"] - crash_data["speed_15min_before"], 2)

# drop the unnecessary columns
crash_data = crash_data[["5min_diff", "15min_diff","1hr_diff"]]

# resample the data by hour and calculate the mean
crash_data = crash_data.resample('5min').mean()

# difference between average speeds after a crash and average speeds
data = pd.DataFrame(round(normal_data["1hr_diff"] - crash_data["1hr_diff"].shift(3), 2))

data = data.sort_values(by=["1hr_diff"])
data = data.dropna()

# check if normally distributed
stat, p_value = stats.shapiro(data["1hr_diff"].to_numpy())
print(f"P-value: {p_value:.4f}")
if p_value < 0.05:
    print("Normally distributed")
    # is the mean significantly different from 0?
    t_stat, p_value = stats.ttest_1samp(data["1hr_diff"].to_numpy(), 0)
    print(f"T-statistic: {t_stat:.4f}")
    print(f"P-value: {p_value:.4f}")

    if p_value / 2 < 0.05:
        if t_stat > 0:
            print("Cars drive slower after a crash")
        else:
            print("Cars drive faster after a crash")



ax = data["1hr_diff"].plot.hist(bins=30, alpha=0.7, edgecolor="black")
plt.axvline(data["1hr_diff"].mean(), color="red", linestyle="dashed", linewidth=1.5, label=f"Mean: {data['1hr_diff'].mean():.2f}")
plt.title("Distribution of 5min Differences")
plt.xlabel("1hr_diff")
plt.ylabel("Frequency")
plt.legend()
plt.show()
# plt.savefig("500meter_hour_temporal.png")
# plt.show()


