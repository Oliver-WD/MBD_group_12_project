import pandas as pd
import scipy.stats as stats
from matplotlib import pyplot as plt
from matplotlib import dates as mdates

# load the crash data
crash_data500b = pd.read_parquet('500mb_data_link.parquet')
crash_data1000b = pd.read_parquet('1000mb_data_link.parquet')
crash_data2000b = pd.read_parquet('2000mb_data_link.parquet')

analyzed = "500"
# choose which data to analyze
crash_data_all = crash_data500b


# only use pre-covid data
crash_data_all = crash_data_all[(crash_data_all["crash_timestamp"] < '2020-01-01') & (crash_data_all["crash_timestamp"] > '2017-01-01')]

# set all dates to the same date (so that resample only considers time, not date)
crash_data_all.set_index(crash_data_all["crash_timestamp"])
crash_data_all = crash_data_all.dropna()
crash_data_all["timestamp"] = pd.to_datetime(crash_data_all["crash_timestamp"]).dt.time
crash_data_all["timestamp"] = pd.to_datetime(crash_data_all["timestamp"], format="%H:%M:%S")
crash_data_all.set_index("timestamp", inplace=True)

# drop the unnecessary columns
crash_data_all = crash_data_all[["speed_15min_before", "speed_5min_after", "speed_15min_after", "speed_1hr_after"]]

# set all dates to the same date (so that resample only considers time, not date)
crash_data1000b.set_index(crash_data1000b["crash_timestamp"])
crash_data1000b = crash_data1000b.dropna()
crash_data1000b["timestamp"] = pd.to_datetime(crash_data1000b["crash_timestamp"]).dt.time
crash_data1000b["timestamp"] = pd.to_datetime(crash_data1000b["timestamp"], format="%H:%M:%S")
crash_data1000b.set_index("timestamp", inplace=True)

# drop the unnecessary columns
crash_data1000b = crash_data1000b[["speed_15min_before", "speed_5min_after", "speed_15min_after", "speed_1hr_after"]]

crash_data1000b = crash_data1000b.resample('5min').mean()

crash_data2000b.set_index(crash_data2000b["crash_timestamp"])
crash_data2000b = crash_data2000b.dropna()
crash_data2000b["timestamp"] = pd.to_datetime(crash_data2000b["crash_timestamp"]).dt.time
crash_data2000b["timestamp"] = pd.to_datetime(crash_data2000b["timestamp"], format="%H:%M:%S")
crash_data2000b.set_index("timestamp", inplace=True)

# drop the unnecessary columns
crash_data2000b = crash_data2000b[["speed_15min_before", "speed_5min_after", "speed_15min_after", "speed_1hr_after"]]

crash_data2000b = crash_data2000b.resample('5min').mean()

crash_data = crash_data_all
normal_data = pd.read_parquet('5min_average_speed_normalized.parquet')

# normal_data = normal_data.resample('5min').mean()
crash_data = crash_data.resample('5min').mean()

# difference between average speeds after a crash and average speeds.
# shifts are needed, speed_1hr_after is 01:00:00 if timestamp says 00:00:00
data = pd.DataFrame(round(normal_data["avg(SPEED)"] - crash_data["speed_15min_before"].shift(-3), 2))
data.columns = ['speed_diff_bef']
data["speed_diff_5aft"] = round(normal_data["avg(SPEED)"] - crash_data["speed_5min_after"].shift(1), 2)
data["speed_diff_15aft"] = round(normal_data["avg(SPEED)"] - crash_data["speed_15min_after"].shift(3), 2)
data["speed_diff_1aft"] = round(normal_data["avg(SPEED)"] - crash_data["speed_1hr_after"].shift(12), 2)
data["500vs1000"] = round(crash_data["speed_5min_after"] - crash_data1000b["speed_5min_after"], 2)
data["500vs2000"] = round(crash_data["speed_5min_after"] - crash_data2000b["speed_5min_after"], 2)
data["500vs100015"] = round(crash_data["speed_15min_before"] - crash_data1000b["speed_15min_before"], 2)

# get the difference in speed
data["average_speed"] = normal_data["avg(SPEED)"]
data["speed_15min_before"] = crash_data["speed_15min_before"].shift(-3)
data["speed_5min_after"] = crash_data["speed_5min_after"].shift(1)
data["speed_15min_after"] = crash_data["speed_15min_after"].shift(3)
data["speed_1hr_after"] = crash_data["speed_1hr_after"].shift(12)
data = data.dropna()

ax = data.plot(y=["average_speed"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
plt.savefig("hourly_average_speeds.png")
plt.show()

ax = data.plot(y=["average_speed","speed_15min_before"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
ax.set_title(f'Crash speed for {analyzed} meters')
plt.savefig("15before_speeds.png")
plt.show()

ax = data.plot(y=["average_speed", "speed_5min_after"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
ax.set_title(f'Crash speed for {analyzed} meters')
plt.savefig("5min_after.png")
plt.show()


ax = data.plot(y=["average_speed", "speed_15min_after"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
ax.set_title(f'Crash speed for {analyzed} meters')
plt.show()

ax = data.plot(y=["average_speed", "speed_1hr_after"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
ax.set_title(f'Crash speed for {analyzed} meters')
plt.show()

ax = data.plot(y=["average_speed", "speed_5min_after", "speed_15min_after", "speed_1hr_after"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
ax.set_title(f'Crash speed for {analyzed} meters')
plt.show()

if data["average_speed"].count() > 3:
    differences = ["speed_diff_bef", "speed_diff_5aft", "speed_diff_15aft", "speed_diff_1aft", "500vs1000", "500vs2000", "500vs100015"]
    for diff in differences:
        stat, p_value = stats.shapiro(data[diff].to_numpy())
        print(f"P-value: {p_value:.4f} for {diff}")

        ax = data[diff].plot.hist(bins=30, alpha=0.7, edgecolor="black")
        plt.axvline(data[diff].mean(), color="red", linestyle="dashed", linewidth=1.5,
                    label=f"Mean: {data[diff].mean():.2f}")
        plt.title(f"Distribution of {diff} for {analyzed} meters")
        plt.xlabel(diff)
        plt.ylabel("Frequency")
        plt.legend()
        plt.show()

        if p_value < 0.05:
            print(f"{diff} for {analyzed} is normally distributed")
            # is the mean significantly different from 0?
            t_stat, p_value = stats.ttest_1samp(data[diff].to_numpy(), 0)
            print(f"T-statistic: {t_stat:.4f}")
            print(f"P-value: {p_value:.10f}")

            if p_value < 0.05:
                if t_stat > 0:
                    print("Cars drive slower after a crash")
                else:
                    print("Cars drive faster after a crash")