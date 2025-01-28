import pandas as pd
import scipy.stats as stats
from matplotlib import pyplot as plt
from matplotlib import dates as mdates

# load the crash data
crash_data500 = pd.read_parquet('500m_data_link.parquet')
crash_data500b = pd.read_parquet('500mb_data_link.parquet')
crash_data1 = pd.read_parquet('1km_data.parquet')
crash_data2 = pd.read_parquet('2km_data.parquet')
crash_data5 = pd.read_parquet('5km_data.parquet')

# choose which data to analyze
crash_data_all = crash_data500b

############# NORMALIZE THE SPEED BASED ON THE LINK_IDS, SAVED IN 5MINAVERAGE BLAH BLAH #############
# load the average speed
# normal_data_all = pd.read_parquet('average_speeds_linkdata.parquet')
#
# # only use pre-covid data
# normal_data_all = normal_data_all[(normal_data_all["DATA_AS_OF"] < '2020-01-01') & (normal_data_all["DATA_AS_OF"] > '2017-01-01')]
#
# # ids that should be considered for calculating the average speed
# valid_ids = pd.unique(crash_data_all["link_id"])
#
# crash_counts = crash_data_all["link_id"].value_counts()
#
# # set all dates to the same date (so that resample only considers time, not date)
# normal_data_all["timestamp"] = pd.to_datetime(normal_data_all["DATA_AS_OF"]).dt.time
# normal_data_all["timestamp"] = pd.to_datetime(normal_data_all["timestamp"], format="%H:%M:%S")
# normal_data_all = normal_data_all[["timestamp", "LINK_ID", "avg(SPEED)"]]
# normal_data_all.groupby(["timestamp", "LINK_ID"]).mean()
# normal_data_all.set_index("timestamp", inplace=True)
#
# # normalize the data based on how often a link appears in the crash dataset (links appearing more often have a strong 'weight)
# first_id = valid_ids[0]
# normal_data_temp = normal_data_all[normal_data_all["LINK_ID"] == first_id]
# normal_data_temp = normal_data_temp["avg(SPEED)"]
# normal_data_temp = normal_data_temp.resample('5min').mean()
# normal_data_temp = normal_data_temp.to_frame()
# print(normal_data_temp)
#
# i = 0
# for id in valid_ids:
#     counts = crash_counts.loc[id]
#     if i != 0:
#         next_data_id = normal_data_all[normal_data_all["LINK_ID"] == id]
#         next_data_id = next_data_id["avg(SPEED)"]
#         next_data_id = next_data_id.resample('5min').mean()
#         next_data_id = next_data_id.to_frame()
#         normal_data_temp["speed"] = next_data_id["avg(SPEED)"]
#         normal_data_temp["avg(SPEED)"] = normal_data_temp["avg(SPEED)"] * (i/(i+counts)) + normal_data_temp["speed"] * (crash_counts.loc[id]/(i+counts))
#     i += crash_counts.loc[id]
#
# normal_data_temp.to_parquet('5min_average_speed_normalized.parquet')
########################################################################################################


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
crash_data_all["speed_15min_before"] = pd.to_numeric(crash_data_all["speed_15min_before"])

crash_data = crash_data_all
normal_data = pd.read_parquet('5min_average_speed_normalized.parquet')

print(normal_data.shape)
print(normal_data)
print(normal_data["avg(SPEED)"])
print(crash_data["speed_15min_before"])

# normal_data = normal_data.resample('5min').mean()
crash_data = crash_data.resample('5min').mean()

# difference between average speeds after a crash and average speeds.
# shifts are needed, speed_1hr_after is 01:00:00 if timestamp says 00:00:00
data = pd.DataFrame(round(normal_data["avg(SPEED)"] - crash_data["speed_15min_before"].shift(-3), 2))
data.columns = ['speed_diff_bef']
data["speed_diff_5aft"] = round(normal_data["avg(SPEED)"] - crash_data["speed_5min_after"].shift(1), 2)
data["speed_diff_15aft"] = round(normal_data["avg(SPEED)"] - crash_data["speed_15min_after"].shift(3), 2)
data["speed_diff_1aft"] = round(normal_data["avg(SPEED)"] - crash_data["speed_1hr_after"].shift(12), 2)

# get the difference in speed
data["average_speed"] = normal_data["avg(SPEED)"]
data["speed_15min_before"] = crash_data["speed_15min_before"].shift(-3)
data["speed_5min_after"] = crash_data["speed_5min_after"].shift(1)
data["speed_15min_after"] = crash_data["speed_15min_after"].shift(3)
data["speed_1hr_after"] = crash_data["speed_1hr_after"].shift(12)
data = data.dropna()

print(normal_data)

ax = data.plot(y=["average_speed"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
# plt.savefig("temporal_speeds.png")
plt.show()

ax = data.plot(y=["average_speed","speed_15min_before"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
# plt.savefig("temporal_speeds.png")
plt.show()

ax = data.plot(y=["average_speed", "speed_5min_after"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
# plt.savefig("temporal_speeds.png")
plt.show()


ax = data.plot(y=["average_speed", "speed_15min_after"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
# plt.savefig("temporal_speeds.png")
plt.show()

ax = data.plot(y=["average_speed", "speed_1hr_after"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
# plt.savefig("temporal_speeds.png")
plt.show()

ax = data.plot(y=["average_speed", "speed_5min_after", "speed_15min_after", "speed_1hr_after"], kind='line')
ax.grid(True)
ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
ax.set_ylabel('Average speed (mph)')
# plt.savefig("temporal_speeds.png")
plt.show()

if data["average_speed"].count() > 3:
    differences = ["speed_diff_bef", "speed_diff_5aft", "speed_diff_15aft", "speed_diff_1aft"]
    for diff in differences:
        stat, p_value = stats.shapiro(data[diff].to_numpy())
        print(f"P-value: {p_value:.4f}")
        if p_value < 0.05:
            print("Normally distributed")
            # is the mean significantly different from 0?
            t_stat, p_value = stats.ttest_1samp(data[diff].to_numpy(), 0)
            print(f"T-statistic: {t_stat:.4f}")
            print(f"P-value: {p_value:.4f}")

            if p_value / 2 < 0.05:
                ax = data[diff].plot.hist(bins=30, alpha=0.7, edgecolor="black")
                plt.axvline(data[diff].mean(), color="red", linestyle="dashed", linewidth=1.5,
                            label=f"Mean: {data[diff].mean():.2f}")
                plt.title(f"Distribution of {diff}")
                plt.xlabel(diff)
                plt.ylabel("Frequency")
                plt.legend()
                plt.show()

                if t_stat > 0:
                    print("Cars drive slower after a crash")
                else:
                    print("Cars drive faster after a crash")

