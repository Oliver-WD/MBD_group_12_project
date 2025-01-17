from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, lit, avg, col
from small_data import data

spark = SparkSession.builder.getOrCreate()

# Takes time of first crash in the small data frame
crash_time = data.iloc[0]['datetime']

# Reads the relevant partition TODO: Don't hardcode
df = spark.read.parquet("/user/s1935941/DOT_traffic_speeds_partitioned/year=2021/month=9")

# Make a df containing all entries of a specific date #TODO: Don't hardcode
date_df = df.filter(to_date(df.DATA_AS_OF) == lit("2021-09-11"))

# Make a df of all speed measurements the hour before and the hour after crash_time
hour_before_crash_time = date_df.filter(date_df.DATA_AS_OF >= crash_time - timedelta(hours=1)).filter(date_df.DATA_AS_OF <= crash_time)
hour_after_crash_time = date_df.filter(date_df.DATA_AS_OF >= crash_time).filter(date_df.DATA_AS_OF <= crash_time + timedelta(hours=1))

hour_before_crash_time.agg(avg(col("SPEED"))).show()
hour_after_crash_time.agg(avg(col("SPEED"))).show()
