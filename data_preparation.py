from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month

spark = SparkSession.builder.getOrCreate()

df = spark.read.option("header", True).csv("/user/s1935941/DOT_traffic_speeds.gz")
# df.count() # 64914523

# Drop the irrelevant columns to us
columns_to_drop = ["STATUS", "OWNER", "TRANSCOM_ID", "BOROUGH", "LINK_NAME"]
df = df.drop(*columns_to_drop)

# transform the DATA_AS_OF column from string to datetime object
datetime_df = df.withColumn("DATA_AS_OF", to_timestamp("DATA_AS_OF", "MM/dd/yyyy hh:mm:ss a"))

partitioned_df = datetime_df.withColumn("year", year("DATA_AS_OF")).withColumn("month", month("DATA_AS_OF"))
partitioned_df.write.partitionBy("year", "month").mode("overwrite").parquet("/user/s1935941"
                                                                            "/DOT_traffic_speeds_partitioned.")
