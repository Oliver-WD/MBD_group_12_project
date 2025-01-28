from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, min, max, avg, when, lit, explode, split, broadcast,
    to_timestamp, unix_timestamp, year, concat, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def analyze_crash_impacts():
    spark = SparkSession.builder \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Read and preprocess traffic data
    traffic_df = spark.read.parquet("/user/s1935941/DOT_traffic_speeds_partitioned").select(
        col("LINK_ID").alias("link_id"),
        "LINK_POINTS",
        col("DATA_AS_OF").alias("timestamp"),
        "SPEED"
    ).filter(
        (col("timestamp") >= "2014-03-01") &
        (col("timestamp") <= "2022-10-31")
    )

    # Process link points for cell calculation
    link_points_df = traffic_df.select(
        "link_id",
        "LINK_POINTS"
    ).distinct().select(
        "link_id",  
        explode(split("LINK_POINTS", " ")).alias("coord")
    ).select(
        "link_id",  
        split("coord", ",")[0].cast(FloatType()).alias("lat"),
        split("coord", ",")[1].cast(FloatType()).alias("lon")
    )

    # Calculate bounds
    bounds_df = link_points_df.select(
        min("lat").alias("lat_min"),
        max("lat").alias("lat_max"),
        min("lon").alias("lon_min"),
        max("lon").alias("lon_max")
    )

    # Create cell IDs for road segments
    road_segments = link_points_df.crossJoin(broadcast(bounds_df)) \
        .withColumn(
            "cell_id",
            ((col("lat") - col("lat_min")) / (500 / 111000)).cast("int") * 1000 +
            ((col("lon") - col("lon_min")) / (500 / 85000)).cast("int")
        ).cache()

    # Process crashes
    crash_schema = StructType([
        StructField("CRASH DATE", StringType(), True),
        StructField("CRASH TIME", StringType(), True),
        StructField("BOROUGH", StringType(), True),
        StructField("ZIP CODE", StringType(), True),
        StructField("LATITUDE", StringType(), True),
        StructField("LONGITUDE", StringType(), True),
        StructField("LOCATION", StringType(), True),
        StructField("ON STREET NAME", StringType(), True),
        StructField("CROSS STREET NAME", StringType(), True),
        StructField("OFF STREET NAME", StringType(), True),
        StructField("NUMBER OF PERSONS INJURED", StringType(), True),
        StructField("NUMBER OF PERSONS KILLED", StringType(), True),
        StructField("NUMBER OF PEDESTRIANS INJURED", StringType(), True),
        StructField("NUMBER OF PEDESTRIANS KILLED", StringType(), True),
        StructField("NUMBER OF CYCLIST INJURED", StringType(), True),
        StructField("NUMBER OF CYCLIST KILLED", StringType(), True),
        StructField("NUMBER OF MOTORIST INJURED", StringType(), True),
        StructField("NUMBER OF MOTORIST KILLED", StringType(), True),
        StructField("CONTRIBUTING FACTOR VEHICLE 1", StringType(), True),
        StructField("CONTRIBUTING FACTOR VEHICLE 2", StringType(), True),
        StructField("CONTRIBUTING FACTOR VEHICLE 3", StringType(), True),
        StructField("CONTRIBUTING FACTOR VEHICLE 4", StringType(), True),
        StructField("CONTRIBUTING FACTOR VEHICLE 5", StringType(), True),
        StructField("COLLISION_ID", StringType(), True),
        StructField("VEHICLE TYPE CODE 1", StringType(), True),
        StructField("VEHICLE TYPE CODE 2", StringType(), True),
        StructField("VEHICLE TYPE CODE 3", StringType(), True),
        StructField("VEHICLE TYPE CODE 4", StringType(), True),
        StructField("VEHICLE TYPE CODE 5", StringType(), True)
    ])

    crash_df = spark.read.csv(
        "/user/s1935941/Motor_Vehicle_Collisions.gz",
        header=True,
        schema=crash_schema
    )

    # Process crashes with timestamps and coordinates
    crash_df_processed = crash_df.select(
        to_timestamp(
            concat(col("CRASH DATE"), lit(" "), col("CRASH TIME")),
            'MM/dd/yyyy HH:mm'
        ).alias("crash_timestamp"),
        col("LATITUDE").cast(FloatType()),
        col("LONGITUDE").cast(FloatType())
    ).filter(
        (col("LATITUDE").isNotNull()) &
        (col("LONGITUDE").isNotNull()) &
        (col("crash_timestamp") >= "2014-03-01") &
        (col("crash_timestamp") <= "2022-10-31")
    )

    # Add cell IDs and time windows to crashes
    crash_df_with_cells = crash_df_processed.crossJoin(broadcast(bounds_df)) \
        .withColumn(
            "cell_id",
            ((col("LATITUDE") - col("lat_min")) / (500 / 111000)).cast("int") * 1000 +
            ((col("LONGITUDE") - col("lon_min")) / (500 / 85000)).cast("int")
        ).withColumn(
            "window_start",
            from_unixtime(unix_timestamp("crash_timestamp") - 15 * 60)
        ).withColumn(
            "window_end",
            from_unixtime(unix_timestamp("crash_timestamp") + 60 * 60)
        ).cache()

    # Get valid cell IDs
    valid_cell_ids = road_segments.select("cell_id").distinct()

    # Filter crashes to valid cells
    valid_crashes = crash_df_with_cells.join(
        broadcast(valid_cell_ids),
        "cell_id",
        "inner"
    )

    # Join with road segments to get LINK_IDs
    crashes_with_links = valid_crashes.join(
        road_segments.select("link_id", "cell_id"),
        "cell_id"
    )

    # For each crash, get relevant traffic data within its time window
    traffic_metrics = crashes_with_links.join(
        traffic_df,
        (crashes_with_links.link_id == traffic_df.link_id) &
        (col("timestamp") >= crashes_with_links.window_start) &
        (col("timestamp") <= crashes_with_links.window_end)
    ).withColumn(
        "time_diff_minutes",
        (unix_timestamp("timestamp") - unix_timestamp("crash_timestamp")) / 60
    )

    # Calculate final metrics
    metrics = traffic_metrics.groupBy(
        crashes_with_links.link_id,
        "crash_timestamp",
        "LATITUDE",
        "LONGITUDE"
    ).agg(
        avg(when(col("time_diff_minutes").between(-15, 0), col("SPEED")))
        .alias("speed_15min_before"),
        avg(when(col("time_diff_minutes").between(0, 5), col("SPEED")))
        .alias("speed_5min_after"),
        avg(when(col("time_diff_minutes").between(0, 15), col("SPEED")))
        .alias("speed_15min_after"),
        avg(when(col("time_diff_minutes").between(0, 60), col("SPEED")))
        .alias("speed_1hr_after")
    ).withColumn(
        "year",
        year("crash_timestamp")
    )

    # Write results
    metrics.write \
        .partitionBy("year") \
        .mode("overwrite") \
        .parquet("/user/s1935941/crash_analysis_500m")

    # Clean up
    road_segments.unpersist()
    crash_df_with_cells.unpersist()

if __name__ == "__main__":
    analyze_crash_impacts()