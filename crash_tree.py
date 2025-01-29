from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, min, max, avg, when, lit, explode, split, broadcast,
    to_timestamp, unix_timestamp, year, concat, from_unixtime, floor
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def create_base_cell_ids(df, bounds_df, is_crash=False):
    """
    Create cell IDs at the base 500m scale
    """
    lat_col = "LATITUDE" if is_crash else "lat"
    lon_col = "LONGITUDE" if is_crash else "lon"

    LAT_METER_PER_DEGREE = 111000  # at NYC's latitude
    LON_METER_PER_DEGREE = 85000   # at NYC's latitude
    BASE_SIZE = 500  # meters

    return df.crossJoin(broadcast(bounds_df)).withColumn(
        "cell_id_500",
        (
            (col(lat_col) - col("lat_min")) / (BASE_SIZE / LAT_METER_PER_DEGREE)
        ).cast("int") * 1000 + (
            (col(lon_col) - col("lon_min")) / (BASE_SIZE / LON_METER_PER_DEGREE)
        ).cast("int")
    )

def analyze_crash_impacts():
    spark = SparkSession.builder \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Read and preprocess traffic data
    traffic_df = spark.read.parquet("/user/s1935941/DOT_traffic_speeds_partitioned") \
        .select(
            col("LINK_ID").alias("traffic_link_id"),
            "LINK_POINTS",
            col("DATA_AS_OF").alias("timestamp"),
            "SPEED"
        ).filter(
            (col("timestamp") >= "2014-03-01") &
            (col("timestamp") <= "2022-10-31")
        ).repartition(115, "traffic_link_id", "timestamp") # Estimation from sampling we did in pysparkshell for 128MB per partition

    # Process link points for base cell calculation
    link_points_df = traffic_df.select(
        "traffic_link_id",
        "LINK_POINTS"
    ).distinct().select(
        "traffic_link_id",
        explode(split("LINK_POINTS", " ")).alias("coord")
    ).select(
        "traffic_link_id",
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

    # Create base cell IDs (500m) for road segments
    road_segments = create_base_cell_ids(
        link_points_df,
        bounds_df,
        is_crash=False
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

    crash_df_raw = spark.read.csv(
        "/user/s1935941/Motor_Vehicle_Collisions.gz",
        header=True,
        schema=crash_schema
    ).select(
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

    # Create base cell IDs for crashes and add time windows
    crash_df = create_base_cell_ids(
        crash_df_raw,
        bounds_df,
        is_crash=True
    ).withColumn(
        "window_start",
        from_unixtime(unix_timestamp("crash_timestamp") - 15 * 60)
    ).withColumn(
        "window_end",
        from_unixtime(unix_timestamp("crash_timestamp") + 60 * 60)
    ).cache()

    # Join crashes with road segments and traffic data at 500m level
    base_metrics = crash_df.join(
        road_segments,
        crash_df.cell_id_500 == road_segments.cell_id_500,
        "inner"
    ).join(
        traffic_df,
        (traffic_df.traffic_link_id == road_segments.traffic_link_id) &
        (col("timestamp") >= col("window_start")) &
        (col("timestamp") <= col("window_end"))
    ).select(
        crash_df["crash_timestamp"],
        crash_df["LATITUDE"],
        crash_df["LONGITUDE"],
        road_segments.traffic_link_id,
        crash_df["cell_id_500"],
        ((unix_timestamp("timestamp") - unix_timestamp("crash_timestamp")) / 60)
        .alias("time_diff_minutes"),
        col("SPEED").alias("traffic_speed")
    )

    base_metrics = base_metrics.repartition(160, "traffic_link_id", "cell_id_500")

    # Calculate metrics at 500m level
    metrics_500m = base_metrics.groupBy(
        "traffic_link_id",
        "crash_timestamp",
        "LATITUDE",
        "LONGITUDE",
        "cell_id_500"
    ).agg(
        avg(when(col("time_diff_minutes").between(-15, 0), col("traffic_speed")))
        .alias("speed_15min_before"),
        avg(when(col("time_diff_minutes").between(0, 5), col("traffic_speed")))
        .alias("speed_5min_after"),
        avg(when(col("time_diff_minutes").between(0, 15), col("traffic_speed")))
        .alias("speed_15min_after"),
        avg(when(col("time_diff_minutes").between(0, 60), col("traffic_speed")))
        .alias("speed_1hr_after")
    ).withColumn("region_size", lit(500))

    # Define common columns for metrics
    metric_columns = [
        "crash_timestamp",
        "LATITUDE",
        "LONGITUDE",
        "speed_15min_before",
        "speed_5min_after",
        "speed_15min_after",
        "speed_1hr_after",
        "region_size"
    ]

    # Create 1km metrics
    metrics_1km = metrics_500m.withColumn(
        "cell_id_1km",
        floor(col("cell_id_500") / 2)
    ).groupBy(
        "cell_id_1km",
        "crash_timestamp",
        "LATITUDE",
        "LONGITUDE"
    ).agg(
        avg("speed_15min_before").alias("speed_15min_before"),
        avg("speed_5min_after").alias("speed_5min_after"),
        avg("speed_15min_after").alias("speed_15min_after"),
        avg("speed_1hr_after").alias("speed_1hr_after")
    ).withColumn("region_size", lit(1000))

    # Create 2km metrics
    metrics_5km = metrics_1km.withColumn(
        "cell_id_5km",
        floor(col("cell_id_1km") / 5)
    ).groupBy(
        "cell_id_5km",
        "crash_timestamp",
        "LATITUDE",
        "LONGITUDE"
    ).agg(
        avg("speed_15min_before").alias("speed_15min_before"),
        avg("speed_5min_after").alias("speed_5min_after"),
        avg("speed_15min_after").alias("speed_15min_after"),
        avg("speed_1hr_after").alias("speed_1hr_after")
    ).withColumn("region_size", lit(5000))

    # Union all metrics using common columns
    all_metrics = metrics_500m.select(*metric_columns).unionByName(
        metrics_1km.select(*metric_columns)
    ).unionByName(
        metrics_5km.select(*metric_columns)
    ).withColumn(
        "year",
        year("crash_timestamp")
    )

    # Write results
    all_metrics.write \
        .partitionBy("year", "region_size") \
        .mode("overwrite") \
        .parquet("/user/s1935941/tree_crash_analysis_large")

    # Clean up
    road_segments.unpersist()
    crash_df.unpersist()

if __name__ == "__main__":
    analyze_crash_impacts()