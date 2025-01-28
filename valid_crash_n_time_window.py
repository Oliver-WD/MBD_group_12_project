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
        col("LINK_ID").alias("traffic_link_id"),
        "LINK_POINTS",
        col("DATA_AS_OF").alias("timestamp"),
        "SPEED"
    ).filter(
        (col("timestamp") >= "2014-03-01") &
        (col("timestamp") <= "2022-10-31")
    )

    # Process link points
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

    def create_cell_id_columns(df):
        """Create cell IDs for different region sizes in parallel"""
        result_df = df
        lat_col = "LATITUDE" if "LATITUDE" in df.columns else "lat"
        lon_col = "LONGITUDE" if "LONGITUDE" in df.columns else "lon"

        for size in [500, 1000, 2000, 5000]:
            result_df = result_df.withColumn(
                f"cell_id_{size}",
                (
                    (col(lat_col) - col("lat_min")) / (size / 111000)
                ).cast("int") * 1000 + (
                    (col(lon_col) - col("lon_min")) / (size / 85000)
                ).cast("int")
            )
        return result_df

    # Process road segments and cache
    road_segments = create_cell_id_columns(
        link_points_df.crossJoin(broadcast(bounds_df))
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

    # Process crashes with timestamps
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

    # Add cell IDs and time windows to crashes
    crash_df = create_cell_id_columns(
        crash_df_raw.crossJoin(broadcast(bounds_df))
    ).withColumn(
        "window_start",
        from_unixtime(unix_timestamp("crash_timestamp") - 15 * 60)
    ).withColumn(
        "window_end",
        from_unixtime(unix_timestamp("crash_timestamp") + 60 * 60)
    ).cache()

    region_sizes = [500, 1000, 2000, 5000]
    all_regions_metrics = []

    for size in region_sizes:
        # Get valid cell IDs for this region size
        valid_cell_ids = road_segments.select(
            col(f"cell_id_{size}").alias("cell_id")
        ).distinct()

        # Filter crashes for this region size
        size_specific_crashes = crash_df.select(
            "crash_timestamp",
            "window_start",
            "window_end",
            "LATITUDE",
            "LONGITUDE",
            col(f"cell_id_{size}").alias("cell_id")
        ).withColumn("region_size", lit(size))

        # Join with valid cell IDs
        valid_crashes = size_specific_crashes.join(
            broadcast(valid_cell_ids),
            "cell_id",
            "inner"
        )

        # Join with road segments
        size_specific_roads = road_segments.select(
            col("traffic_link_id").alias(f"link_id_{size}"),
            col(f"cell_id_{size}").alias("cell_id")
        )

        # Join crashes with road segments and traffic data
        joined = valid_crashes.join(
            size_specific_roads,
            "cell_id"
        ).join(
            traffic_df,
            (col(f"link_id_{size}") == traffic_df.traffic_link_id) &
            (col("timestamp") >= col("window_start")) &
            (col("timestamp") <= col("window_end"))
        ).withColumn(
            "time_diff_minutes",
            (unix_timestamp("timestamp") - unix_timestamp("crash_timestamp")) / 60
        )

        # Calculate metrics for this region size
        metrics = joined.groupBy(
            col(f"link_id_{size}").alias("traffic_link_id"),
            "crash_timestamp",
            "LATITUDE",
            "LONGITUDE",
            "region_size"
        ).agg(
            avg(when(col("time_diff_minutes").between(-15, 0), col("SPEED")))
            .alias("speed_15min_before"),
            avg(when(col("time_diff_minutes").between(0, 5), col("SPEED")))
            .alias("speed_5min_after"),
            avg(when(col("time_diff_minutes").between(0, 15), col("SPEED")))
            .alias("speed_15min_after"),
            avg(when(col("time_diff_minutes").between(0, 60), col("SPEED")))
            .alias("speed_1hr_after")
        )

        all_regions_metrics.append(metrics)

    # Union all region sizes
    final_metrics = all_regions_metrics[0]
    for df in all_regions_metrics[1:]:
        final_metrics = final_metrics.unionByName(df)

    # Add year column and write results
    final_metrics.withColumn(
        "year",
        year("crash_timestamp")
    ).write \
        .partitionBy("year", "region_size") \
        .mode("overwrite") \
        .parquet("/user/s1935941/valid_crash_analysis_results")

    # Clean up cached DataFrames
    road_segments.unpersist()
    crash_df.unpersist()

if __name__ == "__main__":
    analyze_crash_impacts()