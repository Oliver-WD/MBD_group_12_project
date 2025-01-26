from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    udf, col, expr, array, struct, collect_list,
    min, max, avg, when, lit, explode, split, ceil
)
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    ArrayType, IntegerType, DoubleType
)
import math

def create_grid_index(lat_min, lat_max, lon_min, lon_max, cell_size_meters=500):
    """Create a grid index covering the given area."""
    # Convert cell size from meters to degrees
    lat_cell_size = cell_size_meters / 111000  # 1 degree ≈ 111km
    lon_cell_size = cell_size_meters / (85000)  # Adjusted for NYC's latitude

    # Calculate number of cells in each dimension
    lat_cells = math.ceil((lat_max - lat_min) / lat_cell_size)
    lon_cells = math.ceil((lon_max - lon_min) / lon_cell_size)

    return {
        'lat_min': lat_min,
        'lon_min': lon_min,
        'lat_cell_size': lat_cell_size,
        'lon_cell_size': lon_cell_size,
        'lat_cells': lat_cells,
        'lon_cells': lon_cells
    }

def get_cell_id(lat, lon, grid_params):
    """Convert coordinates to grid cell ID."""
    if lat is None or lon is None:
        return None

    lat_idx = int((lat - grid_params['lat_min']) / grid_params['lat_cell_size'])
    lon_idx = int((lon - grid_params['lon_min']) / grid_params['lon_cell_size'])

    if (lat_idx < 0 or lat_idx >= grid_params['lat_cells'] or
            lon_idx < 0 or lon_idx >= grid_params['lon_cells']):
        return None

    return lat_idx * grid_params['lon_cells'] + lon_idx

def analyze_crash_impacts():
    # Initialize Spark with settings needed for timezone issues
    spark = SparkSession.builder \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Read and preprocess traffic data
    traffic_df = spark.read.parquet("/user/s1935941/DOT_traffic_speeds_partitioned")

    # Extract coordinates from LINK_POINTS and find bounding box
    bounds_df = traffic_df.select(
        explode(split("LINK_POINTS", " ")).alias("coord")
    ).select(
        split("coord", ",")[0].cast(FloatType()).alias("lat"),
        split("coord", ",")[1].cast(FloatType()).alias("lon")
    ).agg(
        min("lat").alias("lat_min"),
        max("lat").alias("lat_max"),
        min("lon").alias("lon_min"),
        max("lon").alias("lon_max")
    )

    # Register UDF for cell_id calculation
    spark.udf.register("get_cell_id",
                      lambda lat, lon, lat_min, lon_min, lat_cell_size, lon_cell_size, lat_cells, lon_cells:
                      get_cell_id(lat, lon, {
                          'lat_min': lat_min,
                          'lon_min': lon_min,
                          'lat_cell_size': lat_cell_size,
                          'lon_cell_size': lon_cell_size,
                          'lat_cells': lat_cells,
                          'lon_cells': lon_cells
                      }), IntegerType())

    # Create grid parameters using Spark SQL functions
    grid_params_df = bounds_df.select(
        col("lat_min"),
        col("lon_min"),
        (lit(500.0) / lit(111000.0)).alias("lat_cell_size"),
        (lit(500.0) / lit(85000.0)).alias("lon_cell_size"),
        ceil((col("lat_max") - col("lat_min")) / (lit(500.0) / lit(111000.0))).alias("lat_cells"),
        ceil((col("lon_max") - col("lon_min")) / (lit(500.0) / lit(85000.0))).alias("lon_cells")
    )

    # Process road segments using the grid parameters from DataFrame
    road_segments = traffic_df.select(
        "LINK_ID",
        "LINK_POINTS"
    ).distinct().select(
        "LINK_ID",
        explode(split("LINK_POINTS", " ")).alias("coord")
    ).select(
        "LINK_ID",
        split("coord", ",")[0].cast(FloatType()).alias("lat"),
        split("coord", ",")[1].cast(FloatType()).alias("lon")
    ).crossJoin(grid_params_df).withColumn(
        "cell_id",
        expr("""get_cell_id(
            lat, lon, 
            lat_min, lon_min, 
            lat_cell_size, lon_cell_size, 
            lat_cells, lon_cells
        )""")
    ).drop(
        "lat_min", "lon_min", "lat_cell_size",
        "lon_cell_size", "lat_cells", "lon_cells"
    )

    # Cache the road segments DataFrame
    road_segments.persist()

    # Process crashes (schema definition and processing remains the same)
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
    ).withColumn(
        "crash_timestamp",
        expr("to_timestamp(concat(`CRASH DATE`, ' ', `CRASH TIME`), 'MM/dd/yyyy HH:mm')")
    ).filter(
        (col("LATITUDE").isNotNull()) &
        (col("LONGITUDE").isNotNull())
    ).crossJoin(grid_params_df).withColumn(
        "cell_id",
        expr("""get_cell_id(
            CAST(LATITUDE AS FLOAT), 
            CAST(LONGITUDE AS FLOAT),
            lat_min, lon_min,
            lat_cell_size, lon_cell_size,
            lat_cells, lon_cells
        )""")
    ).drop(
        "lat_min", "lon_min", "lat_cell_size",
        "lon_cell_size", "lat_cells", "lon_cells"
    )

    # Join crashes with road segments and calculate metrics
    crash_road_matches = crash_df.join(
        road_segments,
        "cell_id"
    ).withColumn(
        "distance",
        expr("""
            power(
                power(CAST(LATITUDE AS DOUBLE) - CAST(lat AS DOUBLE), 2) +
                power(CAST(LONGITUDE AS DOUBLE) - CAST(lon AS DOUBLE), 2),
                0.5
            )
        """)
    ).withColumn(
        "rank",
        expr("row_number() over (partition by crash_timestamp order by distance)")
    ).filter("rank = 1").select(
        "crash_timestamp",
        "LATITUDE",
        "LONGITUDE",
        "LINK_ID"
    )

    # Calculate final metrics
    result = crash_road_matches.join(
        traffic_df,
        "LINK_ID"
    ).select(
        "LINK_ID",
        "crash_timestamp",
        "LATITUDE",
        "LONGITUDE",
        "DATA_AS_OF",
        "SPEED"
    ).withColumn(
        "time_diff_minutes",
        expr("(unix_timestamp(DATA_AS_OF) - unix_timestamp(crash_timestamp)) / 60")
    ).groupBy(
        "LINK_ID",
        "crash_timestamp",
        "LATITUDE",
        "LONGITUDE"
    ).agg(
        avg(when(
            col("time_diff_minutes").between(-15, 0),
            col("SPEED")
        )).alias("speed_15min_before"),
        avg(when(
            col("time_diff_minutes").between(0, 5),
            col("SPEED")
        )).alias("speed_5min_after"),
        avg(when(
            col("time_diff_minutes").between(0, 15),
            col("SPEED")
        )).alias("speed_15min_after"),
        avg(when(
            col("time_diff_minutes").between(0, 60),
            col("SPEED")
        )).alias("speed_1hr_after")
    )

    # Unpersist cached DataFrame
    road_segments.unpersist()

    # Write results
    result.write \
        .partitionBy("LINK_ID") \
        .mode("overwrite") \
            .parquet("/user/s1935941/crash_analysis_results")

if __name__ == "__main__":
    analyze_crash_impacts()