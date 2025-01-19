from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, when, lit
from pyspark.sql.types import BooleanType


def euclidean_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat_meters = (lat2 - lat1) * 111000
    lon_meters = (lon2 - lon1) * 85000
    return (lat_meters ** 2 + lon_meters ** 2) ** 0.5


def is_polyline_within_range(polyline: str, crash_lat: float, crash_lon: float, range_meters: int) -> bool:
    try:
        coords = polyline.split()
        if not coords:
            return False

        for coord in coords:
            lat, lon = map(float, coord.split(','))
            if euclidean_distance(crash_lat, crash_lon, lat, lon) <= range_meters:
                return True

        return False
    except (IndexError, ValueError):
        return False


def get_traffic_data_near_crash(
        spark: SparkSession,
        df: DataFrame,
        crash_location: tuple,
        crash_time: datetime,
        range_meters: int
) -> DataFrame:
    is_within_range_udf = udf(
        lambda polyline: is_polyline_within_range(
            polyline,
            crash_location[0],
            crash_location[1],
            range_meters
        ),
        BooleanType()
    )

    time_window_start = crash_time - timedelta(hours=1)
    time_window_end = crash_time + timedelta(hours=1)

    filtered_df = df.filter(
        (col("DATA_AS_OF").between(time_window_start, time_window_end)) &
        is_within_range_udf(col("LINK_POINTS"))
    )

    result_df = filtered_df.withColumn(
        "period",
        when(col("DATA_AS_OF") <= crash_time, "before")
        .otherwise("after")
    )

    return result_df