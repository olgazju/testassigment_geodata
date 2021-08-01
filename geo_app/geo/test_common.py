import pytest
from datetime import datetime

from pyspark.sql import SparkSession

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

from geo.common import calculate_distance

@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("insest_test") \
      .getOrCreate()


@pytest.fixture(scope='session')
def trajectories_with_labels(spark):

    trajectories_with_labels_test = [
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9755666666667, 116.33035, 226.377952755906, datetime.strptime("2007-04-29 08:34:32", '%Y-%m-%d %H:%M:%S'), 0),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.97545, 116.330233333333, 301.837270341207, datetime.strptime("2007-04-29 08:37:00", '%Y-%m-%d %H:%M:%S'), 0),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9738333333333, 116.332966666667, 328.083989501312, datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), 1),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9739, 116.332733333333, 328.083989501312, datetime.strptime("2007-04-29 12:34:32", '%Y-%m-%d %H:%M:%S'), 1),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2079666666667, 116.9131, 131.233595800525, datetime.strptime("2007-04-29 12:37:20", '%Y-%m-%d %H:%M:%S'), 1),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2088333333333, 116.9175, 131.233595800525, datetime.strptime("2007-04-29 12:37:25", '%Y-%m-%d %H:%M:%S'), 1),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2087333333333, 116.917683333333, 131.233595800525, datetime.strptime("2007-04-29 23:37:11", '%Y-%m-%d %H:%M:%S'), 2),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2055666666667, 116.91735, 141.076115485564, datetime.strptime("2007-04-29 23:37:16", '%Y-%m-%d %H:%M:%S'), 2),
         ]

    trajectories_with_labels_schema = StructType([StructField("IngestionTime", TimestampType(), False),
                        StructField("UserId", StringType(), False),
                        StructField("TrajectoryID", StringType(), False),
                        StructField("Latitude", DoubleType(), True),
                        StructField("Longitude", DoubleType(), True),
                        StructField("Altitude", DoubleType(), True),
                        StructField("StepTimestamp", TimestampType(), True),
                        StructField("Label", IntegerType(), False)])

    return spark.createDataFrame(trajectories_with_labels_test, trajectories_with_labels_schema)


@pytest.fixture(scope='session')
def trajectories_with_distance(spark):

    trajectories_with_distance_test = [
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9755666666667, 116.33035, 226.377952755906, datetime.strptime("2007-04-29 08:34:32", '%Y-%m-%d %H:%M:%S'), 0, None, None, 0.0 ),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.97545, 116.330233333333, 301.837270341207, datetime.strptime("2007-04-29 08:37:00", '%Y-%m-%d %H:%M:%S'), 0, 116.33035, 39.9755666666667, 0.016343826744769148),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9738333333333, 116.332966666667, 328.083989501312, datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), 1, 116.330233333333, 39.97545, 0.29421713655076837),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9739, 116.332733333333, 328.083989501312, datetime.strptime("2007-04-29 12:34:32", '%Y-%m-%d %H:%M:%S'), 1, 116.332966666667, 39.9738333333333, 0.021219945729212226),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2079666666667, 116.9131, 131.233595800525, datetime.strptime("2007-04-29 12:37:20", '%Y-%m-%d %H:%M:%S'), 1, None, None, 0.0),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2088333333333, 116.9175, 131.233595800525, datetime.strptime("2007-04-29 12:37:25", '%Y-%m-%d %H:%M:%S'), 1, 116.9131, 28.2079666666667, 0.4417893098085645),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2087333333333, 116.917683333333, 131.233595800525, datetime.strptime("2007-04-29 23:37:11", '%Y-%m-%d %H:%M:%S'), 2, 116.9175, 28.2088333333333, 0.02112742108758325),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2055666666667, 116.91735, 141.076115485564, datetime.strptime("2007-04-29 23:37:16", '%Y-%m-%d %H:%M:%S'), 2, 116.917683333333, 28.2087333333333, 0.35362898876982113),
         ]

    trajectories_with_distance_schema = StructType([StructField("IngestionTime", TimestampType(), False),
                        StructField("UserId", StringType(), False),
                        StructField("TrajectoryID", StringType(), False),
                        StructField("Latitude", DoubleType(), True),
                        StructField("Longitude", DoubleType(), True),
                        StructField("Altitude", DoubleType(), True),
                        StructField("StepTimestamp", TimestampType(), True),
                        StructField("Label", IntegerType(), False),
                        StructField("PrevLongitude", DoubleType(), True),
                        StructField("PrevLatitude", DoubleType(), True),
                        StructField("Distance", DoubleType(), True)])

    return spark.createDataFrame(trajectories_with_distance_test, trajectories_with_distance_schema)



def test_calculate_distance(spark, trajectories_with_labels, trajectories_with_distance):

	df_sql = trajectories_with_labels

	df_sql = calculate_distance(trajectories_with_labels, ["UserId", "TrajectoryId"], "StepTimestamp")

	assert_df_equality(df_sql, trajectories_with_distance, ignore_nullable=True)
