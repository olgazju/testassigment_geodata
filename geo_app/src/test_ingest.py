from ingest import read_labels, read_user_trajectories, LABELS_EXCEPTION
from datetime import datetime

import os

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType


import pytest
from pyspark.sql import SparkSession
@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("insest_test") \
      .getOrCreate()


def test_read_labels(spark):

    labels_test = [
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), datetime.strptime("2007-04-29 12:53:45", '%Y-%m-%d %H:%M:%S'), 1),
        (datetime.strptime("2007-04-29 22:27:11", '%Y-%m-%d %H:%M:%S'), datetime.strptime("2007-04-30 04:28:00", '%Y-%m-%d %H:%M:%S'), 2)
    ]

    labels_test_schema = StructType([StructField("StartTime", TimestampType(), True),
                    StructField("EndTime", TimestampType(), True),
                    StructField("Label", IntegerType(), True)])

    labels_test_df = spark.createDataFrame(labels_test, labels_test_schema)

    labels_df = read_labels(spark, os.path.join("mock_data", "labels.txt"))


    assert_df_equality(labels_test_df, labels_df)


def test_no_labels(spark):

    labels_df = read_labels(spark, os.path.join("mock_data", "nolabels.txt"))
    print(labels_df)
    assert labels_df is None


def test_wrong_labels(spark):
    
    with pytest.raises(ValueError) as excinfo:

        labels_df = read_labels(spark, os.path.join("mock_data", "labels_wrong.txt"))

    assert LABELS_EXCEPTION in str(excinfo.value)



def test_read_trajectories(spark):

    current_run_timestamp = datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S')
    trajectories_df = read_user_trajectories(spark, os.path.join("mock_data", "Trajectory", "*.plt"), "000", current_run_timestamp)

    trajectories_test = [
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9755666666667, 116.33035, 226.377952755906, datetime.strptime("2007-04-29 08:34:32", '%Y-%m-%d %H:%M:%S')),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.97545, 116.330233333333, 301.837270341207, datetime.strptime("2007-04-29 08:37:00", '%Y-%m-%d %H:%M:%S')),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9738333333333, 116.332966666667, 328.083989501312, datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S')),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070429083432", 39.9739, 116.332733333333, 328.083989501312, datetime.strptime("2007-04-29 12:34:32", '%Y-%m-%d %H:%M:%S')),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2079666666667, 116.9131, 131.233595800525, datetime.strptime("2007-04-29 12:37:20", '%Y-%m-%d %H:%M:%S')),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2088333333333, 116.9175, 131.233595800525, datetime.strptime("2007-04-29 12:37:25", '%Y-%m-%d %H:%M:%S')),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2087333333333, 116.917683333333, 131.233595800525, datetime.strptime("2007-04-29 23:37:11", '%Y-%m-%d %H:%M:%S')),
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), "000", "20070502001318", 28.2055666666667, 116.91735, 141.076115485564, datetime.strptime("2007-04-29 23:37:16", '%Y-%m-%d %H:%M:%S')),
         ]

    trajectory_test_schema = StructType([StructField("IngestionTime", TimestampType(), False),
                        StructField("UserId", StringType(), False),
                        StructField("TrajectoryID", StringType(), False),
                        StructField("Latitude", DoubleType(), True),
                        StructField("Longitude", DoubleType(), True),
                        StructField("Altitude", DoubleType(), True),
                        StructField("StepTimestamp", TimestampType(), True)])

    trajectories_test_df = spark.createDataFrame(trajectories_test, trajectory_test_schema)

    assert_df_equality(trajectories_df, trajectories_test_df)
