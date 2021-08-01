from datetime import datetime
import os
import time

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

from geo import ingest


import pytest
from pyspark.sql import SparkSession
@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
      .master("local") \
      .appName("insest_test") \
      .getOrCreate()


@pytest.fixture(scope='session')
def labels(spark):
    labels_test = [
        (datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S'), datetime.strptime("2007-04-29 12:53:45", '%Y-%m-%d %H:%M:%S'), 1),
        (datetime.strptime("2007-04-29 22:27:11", '%Y-%m-%d %H:%M:%S'), datetime.strptime("2007-04-30 04:28:00", '%Y-%m-%d %H:%M:%S'), 2)
    ]

    labels_test_schema = StructType([StructField("StartTime", TimestampType(), True),
                    StructField("EndTime", TimestampType(), True),
                    StructField("Label", IntegerType(), True)])

    return spark.createDataFrame(labels_test, labels_test_schema)



@pytest.fixture(scope='session')
def trajectories(spark):

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

    return spark.createDataFrame(trajectories_test, trajectory_test_schema)

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



def test_read_labels(spark, labels):


    labels_df = ingest.read_labels(spark, os.path.join("geo", "mock_data", "Data", "000", "labels.txt"))
    assert_df_equality(labels, labels_df)

def test_no_labels(spark):

    labels_df = ingest.read_labels(spark, os.path.join("geo", "mock_data", "Data", "000", "nolabels.txt"))
    print(labels_df)
    assert labels_df is None


def test_wrong_labels(spark):
    
    with pytest.raises(ValueError) as excinfo:

        labels_df = ingest.read_labels(spark, os.path.join("geo", "mock_data", "Data", "000", "labels_wrong.txt"))

    assert ingest.LABELS_EXCEPTION in str(excinfo.value)

def test_read_trajectories(spark, trajectories):

    current_run_timestamp = datetime.strptime("2007-04-29 12:34:24", '%Y-%m-%d %H:%M:%S')
    trajectories_df = ingest.read_user_trajectories(spark, os.path.join("geo", "mock_data", "Data", "000", "Trajectory", "*.plt"), "000", current_run_timestamp)

    assert_df_equality(trajectories_df, trajectories)


def test_merge_labels(spark, labels, trajectories, trajectories_with_labels):

    merged_df = ingest.merge_labels(labels, trajectories)
    assert_df_equality(merged_df, trajectories_with_labels)


def test_ingest_no_folder(spark):

    assert ingest.ingest("fake_folder", "mock_data_output") is False

    
def test_ingest(spark, trajectories_with_labels):

    PARQUET_NAME = "geo_table.parquet_" + str(time.time())
    result = ingest.ingest(os.path.join("geo", "mock_data"), os.path.join("geo","mock_data_output"), PARQUET_NAME)

    assert result is True
  
    #df = spark.read.parquet(os.path.join("mock_data_output", PARQUET_NAME))

    #assert_df_equality(df, trajectories_with_labels)
