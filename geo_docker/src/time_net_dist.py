import os
import argparse
import logging
import traceback
import time

import matplotlib.pyplot as plt
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer

from haversine import haversine

logger = logging.getLogger('spark')
logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s")


def get_haversine_dist(lat_x: float, long_x: float, lat_y: float, long_y: float) -> float:
    '''
    Get distanse in KM
    '''
    if not (lat_x is None) and not (long_x is None) and not (lat_y is None) and not (long_y is None):
        return haversine((lat_x, long_x), (lat_y, long_y))
    else:
        return None

def get_time_diff(time1, time2):

     if not (time1 is None) and not (time2 is None):
        return (time2 - time1).total_seconds() / 60 / 60
     else:
        return None   


def analyse_time_gross_dist(database_folder: str, spark_filename: str = "geo_table.parquet") -> bool:

    if not os.path.isdir(database_folder):
        logger.error("Database folder doesn't exit")
        return False

    try:  

        time_salt = int(time.time())

        spark_context = SparkContext.getOrCreate(SparkConf().setMaster("local[*]").setAppName("geoAppLengthDistAnalysis"))
        spark = SparkSession(spark_context)
        spark.sparkContext.setLogLevel("ERROR")
        df=spark.read.parquet(os.path.join(database_folder, spark_filename))

        df.createOrReplaceTempView("geo_table")
        df_sql = spark.sql("select UserId, TrajectoryId, latitude, Longitude, StepTimestamp from geo_table")

        udf_get_haversine_dist = F.udf(get_haversine_dist, DoubleType())

        column_list = ["UserId", "TrajectoryId"]
        windowSpec = Window.partitionBy([F.col(x) for x in column_list]).orderBy("StepTimestamp")
        #df_sql = df_sql.withColumn("StepTimestampDiff", F.col("StepTimestamp") - F.lag("StepTimestamp", 1).over(windowSpec))\
        #             .fillna({'StepTimestampDiff':0.0})
        df_sql = df_sql.withColumn("Distance", udf_get_haversine_dist("latitude", "Longitude", F.lag("latitude", 1).over(windowSpec), F.lag("Longitude", 1).over(windowSpec)))\
                     .fillna({'Distance':0.0})


        

        udf_get_time_diff = F.udf(get_time_diff, DoubleType())
        df_sql = df_sql.withColumn("StepTimestampDiff", udf_get_time_diff( F.lag("StepTimestamp", 1).over(windowSpec), F.col("StepTimestamp")))
        #df_sql = df_sql.withColumn("StepTimestampLag", F.lag("StepTimestamp", 1).over(windowSpec))
                     #.fillna({'StepTimestampDiff':0.0})

        df_sql.show()

        # define human step
        df.filter((F.col("Regionname") == "Northern Metropolitan"))


        return True

    except Exception as e:
        logger.error(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        return False



if __name__ == '__main__':

    app_parser = argparse.ArgumentParser(description='geoAppLengthDistAnalysis')

    app_parser.add_argument('database_folder',
                            nargs='?',
                            type=str,
                            default="../../geo_data_output",
                            help='local path to outputs storage')

    args = app_parser.parse_args()

    database_folder = args.database_folder

    analyse_time_gross_dist(database_folder)
