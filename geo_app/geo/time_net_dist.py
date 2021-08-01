import os
import argparse
import logging
import traceback
import time

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer

from pyspark.sql.utils import AnalysisException

try:
    from geo.common import save_pie_plot, bucketize, calculate_distance
except ImportError:
    from common import save_pie_plot, bucketize, calculate_distance

logger = logging.getLogger('spark')
logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s")


def get_time_diff(time1, time2):

     if not (time1 is None) and not (time2 is None):
        return (time2 - time1).total_seconds() / 60 / 60
     else:
        return None   


def analyse_time_net_dist(database_folder: str, spark_filename: str = "geo_table.parquet") -> bool:

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
        df_sql = spark.sql("select UserId, TrajectoryId, Latitude, Longitude, StepTimestamp from geo_table")

        df_sql = calculate_distance(df_sql, ["UserId", "TrajectoryId"], "StepTimestamp")

        df_sql = df_sql.drop("Latitude", "Longitude", "PrevLongitude", "PrevLatitude")

        # get time of each step in hours

        column_list = ["UserId", "TrajectoryId"]
        windowSpec = Window.partitionBy([F.col(x) for x in column_list]).orderBy("StepTimestamp")
        udf_get_time_diff = F.udf(get_time_diff, DoubleType())
        df_sql = df_sql.withColumn("StepTimestampDiff", udf_get_time_diff( F.lag("StepTimestamp", 1).over(windowSpec), F.col("StepTimestamp")))

        #get speed on each step in km/h
        df_sql = df_sql.withColumn("Speed", (F.col("StepTimestampDiff") / F.col("Distance")))\
                        .fillna({'Speed':0.0})


        df_sql = bucketize(df_sql, 
                            [-float('Inf'), 0.05, float('Inf')], 
                            ['stop', 'not-stop'], 
                            "Speed", 
                            "SpeedCat").fillna({'StepTimestampDiff':0.0})

        df_sql = df_sql.drop("Speed", "Distance", "StepTimestamp")

        # choose only non stop steps
        df_diff = df_sql.filter(df_sql.SpeedCat == 'not-stop')

        df_diff = df_diff.groupby("UserId", "TrajectoryId").sum("StepTimestampDiff").withColumnRenamed("sum(StepTimestampDiff)", "StepTimestampDiff")

        # add buckets as TimeCat column according to TimeDiffHours
        df_diff = bucketize(df_diff, 
                            [-float('Inf'), 1, 6, 12, float('Inf')], 
                            ['<1', '1-6', '6-12', '>=12'], 
                            "StepTimestampDiff", 
                            "TimeCat")

        # count gross time distribution per bucket for all users
        df_diff = df_diff.groupby("TimeCat").count().withColumnRenamed("count", "TimeNetDistribution")
        
        # count distribution in percentage  
        df_diff = df_diff.withColumn('percentage', F.lit(100) * F.col('TimeNetDistribution')/F.sum('TimeNetDistribution').over(Window.partitionBy()))


        df_diff.write.option("maxRecordsPerFile", 10000).parquet(os.path.join(database_folder, "time_net_{}.parquet".format(time_salt)))
        # pie plot

        prq_file = os.path.join(database_folder, "time_net_{}.parquet".format(time_salt))
        png_file = os.path.join(database_folder, "time_net_{}.png".format(time_salt))

        save_pie_plot(prq_file, png_file, 'percentage', "TimeCat")

        return True

    except AnalysisException as e:
        logger.error("Failed to analyze a SQL query plan. Check if parquet file exists")
        logger.error(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        return False
    except Exception as e:
        logger.error(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        return False



if __name__ == '__main__':

    app_parser = argparse.ArgumentParser(description='geoAppLengthDistAnalysis')

    app_parser.add_argument('--database_folder',
                            nargs='?',
                            type=str,
                            default="../../geo_data_output",
                            help='local path to outputs storage')

    args = app_parser.parse_args()

    database_folder = args.database_folder

    analyse_time_net_dist(database_folder)
