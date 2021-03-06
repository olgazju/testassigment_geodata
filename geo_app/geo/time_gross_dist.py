import os
import argparse
import logging
import traceback
import time

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException

try:
    from geo.common import save_pie_plot, bucketize
except ImportError:
    from common import save_pie_plot, bucketize

logger = logging.getLogger('spark')
logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s")


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
        df_sql = spark.sql("select UserId, TrajectoryId, StepTimestamp from geo_table")

        df_sql_total = df_sql.groupBy("UserId", "TrajectoryId").agg((F.max('StepTimestamp').cast(LongType()) - F.min('StepTimestamp').cast(LongType()))/60/60)\
        .withColumnRenamed("(((CAST(max(StepTimestamp) AS BIGINT) - CAST(min(StepTimestamp) AS BIGINT)) / 60) / 60)", "TimeDiffHours")

		# add buckets as TimeCat column according to TimeDiffHours
        bins = [-float('Inf'), 1, 6, 12, float('Inf')] 
        bucket_names = ['<1', '1-6', '6-12', '>=12']
        df_sql_total = bucketize(df_sql_total, bins, bucket_names, "TimeDiffHours", "TimeCat")

        # count gross time distribution per bucket for all users
        df_sql_total = df_sql_total.groupby("TimeCat").count().withColumnRenamed("count", "TimeGrossDistribution")

        # count distribution in percentage  
        df_sql_total = df_sql_total.withColumn('percentage', F.lit(100) * F.col('TimeGrossDistribution')/F.sum('TimeGrossDistribution').over(Window.partitionBy()))

        df_sql_total.write.option("maxRecordsPerFile", 10000).parquet(os.path.join(database_folder, "time_gross_{}.parquet".format(time_salt)))
        # pie plot

        prq_file = os.path.join(database_folder, "time_gross_{}.parquet".format(time_salt))
        png_file = os.path.join(database_folder, "time_gross_{}.png".format(time_salt))

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

    analyse_time_gross_dist(database_folder)
