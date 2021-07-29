import os
import argparse
import logging
import traceback
import time

import matplotlib.pyplot as plt
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col, lit, coalesce, create_map, udf, lag, sum
from pyspark.ml.feature import Bucketizer

from pyspark.sql.utils import AnalysisException

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


def analyse_length_dist(database_folder: str, spark_filename: str = "geo_table.parquet") -> bool:

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
        #or direct read without createOrReplaceTempView
        #df_sql = spark.sql("SELECT UserId, TrajectoryId, Latitude, Longitude, StepTimestamp FROM parquet.`geo_table.parquet`")


        # calculate distance between each trajectory steps one by one using haversine formula 
        # for latitude and Longitude per user and trajectory
        udf_get_haversine_dist = udf(get_haversine_dist, DoubleType())

        column_list = ["UserId", "TrajectoryId"]
        windowSpec = Window.partitionBy([col(x) for x in column_list]).orderBy("StepTimestamp")
        df_sql = df_sql.withColumn("Distance", udf_get_haversine_dist("latitude", "Longitude", lag("latitude", 1).over(windowSpec), lag("Longitude", 1).over(windowSpec)))\
                     .fillna({'Distance':0.0})

        # sum all distances per user and trajectory
        df_sql_total= df_sql.groupby("UserId", "TrajectoryId").sum("Distance").withColumnRenamed("sum(Distance)", "TotalDistance")

        # add buckets as DistCat column according to TotalDistance
        bins = [0, 5, 20, 100, float('Inf')] 
        bucket_names = ['<5', '5-20', '20-100', '>=100']
        bucket_names_dict = {float(i): s for i, s in enumerate(bucket_names)}
        bucketizer = Bucketizer(splits=bins, inputCol="TotalDistance", outputCol="DistCat")
        df_sql_total = bucketizer.setHandleInvalid("keep").transform(df_sql_total)

        udf_bucket = udf(lambda x: bucket_names_dict[x], StringType())
        df_sql_total = df_sql_total.withColumn("DistCat", udf_bucket("DistCat"))

        # count trajectory length distribution per bucket for all users
        df_sql_total = df_sql_total.groupby("DistCat").count().withColumnRenamed("count", "TotalDistanceDistribution")

        # count distribution in percentage  
        df_sql_total = df_sql_total.withColumn('percentage', lit(100) * col('TotalDistanceDistribution')/sum('TotalDistanceDistribution').over(Window.partitionBy()))
        df_sql_total.write.format("csv").option("header", "true").save(os.path.join(database_folder, "length_{}.csv".format(time_salt)))

        
        # pie plot
        labels = [i['DistCat'] for i in df_sql_total.collect()]
        sizes = [i['percentage'] for i in df_sql_total.collect()]

        fig1, ax1 = plt.subplots()
        ax1.pie(sizes, labels=labels, autopct='%1.1f%%',
                shadow=True, startangle=90)
        ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

        plt.savefig(os.path.join(database_folder, "length_{}.png".format(time_salt)))


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

    analyse_length_dist(database_folder)
