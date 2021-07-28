import logging
import argparse
import os
import glob 
import traceback
from typing import Type
from itertools import chain
from datetime import datetime

from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType
from pyspark.sql.functions import concat, col, lit, coalesce, create_map

logger = logging.getLogger('spark')
logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s")


def read_labels(spark: Type[SparkSession], labels_filename: str, labels_schema: Type[StructType]) -> Type[DataFrame]:

    if os.path.exists(labels_filename):
        LABELS = ['walk', 'bike', 'bus', 'car', 'subway','train', 'airplane', 'boat', 'run', 'motorcycle', 'taxi']
        mode_ids = {s : i + 1 for i, s in enumerate(LABELS)}

        labels_df = spark.read.option("header", True).option("timestampFormat", "yyyy/MM/dd HH:mm:ss").option("delimiter", "\t").schema(labels_schema).csv(labels_filename).cache()

        # map string modes to numeric categories
        mapping_expr  = create_map([lit(x) for x in chain(*mode_ids.items())])
        labels_df= labels_df.withColumn('Label', coalesce(mapping_expr[labels_df['Mode']], labels_df['Mode']).cast(IntegerType()))

        return labels_df

    else:

        return None


def merge_labels(labels_df: Type[DataFrame], user_df: Type[DataFrame]) -> Type[DataFrame]:

    if labels_df:
        cond = [(user_df['StepTimestamp'] >= labels_df['StartTime']) & (user_df['StepTimestamp'] <= labels_df["EndTime"])]
        return user_df.join(labels_df, cond, "left")\
               .fillna({'Label':0})\
               .drop('StartTime', "EndTime", "Mode")
    else:
        return user_df.withColumn('Label', lit(0))


def read_user_trajectory(spark: Type[SparkSession], trajectory_filename: str, trajectory_schema: Type[StructType], user_id: str, current_run_timestamp) -> Type[DataFrame]:

    '''
    Data Format:
        lines 1...6 from plt file are useless and are removed
        Column 2 (always 0) and 4 (same as 5&6) are skipped
        a new field TrTimestamp was created from columns 5&6, date and time were combined
        a new field IngestionTime was added as current run time
        UserId and TrajectoryID were also added as columns
        Row example:
        IngestionTime |UserId| TrajectoryID| Latitude| Longitude| Altitude| StepTimestamp
        2021-07-28 18:02:02| 021| 20070503005908| 28.9177333333333| 118.052183333333| 5134.51443569554| 2007-05-03 00:59:08
    '''
    trajectory_id = os.path.basename(trajectory_filename).replace('.plt','')
    data = spark.sparkContext.textFile(trajectory_filename)\
                      .zipWithIndex()\
                      .filter(lambda row: row[1] >= 6)\
                      .map(lambda row: row[0])\
                      .map(lambda x: x.split(','))\
                      .map(lambda x: [current_run_timestamp, user_id, trajectory_id, float(x[0]), float(x[1]), float(x[3]),  datetime.strptime(" ".join([x[5], x[6]]), '%Y-%m-%d %H:%M:%S')])

    return spark.createDataFrame(data, trajectory_schema)


def ingest(input_folder: str, output_folder: str) -> bool:

    if not os.path.isdir(input_folder):
        logger.error("Input folder doesn't exit")
        return False

    try:  

        trajectory_schema = StructType([StructField("IngestionTime", TimestampType(), False),
                                StructField("UserId", StringType(), False),
                                StructField("TrajectoryID", StringType(), False),
                                StructField("Latitude", DoubleType(), True),
                                StructField("Longitude", DoubleType(), True),
                                StructField("Altitude", DoubleType(), True),
                                StructField("StepTimestamp", TimestampType(), True)])

        labels_schema = StructType([StructField("StartTime", TimestampType(), False),
                                    StructField("EndTime", TimestampType(), False),
                                    StructField("Mode", StringType(), False)])

        spark_context = SparkContext.getOrCreate(SparkConf().setMaster("local[*]").setAppName("geoAppIngestion"))
        spark = SparkSession(spark_context)
        spark.sparkContext.setLogLevel("ERROR")

        current_run_timestamp = datetime.now()

        # iterate over all folders in the dataset, folder name means user ID
        for sub_folder in glob.iglob(os.path.join(input_folder, "Data", "[!.]*")):
            user_id = os.path.basename(sub_folder)
            print("processing user {}".format(user_id))

            # read labels
            labels_df = read_labels(spark, os.path.join(sub_folder, "labels.txt"), labels_schema)

            # read trajectories from current user, merge with labels and save to user_trajectories_df
            # iterate over all files in the folder, file name means Trajectory ID
            user_trajectories_df = None

            for file in glob.iglob(os.path.join(sub_folder, "Trajectory", "*.plt")):

                df = read_user_trajectory(spark, file, trajectory_schema, user_id, current_run_timestamp)
                df = merge_labels(labels_df, df)

                if user_trajectories_df:
                    user_trajectories_df = user_trajectories_df.union(df)
                else:
                    user_trajectories_df = df

            user_trajectories_df.show()

            print("Save to parquet, user {}".format(user_id))
            user_trajectories_df.write.mode('append').parquet(os.path.join(output_folder, "geo_table.parquet"))

    except Exception as e:
        logger.error(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))


if __name__ == '__main__':

    app_parser = argparse.ArgumentParser(description='geoAppIngestion')

    app_parser.add_argument('input_folder',
                       nargs='?',
                       type=str,
                       default="../../test_data",
                       #default="../../geo_data_source/Geolife Trajectories 1.3",
                       help='local path to GEO dataset')

    app_parser.add_argument('output_folder',
                            nargs='?',
                            type=str,
                            default="../../geo_data_output",
                            help='local path to outputs storage')

    args = app_parser.parse_args()

    input_folder = args.input_folder
    output_folder = args.output_folder

    ingest(input_folder, output_folder)
