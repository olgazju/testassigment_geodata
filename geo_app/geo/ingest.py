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

LABELS_EXCEPTION = 'Labels.txt file exists but 0 data was read'

logger = logging.getLogger('spark')
logging.basicConfig(
    format="%(asctime)-15s [%(levelname)s] %(funcName)s: %(message)s")


def read_labels(spark: Type[SparkSession], labels_filename: str) -> Type[DataFrame]:
    '''
    Read transportation modes from lables files and convert them to categorical values
    '''
    if os.path.exists(labels_filename):

        labels_file_schema = StructType([StructField("StartTime", TimestampType(), True),
                             StructField("EndTime", TimestampType(), True),
                             StructField("Mode", StringType(), True)])

        # !!! Spark always switch nullable from False to True on read, use df.na.drop() to drop null values

        LABELS = ['walk', 'bike', 'bus', 'car', 'subway','train', 'airplane', 'boat', 'run', 'motorcycle', 'taxi']
        mode_ids = {s : i + 1 for i, s in enumerate(LABELS)}

        labels_df = spark.read.option("header", True).option("timestampFormat", "yyyy/MM/dd HH:mm:ss").option("delimiter", "\t").schema(labels_file_schema).csv(labels_filename)

        if len(labels_df.head(1)) == 0: 
            raise ValueError(LABELS_EXCEPTION)

        # map string modes to numeric categories
        mapping_expr  = create_map([lit(x) for x in chain(*mode_ids.items())])
        labels_df= labels_df.withColumn('Label', coalesce(mapping_expr[labels_df['Mode']], labels_df['Mode']).cast(IntegerType())).drop("Mode")

        return labels_df

    else:

        return None


def merge_labels(labels_df: Type[DataFrame], user_df: Type[DataFrame]) -> Type[DataFrame]:

    # merge labels with trajectory if labels.StartTime < trajectory.StepTimestamp < labels.EndTime
    # label 0 means unknown label
    if labels_df:
        cond = [(user_df['StepTimestamp'] >= labels_df['StartTime']) & (user_df['StepTimestamp'] <= labels_df["EndTime"])]
        return user_df.join(labels_df, cond, "left")\
               .fillna({'Label':0})\
               .drop('StartTime', "EndTime", "Mode")
    else:
        return user_df.withColumn('Label', lit(0))


def flatten(pair):
    f, text = pair
    return [line.split(",") + [f] for line in text.splitlines()][6:]
 
      

def read_user_trajectories(spark: Type[SparkSession], trajectory_dir: str, user_id: str, current_run_timestamp) -> Type[DataFrame]:

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

    trajectory_schema = StructType([StructField("IngestionTime", TimestampType(), False),
                        StructField("UserId", StringType(), False),
                        StructField("TrajectoryID", StringType(), False),
                        StructField("Latitude", DoubleType(), True),
                        StructField("Longitude", DoubleType(), True),
                        StructField("Altitude", DoubleType(), True),
                        StructField("StepTimestamp", TimestampType(), True)])

    # !!! wholeTextFiles is not memory safe for big files or for a big amount of files
    # but textFile doesn't provide filename and work much slower on a big amount of small files
    data = spark.sparkContext.wholeTextFiles(trajectory_dir).flatMap(flatten)\
                                .map(lambda x: [current_run_timestamp, user_id, os.path.basename(x[7]).replace('.plt',''), float(x[0]), float(x[1]), float(x[3]),  datetime.strptime(" ".join([x[5], x[6]]), '%Y-%m-%d %H:%M:%S')])

    return spark.createDataFrame(data, trajectory_schema)


def ingest(input_folder: str, output_folder: str, spark_filename: str = "geo_table.parquet") -> bool:

    if not os.path.isdir(input_folder):
        logger.error("Input folder doesn't exist")
        return False

    try:  

        print("ingest input folder", input_folder)
        print("ingest output folder", output_folder)

        spark_context = SparkContext.getOrCreate(SparkConf().setMaster("local[*]").setAppName("geoAppIngestion"))
        spark = SparkSession(spark_context)
        spark.sparkContext.setLogLevel("ERROR")

        current_run_timestamp = datetime.now()

        # iterate over all folders in the dataset, folder name means user ID
        i = 0
        for sub_folder in glob.iglob(os.path.join(input_folder, "Data", "[!.]*")):
            user_id = os.path.basename(sub_folder)
            print("processing user {}, number {}".format(user_id, i))

            # read labels
            labels_df = read_labels(spark, os.path.join(sub_folder, "labels.txt"))

            # read trajectories from current user, merge with labels and save to user_trajectories_df
            # iterate over all files in the folder, file name means Trajectory ID
            user_trajectories_df = read_user_trajectories(spark, os.path.join(sub_folder, "Trajectory", "*.plt"), user_id, current_run_timestamp)

            print("Save to parquet, user {}".format(user_id))
            user_trajectories_df.write.option("maxRecordsPerFile", 10000).mode('append').parquet(os.path.join(output_folder, spark_filename))

            i = i + 1

        if i == 0: 
             logger.error("No data inside input folder")
             return False

        return True

    except Exception as e:
        logger.error(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
        return False


if __name__ == '__main__':

    app_parser = argparse.ArgumentParser(description='geoAppIngestion')

    app_parser.add_argument('--input_folder',
                       nargs='?',
                       type=str,
                       #default="../../test_data",
                       default="../../geo_data_source/Geolife Trajectories 1.3",
                       help='local path to GEO dataset')

    app_parser.add_argument('--output_folder',
                            nargs='?',
                            type=str,
                            default="../../geo_data_output",
                            help='local path to outputs storage')

    args = app_parser.parse_args()

    input_folder = args.input_folder
    output_folder = args.output_folder

    ingest(input_folder, output_folder)
