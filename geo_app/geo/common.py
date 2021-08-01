import matplotlib.pyplot as plt
from typing import Type

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer
from pyspark.sql.window import Window

import pandas as pd


def save_pie_plot(prq_file: str, file_name: str, proportions_name: str, labels_name: str):

    df = pd.read_parquet(prq_file)

    proportions = df[proportions_name].values
    labels = df[labels_name].values

    fig1, ax1 = plt.subplots()
    ax1.pie(proportions, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.savefig(file_name)


def bucketize(geo_df: Type[DataFrame], bins: list, bucket_names: list, inputCol: str, outputCol: str):

    bucket_names_dict = {float(i): s for i, s in enumerate(bucket_names)}
    bucketizer = Bucketizer(splits=bins, inputCol=inputCol, outputCol=outputCol)
    geo_df = bucketizer.setHandleInvalid("keep").transform(geo_df)

    udf_buckett = F.udf(lambda x: bucket_names_dict[x], StringType())
    return geo_df.withColumn(outputCol, udf_buckett(outputCol))


def hav_dist(origin_lat, origin_long, dest_lat, dest_long):
    a = (
        F.pow(F.sin(F.radians(dest_lat - origin_lat) / 2), 2) +
        F.cos(F.radians(origin_lat)) * F.cos(F.radians(dest_lat)) *
        F.pow(F.sin(F.radians(dest_long - origin_long) / 2), 2))
    return ( F.atan2(F.sqrt(a), F.sqrt(-a + 1)) * 12742)


def calculate_distance(geo_df: Type[DataFrame], group_columns: list, orderByColumn: str):


    column_list = group_columns
    windowSpec = Window.partitionBy([F.col(x) for x in column_list]).orderBy(orderByColumn)

    new_geo_df = geo_df.withColumn("PrevLongitude", F.lag("Longitude", 1).over(windowSpec))
    new_geo_df = new_geo_df.withColumn("PrevLatitude", F.lag("Latitude", 1).over(windowSpec))

    
    distance_geo_df = new_geo_df.withColumn("Distance", 
          hav_dist( F.col("PrevLatitude"), F.col("PrevLongitude"), F.col("Latitude"), F.col("Longitude")))\
            .fillna({'Distance':0.0})

    return distance_geo_df