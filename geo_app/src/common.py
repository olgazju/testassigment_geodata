import matplotlib.pyplot as plt
from typing import Type

from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer


def save_pie_plot(proportions: list, labels: list, filename: str):

    fig1, ax1 = plt.subplots()
    ax1.pie(proportions, labels=labels, autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.savefig(filename)


def bucketize(geo_df: Type[DataFrame], bins: list, bucket_names: list, inputCol: str, outputCol: str):

    bucket_names_dict = {float(i): s for i, s in enumerate(bucket_names)}
    bucketizer = Bucketizer(splits=bins, inputCol=inputCol, outputCol=outputCol)
    geo_df = bucketizer.setHandleInvalid("keep").transform(geo_df)

    udf_buckett = F.udf(lambda x: bucket_names_dict[x], StringType())
    return geo_df.withColumn(outputCol, udf_buckett(outputCol))