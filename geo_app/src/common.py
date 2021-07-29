LABELS_SCHEMA = StructType([StructField("StartTime", TimestampType(), False),
                    StructField("EndTime", TimestampType(), False),
                    StructField("Mode", StringType(), False)])
