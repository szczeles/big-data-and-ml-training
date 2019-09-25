from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--date')
args = parser.parse_args()

spark = (SparkSession.builder
    .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    .config('hive.exec.dynamic.partition.mode', 'nonstrict')
    .enableHiveSupport().getOrCreate())

daily_data = spark.table('default.badges') \
    .where(col('date').cast('date') == args.date)

daily_data.groupBy('name').count() \
    .withColumn('dt', lit(args.date)) \
    .repartition(1) \
    .write.mode("overwrite").insertInto("badgestats", overwrite=True)
