from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, to_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType, DateType, IntegerType
import time

DAYS_DIFF = 723
BUCKET_NAME = 'elevated-dynamo-bucket'
PROJECT_ID = 'elevated-dynamo-370709'
DATASET_ID = 'dataproc'

spark_session = SparkSession.builder \
    .appName("JSONtoBigQuery") \
    .getOrCreate()


start = time.time()
df_with_schema = spark_session.read.format("csv") \
    .load(f"gs://elevated-dynamo-bucket/supermarket_sales - Sheet1.csv")
print(f"Read data execution time without specifying schema: {time.time() - start} seconds")

df_with_schema.write.format('bigquery') \
    .option("temporaryGcsBucket", 'dataprocbucket24123') \
    .option('table', 'elevated-dynamo-370709.dataproc.sales' ) \
    .save()

spark_session.stop()