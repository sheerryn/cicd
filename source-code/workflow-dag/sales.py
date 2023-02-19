from pyspark.sql import SparkSession
from pyspark.sql.functions import date_add, to_timestamp
from pyspark.sql.types import LongType, StringType, StructField, StructType, DateType, IntegerType
import time

DAYS_DIFF = 723
BUCKET_NAME = '${GCP_PROJECT_ID}-bucket'
PROJECT_ID = $(gcloud config list --format 'value(core.project)')
DATASET_ID = 'project_bq'

spark_session = SparkSession.builder \
    .appName("JSONtoBigQuery") \
    .getOrCreate()


start = time.time()
df_with_schema = spark_session.read.format("csv") \
    .load(f"gs://${GCP_PROJECT_ID}-bucket/supermarket_sales.csv")
print(f"Read data execution time without specifying schema: {time.time() - start} seconds")

df_with_schema.write.format('bigquery') \
    .option("temporaryGcsBucket", 'dataprocbucket241233') \
    .option('table', 'project_bq.dataproc.sales' ) \
    .save()

spark_session.stop()