from delta import *
import pyspark
from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from pyspark.sql.functions import *
from pyspark.sql.types import *

connection = BaseHook.get_connection('minio_conn')
    
# Extract connection details
minio_access_key = connection.login
minio_secret_key = connection.password

builder = (pyspark.sql.SparkSession.builder.appName("MyApp")
           .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
           .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
           .config("spark.driver.host", "master1")
           .config("spark.driver.port", "7077")
           .config("spark.driver.bindAddress", "0.0.0.0") 
           .config("spark.driver.blockManager.port", "31311")
           .enableHiveSupport())

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create a sample DataFrame

df = spark.read.json('s3a://trady/landing/bots.json')

bots_df = df.select("id",
                    "account", 
                    col("base_order_size").cast(DoubleType()), 
                    col("created_at").cast(TimestampType()),
                    col("enabled").cast(BooleanType()),
                    "name",
                    "owner",
                    "start_order_type",
                    "strategy",
                    "type"
                    
)

bots_df.write.format("delta").saveAsTable("default.silver_bots", mode="overwrite")

# Stop the Spark session
spark.stop()
