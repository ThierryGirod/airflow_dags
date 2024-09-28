from delta import *
import pyspark
from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook

connection = BaseHook.get_connection('minio_conn')
    
# Extract connection details
minio_access_key = connection.login
minio_secret_key = connection.password

builder = (pyspark.sql.SparkSession.builder.appName("MyApp")
           .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
           .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
           .enableHiveSupport())

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create a sample DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])

configs = spark.sparkContext.getConf().getAll()

df.write.format("delta").saveAsTable("default.test", mode="overwrite")

# Show DataFrame
df.show()

# Stop the Spark session
spark.stop()
