from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("exampleApp").getOrCreate()

# Create a sample DataFrame
data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
df = spark.createDataFrame(data, ["Name", "Age"])

#df.write.format("delta").saveAsTable("default.test")

# Show DataFrame
df.show()

# Stop the Spark session
spark.stop()