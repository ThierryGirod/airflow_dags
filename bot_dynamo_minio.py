from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3
import pandas as pd
from io import BytesIO
from minio import Minio

def get_minio_client():
    """Retrieve MinIO connection details from Airflow and return a Minio client."""
    connection = BaseHook.get_connection('minio_conn')
    
    # Extract connection details
    minio_endpoint = connection.host
    minio_access_key = connection.login
    minio_secret_key = connection.password
    minio_secure = connection.extra_dejson.get('secure', False)  # Handle secure if set in connection
    
    # Return MinIO client
    return Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=minio_secure
    )

def get_dynamo_client():
    """Retrieve dynamo connection details from Airflow and return a boto3 client."""
    connection = BaseHook.get_connection('aws_dynamo')
    
    # Extract connection details
    aws_access_key = connection.login
    aws_secret_key = connection.password
    aws_region = 'eu-central-2'
    
    return boto3.resource('dynamodb', 
                          aws_access_key_id=aws_access_key,
                          aws_secret_access_key=aws_secret_key,
                          region_name=aws_region)
    
def get_dynamo_data():
    """Fetch data from DynamoDB."""
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('bot')
    
    # Scan DynamoDB table
    response = table.scan()
    data = response['Items']
    
    # Convert to pandas DataFrame
    df = pd.DataFrame(data)
    
    return df

def convert_to_csv(df):
    """Convert DataFrame to CSV format in memory."""
    buffer = BytesIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue()

def upload_to_minio(csv_data):
    """Upload CSV data to MinIO using credentials from Airflow connection."""
    minio_client = get_minio_client()
    minio_bucket = 'trady'
    
    # Check if bucket exists
    if not minio_client.bucket_exists(minio_bucket):
        minio_client.make_bucket(minio_bucket)
    
    # Upload CSV file
    minio_client.put_object(
        minio_bucket,
        'bronze/bots/bots.csv',
        data=BytesIO(csv_data),
        length=len(csv_data),
        content_type='text/csv'
    )

def dynamo_to_minio():
    """Main function to fetch data from DynamoDB and upload to MinIO."""
    # Step 1: Fetch data from DynamoDB
    df = get_dynamo_data()
    
    # Step 2: Convert DataFrame to CSV
    csv_data = convert_to_csv(df)
    
    # Step 3: Upload to MinIO
    upload_to_minio(csv_data)

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 24),
}

with DAG('bot_dynamo_to_minio_csv',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:
    
    # Task: DynamoDB to MinIO
    dynamo_to_minio_task = PythonOperator(
        task_id='dynamo_to_minio_task',
        python_callable=dynamo_to_minio
    )

    # Define the SparkSubmitOperator task
    submit_spark_job = SparkSubmitOperator(
        task_id='bot_silver_gold_hop',
        name="bot_silver_gold_hop",
        verbose=True,
        conn_id='spark_default',  # The connection defined in Airflow (YARN, Spark Standalone, etc.)
        packages="io.delta:delta-spark_2.12:3.2.0"
        application="/bitnami/python/bot_silver_gold_hop.py",
    )

    dynamo_to_minio_task >> submit_spark_job