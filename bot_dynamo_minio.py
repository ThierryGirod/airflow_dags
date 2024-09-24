from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3
import pandas as pd
from io import BytesIO
from minio import Minio

# AWS DynamoDB Configuration
AWS_REGION = 'eu-central-2'
DYNAMO_TABLE = 'bot'

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

def get_dynamo_data():
    """Fetch data from DynamoDB."""
    dynamo_client = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = dynamo_client.Table(DYNAMO_TABLE)
    
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
        'bots.csv',
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
    'retries': 1,
}

with DAG('bot_dynamo_to_minio_csv',
         default_args=default_args,
         schedule_interval='*/1 * * * *',
         catchup=False) as dag:
    
    # Task: DynamoDB to MinIO
    dynamo_to_minio_task = PythonOperator(
        task_id='dynamo_to_minio_task',
        python_callable=dynamo_to_minio
    )

    dynamo_to_minio_task