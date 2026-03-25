import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src')))
from utils.logger import logging

# LocalStack S3 endpoint (for local testing)
S3_ENDPOINT = "http://localhost:4566"
S3_BUCKET_NAME = "social-media-brand-monitor"

def create_s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,  # LocalStack endpoint
        aws_access_key_id='test',
        aws_secret_access_key='test',
        region_name='us-east-1'  # Specify a default region
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """Ensures that the S3 bucket exists, creating it if necessary."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        # If a 404 error is raised, the bucket does not exist.
        if e.response['Error']['Code'] == '404':
            logging.info(f"Bucket '{bucket_name}' does not exist. Creating it...")
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' created successfully.")
        else:
            # Re-raise the exception if it's not a 404.
            logging.error("An unexpected error occurred when checking for the bucket.")
            raise

def upload_file_to_s3(file_path, file_name):
    s3 = create_s3_client()
    try:
        ensure_bucket_exists(s3, S3_BUCKET_NAME)
        logging.info(f"Started uploading {file_name} to S3 bucket {S3_BUCKET_NAME}")
        s3.upload_file(file_path, S3_BUCKET_NAME, file_name)
        logging.info(f"Successfully uploaded {file_name} to {S3_BUCKET_NAME}")
    except FileNotFoundError:
        logging.error(f"The file {file_path} was not found.")
    except NoCredentialsError:
        logging.error("Credentials not available.")
    except ClientError as e:
        logging.error(f"An S3 client error occurred: {e}")


def list_objects_in_bucket():
    s3 = create_s3_client()
    try:
        ensure_bucket_exists(s3, S3_BUCKET_NAME)
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME)
        if 'Contents' in response:
            for obj in response['Contents']:
                logging.info(f"Object: {obj['Key']}, Size: {obj['Size']} bytes")
        else:
            logging.info("No objects found in the bucket.")
    except NoCredentialsError:
        logging.error("Credentials not available.")
    except ClientError as e:
        logging.error(f"An S3 client error occurred: {e}")

# Example usage
if __name__ == "__main__":
    list_objects_in_bucket()
