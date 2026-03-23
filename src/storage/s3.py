
import boto3
from botocore.exceptions import NoCredentialsError
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
    )

def upload_file_to_s3(file_path, file_name):
    s3 = create_s3_client()
    try:
        logging.info(f"Started uploading {file_name} to S3 bucket {S3_BUCKET_NAME}")
        s3.upload_file(file_path, S3_BUCKET_NAME, file_name)
        logging.info(f"Successfully uploaded {file_name} to {S3_BUCKET_NAME}")
    except FileNotFoundError:
        logging.error(f"The file {file_path} was not found.")
    except NoCredentialsError:
        logging.error("Credentials not available.")

# Example usage
if __name__ == "__main__":
    file_path = "data/raw/api/newsapi_Apple_page_1.json"  # Example file
    file_name = "newsapi_Apple_page_1.json"  # The name to use in S3 bucket
    upload_file_to_s3(file_path, file_name)
