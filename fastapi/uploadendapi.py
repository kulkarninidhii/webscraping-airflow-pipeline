import logging
from fastapi import FastAPI, File, UploadFile, HTTPException
import boto3
import os
from dotenv import load_dotenv

import logging
import boto3

# Enable debug logging for Boto3
logging.basicConfig(level=logging.ERROR)
boto3.set_stream_logger(name='botocore')

# Load environment variables from .env file
load_dotenv()

# Use AWS credentials
aws_access_key_id = 'AKIAZI2LHQ7DCO56WE6D'
aws_secret_access_key = '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'
print(aws_access_key_id, aws_secret_access_key)
# Set up logging configuration
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize S3 client
s3_client = boto3.client('s3',region_name="us-east-2", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_client.list_buckets
bucket_name = os.getenv('bucket')

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # Upload file to S3
        logger.info(f"Uploading file: {file.filename}")
        s3_client.upload_fileobj(file.file, bucket_name, file.filename)
        return {"message": "File uploaded successfully"}
    except Exception as e:
        logger.error(f"Failed to upload file '{file.filename}' to S3: {e}")
        raise HTTPException(status_code=500, detail=str(e))

