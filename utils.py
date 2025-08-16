import boto3
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

BUCKET = os.getenv("S3_BUCKET_NAME")

def upload_to_s3(file):
    try:
        s3.upload_fileobj(file, BUCKET, file.filename)
        return f"https://{BUCKET}.s3.amazonaws.com/{file.filename}"
    except NoCredentialsError:
        return None
