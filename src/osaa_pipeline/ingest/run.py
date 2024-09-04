from dotenv import load_dotenv
import os
import boto3
from osaa_pipeline.utils import upload_s3_client
import osaa_pipeline.config as config 

load_dotenv()

# Initialize S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('KEY_ID'),
    aws_secret_access_key=os.getenv('SECRET'),
    region_name=os.getenv('REGION')
)

# Load files to S3 buckets
def upload_files_to_s3():
    """
    Uploads files from the local raw_data directory to specified landing folders in the S3 bucket.
    """
    # Use config variables directly in the function call
    upload_s3_client(
        s3_client=s3_client, 
        local_data_dir=config.RAW_DATA_DIR, 
        s3_bucket_name=config.S3_BUCKET_NAME, 
        landing_area_folder=config.LANDING_AREA_FOLDER, 
        file_to_s3_folder_mapping=config.FILE_TO_S3_FOLDER_MAPPING
    )

if __name__ == '__main__':
    upload_files_to_s3()