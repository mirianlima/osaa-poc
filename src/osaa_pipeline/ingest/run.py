from dotenv import load_dotenv
import os
import boto3
import osaa_pipeline.config as config

load_dotenv()

# Initialize S3 Client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('KEY_ID'),
    aws_secret_access_key=os.getenv('SECRET'),
    region_name=os.getenv('REGION')
)

# Access configurations
raw_data_dir = config.RAW_DATA_DIR
s3_bucket_name = config.S3_BUCKET_NAME
landing_area_folder = config.LANDING_AREA_FOLDER
file_to_s3_folder_mapping = config.FILE_TO_S3_FOLDER_MAPPING

# Load files to s3 buckets
def upload_files_to_s3():
    for local_file_name, s3_sub_folder in file_to_s3_folder_mapping.items():
        # Construct local folder path based on the S3 sub-folder mapping
        local_folder_path = os.path.join(raw_data_dir, s3_sub_folder)
        
        # Construct the full local file path
        local_file_path = os.path.join(local_folder_path, local_file_name)
        
        # Construct S3 object key (path within the bucket)
        s3_object_key = f'{landing_area_folder}/{s3_sub_folder}/{local_file_name}'
        
        try:
            # Check if local file exists
            if os.path.isfile(local_file_path):
                # Upload the file to S3
                s3_client.upload_file(local_file_path, s3_bucket_name, s3_object_key)
                print(f'Successfully uploaded {local_file_name} to s3://{s3_bucket_name}/{s3_object_key}')
            else:
                print(f'File not found: {local_file_path}')
        except Exception as e:
            print(f'Error uploading {local_file_name} to S3: {e}')

if __name__ == '__main__':
    upload_files_to_s3()