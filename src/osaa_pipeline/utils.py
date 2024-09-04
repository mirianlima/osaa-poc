import os
import logging
import boto3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


### INGEST ###

def generate_file_to_s3_folder_mapping(raw_data_dir):
    """
    Generates a mapping of files to their corresponding S3 folder based on
    the directory structure in the raw_data folder.

    :param raw_data_dir: The base directory containing raw data subfolders.
    :return: A dictionary where the key is the filename and the value is the subfolder name.
    """
    file_to_s3_folder_mapping = {}

    # Traverse the raw_data directory
    for subdir, _, files in os.walk(raw_data_dir):
        # Get the subfolder name (relative to raw_data_dir)
        sub_folder = os.path.relpath(subdir, raw_data_dir)
        
        # Map each file to its corresponding subfolder
        for file_name in files:
            file_to_s3_folder_mapping[file_name] = sub_folder

    return file_to_s3_folder_mapping

### AWS S3 ###

def upload_s3_client(s3_client, local_data_dir, s3_bucket_name, landing_area_folder, file_to_s3_folder_mapping):
    """
    Uploads files from the local data directory to specified folders in an S3 bucket.

    :param s3_client: The boto3 S3 client.
    :param local_data_dir: The base directory containing the local files.
    :param s3_bucket_name: The name of the S3 bucket.
    :param landing_area_folder: The landing area folder in the S3 bucket.
    :param file_to_s3_folder_mapping: Mapping of file names to S3 subfolder names.
    """
    for local_file_name, s3_sub_folder in file_to_s3_folder_mapping.items():
        # Construct the full local file path
        local_file_path = os.path.join(local_data_dir, s3_sub_folder, local_file_name)
        
        # Construct the S3 object key (path within the bucket)
        s3_object_key = f'{landing_area_folder}/{s3_sub_folder}/{local_file_name}'
        
        try:
            # Check if local file exists
            if os.path.isfile(local_file_path):
                # Upload the file to S3
                s3_client.upload_file(local_file_path, s3_bucket_name, s3_object_key)
                logging.info(f'Successfully uploaded {local_file_name} to s3://{s3_bucket_name}/{s3_object_key}')
            else:
                logging.warning(f'File not found: {local_file_path}')
        except Exception as e:
            logging.error(f'Error uploading {local_file_name} to S3: {e}', exc_info=True)

def download_s3_client(s3_client, s3_bucket_name, s3_folder, local_dir):
    """
    Downloads all files from a specified S3 folder to a local directory.

    :param s3_client: The boto3 S3 client.
    :param s3_bucket_name: The name of the S3 bucket.
    :param s3_folder: The folder within the S3 bucket to download files from.
    :param local_dir: The local directory to save downloaded files.
    """
    try:
        # Ensure the local directory exists
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)

        # List objects in the specified S3 folder
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=s3_folder)
        
        # Check if any objects are returned
        if 'Contents' in response:
            for obj in response['Contents']:
                s3_key = obj['Key']
                local_file_path = os.path.join(local_dir, os.path.basename(s3_key))
                
                # Download the file
                s3_client.download_file(s3_bucket_name, s3_key, local_file_path)
                logging.info(f'Successfully downloaded {s3_key} to {local_file_path}')
        else:
            logging.warning(f'No files found in s3://{s3_bucket_name}/{s3_folder}')

    except Exception as e:
        logging.error(f'Error downloading files from S3: {e}', exc_info=True)