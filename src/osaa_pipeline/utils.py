import os
import logging
import boto3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

### s3 CLIENT INITIALIZER ###
def s3_client_init():
    from dotenv import load_dotenv
    load_dotenv()

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('KEY_ID'),
        aws_secret_access_key=os.getenv('SECRET'),
        region_name=os.getenv('REGION')
    )

    return s3_client

### AWS S3 INTERACTIONS ###

def get_s3_file_paths(bucket_name, prefix='landing/'):
    """
    Get a list of file paths from the S3 bucket and organize them into a dictionary.
    """
    paginator = s3_client_init().get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': prefix}
    page_iterator = paginator.paginate(**operation_parameters)
    filtered_iterator = page_iterator.search("Contents[?Key != 'landing/'][]")

    file_paths = {}
    for key_data in filtered_iterator:
        key = key_data['Key']
        parts = key.split('/')
        if len(parts) == 3:  # Ensure we have landing/source/filename structure
            source, filename = parts[1], parts[2]
            if source not in file_paths:
                file_paths[source] = {}
            file_paths[source][filename.split('.')[0]] = f"s3://{bucket_name}/{key}"

    return file_paths

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