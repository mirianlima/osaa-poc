import os

# get the local root directory 
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Define the raw_data directory relative to the root
RAW_DATA_DIR = os.path.join(ROOT_DIR, 'raw_data')

# S3 configurations
S3_BUCKET_NAME = 'poc-aug-2024'
LANDING_AREA_FOLDER = 'landing'
STAGING_AREA_FOLDER = 'staging'
