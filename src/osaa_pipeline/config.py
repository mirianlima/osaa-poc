import os
from osaa_pipeline.utils import generate_file_to_s3_folder_mapping

# get the local root directory 
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# Define the raw_data directory relative to the root
RAW_DATA_DIR = os.path.join(ROOT_DIR, 'raw_data')

# S3 configurations
S3_BUCKET_NAME = 'poc-aug-2024'
LANDING_AREA_FOLDER = 'landing'

# Automatically generate the mapping
FILE_TO_S3_FOLDER_MAPPING = generate_file_to_s3_folder_mapping(RAW_DATA_DIR)
