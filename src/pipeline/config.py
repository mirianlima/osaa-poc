import os

# get the local root directory 
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))



# Define the LOCAL DATA directory relative to the root
    # RAW_DATA_DIR = os.path.join(ROOT_DIR, 'raw_data')
    # PROC_DATA_DIR = os.path.join(ROOT_DIR, 'processed')

DATALAKE_DIR = os.path.join(ROOT_DIR, 'datalake')
RAW_DATA_DIR = os.path.join(DATALAKE_DIR, 'raw')
STAGING_DATA_DIR = os.path.join(DATALAKE_DIR, 'staging')
MASTER_DATA_DIR = os.path.join(STAGING_DATA_DIR, 'master')

# S3 configurations
S3_BUCKET_NAME = 'poc-aug-2024'
LANDING_AREA_FOLDER = 'landing'
STAGING_AREA_PATH = 'staging'

# Local copy of master data
LOCAL=True
