import os
import ibis
import logging
from datetime import datetime
from osaa_pipeline.utils import download_files_from_s3
from osaa_pipeline.config import S3_BUCKET_NAME, LANDING_AREA_FOLDER
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Load environment variables from .env file if present
load_dotenv()

# Initialize S3 client
import boto3
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

# Set extracted_at timestamp
extracted_at = datetime.utcnow().isoformat()

# Utility functions

def add_extracted_at(t):
    """
    Add extracted_at column to the table to track extraction time.
    """
    t = t.mutate(extracted_at=ibis.literal(extracted_at)).relocate("extracted_at")
    return t

def constraints(t):
    """
    Ensure table is not empty by applying constraints.
    """
    assert t.count().to_pyarrow().as_py() > 0, "table is empty!"
    return t

# Main extraction functions for each data source

def extract_source_data(source_folder):
    """
    Extracts data from the S3 landing folder for the given source folder.
    
    :param source_folder: The folder in the landing area to extract (e.g., 'wdi', 'edu').
    :return: A Parquet table with the extracted data.
    """
    try:
        # Download all files from the source folder in S3 to a local directory
        local_dir = os.path.join('/tmp', source_folder)  # Use /tmp or another temp location
        s3_folder = f'{LANDING_AREA_FOLDER}/{source_folder}'
        download_files_from_s3(s3_client, S3_BUCKET_NAME, s3_folder, local_dir)
        
        # Convert all CSV files to Ibis tables and merge them
        data_files = [os.path.join(local_dir, f) for f in os.listdir(local_dir) if f.endswith('.csv')]
        tables = [ibis.read_csv(file) for file in data_files]
        
        # Combine all tables into one
        combined_table = ibis.concat(tables)
        
        # Add 'extracted_at' timestamp column
        combined_table = add_extracted_at(combined_table)
        
        # Apply constraints (check if the table is not empty)
        combined_table = constraints(combined_table)
        
        # Write the final table to Parquet
        parquet_output_path = os.path.join(local_dir, f'{source_folder}.parquet')
        combined_table.to_parquet(parquet_output_path)
        
        logging.info(f'Successfully extracted and converted data for {source_folder} to Parquet.')
        return combined_table

    except Exception as e:
        logging.error(f"Error extracting data from {source_folder}: {e}", exc_info=True)
        return None

# Main function to extract all sources
def extract_all_data():
    """
    Extracts all data from the landing folder in the S3 bucket, converts it to Parquet,
    and adds the extracted_at timestamp.
    """
    sources = ['wdi', 'edu']  # Add more sources as needed
    for source in sources:
        logging.info(f"Extracting data from {source} folder...")
        extract_source_data(source)

if __name__ == "__main__":
    extract_all_data()
