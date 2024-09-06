import os
import logging
import duckdb
from dotenv import load_dotenv
import osaa_pipeline.config as config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

key_id=os.getenv('KEY_ID')
secret=os.getenv('SECRET')
region=os.getenv('REGION')

# Set AWS credentials from environment variables
def setup_s3_secret(con: duckdb.DuckDBPyConnection):
    """
    Sets up the S3 secret in DuckDB for S3 access using parameterized queries.

    :param con: DuckDB connection
    :param key_id: AWS access key ID
    :param secret: AWS secret access key
    :param region: AWS region
    """
    con.sql(f"""
    CREATE SECRET my_s3_secret (
        TYPE S3,
        KEY_ID '{key_id}',
        SECRET '{secret}',
        REGION '{region}'
    );
    """)

def convert_csv_to_parquet_and_upload(con, local_file_path, s3_file_path):
    """
    Converts a CSV file to Parquet and uploads it directly to S3 using DuckDB.

    :param con: DuckDB connection
    :param local_file_path: Path to the local CSV file.
    :param s3_file_path: The S3 file path for the output Parquet file.
    """
    try:
        con.sql(f"""
        COPY (SELECT * FROM read_csv('{local_file_path}', header = true))
        TO '{s3_file_path}'
        (FORMAT PARQUET)
        """
        )

        logging.info(f"Successfully converted and uploaded {local_file_path} to {s3_file_path}")

    except Exception as e:

        logging.error(f"Error converting and uploading {local_file_path} to S3: {e}", exc_info=True)


# Map the local raw data paths to s3-level paths
def generate_file_to_s3_folder_mapping(raw_data_dir):
    """
    Generates a mapping of files to their corresponding S3 folder based on
    the directory structure in the raw_data folder.
    Excludes any folder and file that starts with symbols from the set `~!@#$%^&*()_-+={[}}|\:;"'<,>.?/`.

    :param raw_data_dir: The base directory containing raw data subfolders.
    :return: A dictionary where the key is the filename and the value is the subfolder name.
    """
    file_to_s3_folder_mapping = {}

    # Folder and filename patterns/symbols to exclude from ingest
    symbols = """`~!@#$%^&*()_-+={[}}|\:;"'<,>.?/"""

    # Traverse the raw_data directory
    for subdir, _, files in os.walk(raw_data_dir):
        # Get the subfolder name (relative to raw_data_dir)
        sub_folder = os.path.relpath(subdir, raw_data_dir)

        # Exclude folders that start with any symbol in the excluded set
        if sub_folder.startswith(tuple(symbols)):
            continue

        # Map each file to its corresponding subfolder, but exclude files starting with symbols
        for file_name in files:
            if not file_name.startswith(tuple(symbols)):
                file_to_s3_folder_mapping[file_name] = sub_folder

    return file_to_s3_folder_mapping

def convert_and_upload_files(
        local_data_dir, 
        s3_bucket_name, 
        landing_area_folder, 
        file_to_s3_folder_mapping):
    """
    Processes CSV files and uploads them as Parquet to specified folders in an S3 bucket.

    :param local_data_dir: The base directory containing the local files.
    :param s3_bucket_name: The name of the S3 bucket.
    :param landing_area_folder: The landing area folder in the S3 bucket.
    :param file_to_s3_folder_mapping: Mapping of local file names to S3 subfolder names.
    """
    con = duckdb.connect()
    setup_s3_secret(con)

    for file_name_csv, s3_sub_folder in file_to_s3_folder_mapping.items():

        local_file_path = os.path.join(local_data_dir, s3_sub_folder, file_name_csv)

        file_name_pq = f'{os.path.splitext(file_name_csv)[0]}.parquet'

        s3_file_path = f's3://{s3_bucket_name}/{landing_area_folder}/{s3_sub_folder}/{file_name_pq}'

        try:
        
            if os.path.isfile(local_file_path):
                convert_csv_to_parquet_and_upload(con, local_file_path, s3_file_path)

                logging.info(f'Successfully uploaded {s3_file_path}')

            else:
                logging.warning(f'File not found: {local_file_path}')

        except Exception as e:
            logging.error(f'Error uploading {file_name_pq} to S3: {e}', exc_info=True)

    con.close()


if __name__ == '__main__':
    convert_and_upload_files(
        local_data_dir=config.RAW_DATA_DIR, 
        s3_bucket_name=config.S3_BUCKET_NAME, 
        landing_area_folder=config.LANDING_AREA_FOLDER, 
        file_to_s3_folder_mapping=generate_file_to_s3_folder_mapping(config.RAW_DATA_DIR)
    )