import ibis
import logging
import fsspec
from datetime import datetime, timezone
from osaa_pipeline.utils import s3_client_init, get_s3_file_paths
from osaa_pipeline.config import S3_BUCKET_NAME, LANDING_AREA_FOLDER

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Init the s3 client
s3_client = s3_client_init()

# set extracted_at timestamp
time_now = datetime.now(timezone.utc)

class DataLoader:
    def __init__(self, bucket_name=S3_BUCKET_NAME):
        # Create an fsspec filesystem for S3
        self.s3 = fsspec.filesystem('s3')

        # Connect to DuckDB
        self.con = ibis.duckdb.connect()

        # Register the S3 filesystem with DuckDB
        self.con.register_filesystem(self.s3)

        # Get file paths from S3
        self.file_paths = get_s3_file_paths(bucket_name, prefix='landing/')

    @staticmethod
    def add_extracted_at(t):
        # Add an extracted_at column with the current timestamp
        return t.mutate(extracted_at=ibis.literal(time_now)).relocate("extracted_at")

    def load_data(self):
        """Extract data from all sources found in S3 landing folder."""
        result = {}
        for source, files in self.file_paths.items():
            result[source] = {}
            for name, path in files.items():
                try:
                    logging.info(f"Processing file: {source}/{name}")
                    
                    data = (
                        self.con
                        .read_parquet(path, table_name=f"{source}_{name}")
                        .pipe(self.add_extracted_at)
                    )

                    result[source][name] = {
                        "data": data,
                        "description": f"Dataset: {source}, File: {name}",
                        "source_path": path,
                        "last_updated": time_now
                    }

                    logging.info(f"Successfully processed file: {source}/{name}")

                except Exception as e:

                    logging.error(f"Error processing file {source}/{name}: {str(e)}")

                    result[source][name] = {
                        "error": str(e),
                        "source_path": path,
                        "last_updated": time_now
                    }

        return result
