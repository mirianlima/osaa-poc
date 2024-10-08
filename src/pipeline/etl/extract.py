import ibis
import ibis.selectors as s
import fsspec
from datetime import datetime, timezone
from pipeline.utils import setup_logger, s3_init, get_s3_file_paths
from pipeline.config import S3_BUCKET_NAME, LANDING_AREA_FOLDER

# Set up logger
logger = setup_logger(__name__)

# Init the s3 client
s3_client = s3_init()

# set extracted_at timestamp
time_now = datetime.now(timezone.utc)

class DataLoader:
    def __init__(self, connection) -> None:
        # Connect to DuckDB
        self.con = connection

        # Register the S3 filesystem with DuckDB
        self.s3 = fsspec.filesystem('s3')
        self.con.register_filesystem(self.s3)

        # Get file paths from S3
        self.file_paths = get_s3_file_paths(S3_BUCKET_NAME, prefix=LANDING_AREA_FOLDER)

    def load_data(self) -> dict:
        """
        Extract data from all sources found in S3 landing folder and load them into an ibis DuckDB backend.

        :return: Dictionary with metadata and any errors encountered during the process.
        """
        result = {}
        for source, files in self.file_paths.items():
            result[source] = {}
            for name, path in files.items():
                try:
                    logger.info(f"Processing file: {source}/{name}")

                    # Load the Parquet data from S3 into DuckDB ibis backend
                    data = (
                        self.con
                        .read_parquet(path, table_name=f"{source}_{name}")
                    )

                    result[source][name] = {
                        "data": data,
                        "description": f"Dataset: {source}, File: {name}",
                        "source_path": path,
                        "last_updated": time_now
                    }

                    logger.info(f"Successfully processed file: {source}/{name}")

                except Exception as e:

                    logger.error(f"Error processing file {source}/{name}: {str(e)}")

                    result[source][name] = {
                        "error": str(e),
                        "source_path": path,
                        "last_updated": time_now
                    }

        # List all tables after loading
        tables = self.con.list_tables()
        logger.info(f"Tables loaded into DuckDB: {tables}")

        return result