import ibis
from pipeline.etl import DataLoader, DataTransformer
from pipeline.utils import setup_logger
from pipeline.catalog import save_s3, save_duckdb, save_parquet
from pipeline.config import MASTER_DATA_DIR, S3_BUCKET_NAME, STAGING_AREA_PATH, LOCAL

# Set up logging
logger = setup_logger(__name__)

class ETL:
    """
    Class to handle the ETL process: Extract, Transform, Load.
    """

    def __init__(self, con):
        """
        Initialize the ETL process with a DuckDB backend.

        :param con: The Ibis-DuckDB backend connection
        """
        self.con = con

    # 1. EXTRACT - LOAD THE DATA
    def extract(self):
        """
        Extract data from the data sources.
        """
        try:
            data_loader = DataLoader(self.con)
            result = data_loader.load_data()
            if not isinstance(result, dict):
                raise ValueError("Data loader did not return a dictionary")
            logger.info("Data successfully loaded.")

        except Exception as e:
            logger.error(f"Failed to load data: {str(e)}. Please check the data source.")
            raise ValueError("Data loading failed") from e
    
    # 2. TRANSFORM - CREATE MASTER TABLE
    def transform(self):
        """
        Transform the data into the master table.
        """
        try:
            data_transformer = DataTransformer(self.con)
            master = data_transformer.create_master_table()
            logger.info("Master table successfully created.")
            return master
        
        except ValueError as e:
            logger.error(f"Failed to create master table: {e}")
            raise

    # 3. LOAD - SAVE MASTER TABLE
    def load(self, master):
        """
        Load the master table to S3 and local storage (parquet and DuckDB files).
        """
        try:
            if LOCAL:
                logger.info("Saving master table to S3 and local storage.")
                save_s3(
                    table_exp=master,
                    s3_path=f's3://{S3_BUCKET_NAME}/{STAGING_AREA_PATH}/master/master.parquet'
                )
                save_parquet(
                    table_exp=master,
                    local_path=f'{MASTER_DATA_DIR}/master.parquet'
                )
                local_db = ibis.duckdb.connect(f'{MASTER_DATA_DIR}/staging.db')
                save_duckdb(table_exp=master, local_db=local_db)
                local_db.disconnect()
            else:
                logger.info("Saving master table only to S3.")
                save_s3(
                    table_exp=master,
                    s3_path=f's3://{S3_BUCKET_NAME}/{STAGING_AREA_PATH}/master/master.parquet'
                )
        except Exception as e:
            logger.error(f"Error saving master table: {str(e)}")
            raise
    
    def run(self):
        """
        Run the entire ETL process: extract, transform, and load.
        """
        try:
            self.extract()
            master = self.transform()
            self.load(master)

        finally:
            self.con.disconnect()

if __name__ == '__main__':
    # Create an in-memory Ibis-DuckDB connection
    con = ibis.duckdb.connect(':memory:')

    # Instantiate and run the ETL process
    etl_process = ETL(con)
    etl_process.run()