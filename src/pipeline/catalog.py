import ibis
from pipeline.utils import setup_logger

# Set up logging
logger = setup_logger(__name__)

def save_s3(table_exp: ibis.Expr, s3_path: str) -> None:
    """
    Save the Ibis table expression to S3 as a Parquet file.

    :param table_exp: Ibis table expression to be saved.
    :param s3_path: The full S3 path where the Parquet file will be saved.
    """
    try:
        table_exp.to_parquet(s3_path)
        logger.info(f"Table successfully uploaded to {s3_path}")

    except Exception as e:
        logger.error(f"Error uploading table to S3: {e}", exc_info=True)
        raise

def save_duckdb(table_exp: ibis.Expr, local_db) -> None:
    """
    Save the Ibis table expression locally to a DuckDB database.

    :param table_exp: Ibis table expression to be saved.
    :param local_db: Connection to the local DuckDB database.
    """
    try:
        local_db.create_table(
           'master', 
            table_exp.execute(), 
            overwrite=True
        )
        logger.info("Table successfully created in persistent DuckDB")

    except Exception as e:
        logger.error(f"Error creating table in DuckDB file: {e}", exc_info=True)
        raise

def save_parquet(table_exp: ibis.Expr, local_path: str) -> None:
    """
    Save the Ibis table expression locally as a Parquet file.

    :param table_exp: Ibis table expression to be saved.
    :param local_path: The local file path where the Parquet file will be saved.
    """
    try:
        table_exp.to_parquet(local_path)
        logger.info(f"Table successfully saved to local Parquet file: {local_path}")

    except Exception as e:
        logger.error(f"Error saving table to local Parquet file: {e}", exc_info=True)
        return
    
# TODO: Function to save the data remotely to motherduck
