import ibis
from pipeline.etl.sources import process_wdi_data, process_edu_data
from pipeline.utils import setup_logger

# Set up logging
logger = setup_logger(__name__)

class DataTransformer:
    """A class to handle dataset transformations."""
    
    def __init__(self, con) -> None:
        """
        Initialize the DataTransformer class.

        :param con: The connection object to the DuckDB instance.
        """
        self.connection = con
    
    def create_master_table(self) -> ibis.Expr:
        """
        Create and return the master table by unioning WDI, OPRI, and SDG datasets.

        :return: A unioned Ibis table expression containing data from all valid datasets.
        :raises ValueError: If no valid datasets are found.
        """
        
        wdi = process_wdi_data(
            connection=self.connection,
            table='wdi_WDICSV',
            label='wdi_WDISeries',
            year_start=2000 # Keep only >= 2000 years
        )

        opri = process_edu_data(
            connection=self.connection,
            data='edu_OPRI_DATA_NATIONAL', 
            label='edu_OPRI_LABEL', 
            dataset_name='opri'
        )

        sdg = process_edu_data(
            connection=self.connection,
            data='edu_SDG_DATA_NATIONAL', 
            label='edu_SDG_LABEL', 
            dataset_name='sdg'
        )

        # Only union the datasets that were processed successfully
        datasets = [wdi, opri, sdg]
        valid_datasets = [ds for ds in datasets if ds is not None]

        if not valid_datasets:
            logger.error("No valid datasets available to create the master table.")
            raise ValueError("No valid datasets found")

        # Union only the valid datasets
        master = ibis.union(*valid_datasets)
        return master