import ibis
import logging

logger = logging.getLogger(__name__)

def process_edu_data(connection, data, label, dataset_name):
    """Process EDU data (e.g., OPRI or SDG) and return the transformed Ibis table."""
    if data not in connection.list_tables() or label not in connection.list_tables():
        logger.error(f"Skipping {dataset_name.upper()} processing as one or both tables do not exist.")
        return None
    
    try:
        logger.info(f"Processing {dataset_name.upper()} data from tables '{data}' and '{label}'...")

        tdata = connection.table(data).rename("snake_case")
        tlabel = connection.table(label).rename("snake_case")

        processed = (
            tdata
            .join(tlabel, tdata.indicator_id == tlabel.indicator_id, how="left")
            .select("country_id", "indicator_id", "year", "value", indicator_label="indicator_label_en")
            .mutate(database=ibis.literal(dataset_name))
            .filter(ibis._.year > 1999)
        )

        logger.info(f"{dataset_name.upper()} data from tables '{data}' and '{label}' successfully processed.")
        
        return processed
    
    except Exception as e:
        logger.exception(f"Error processing {dataset_name.upper()} data from tables '{data}' and '{label}': {e}")
        return None