import ibis
from ibis import _
import ibis.selectors as s
import logging

logger = logging.getLogger(__name__)

def process_wdi_data(connection, table, label, year_start):
    """Process WDI data and return the transformed Ibis table."""
    if table not in connection.list_tables() or label not in connection.list_tables():
        logger.error(f"Skipping WDI processing as one or more tables do not exist.")
        return None
    
    try:
        wdi_data = (
            connection.table(table)
            .rename("snake_case")
            .pivot_longer(
                s.r["1960":],
                names_to="year",
                values_to="value"
            )
            .cast({"year": "int64"})
            .filter(_.year >= year_start)
            .rename(country_id="country_code", indicator_id="indicator_code")
        )

        wdi_label = (
            connection.table(label)
            .rename("snake_case")
            .rename(indicator_id="series_code", indicator_label="indicator_name")
        )

        wdi = (
            wdi_data
            .join(wdi_label, wdi_data.indicator_id == wdi_label.indicator_id, how="left")
            .select("country_id", "indicator_id", "year", "value", "indicator_label")
            .mutate(database=ibis.literal("wdi"))
            .cast({"year": "int64"})
        )

        logger.info(f"WDI data from table '{table}' successfully processed.")
        return wdi

    except Exception as e:
        logger.exception(f"Error processing WDI data from table '{table}': {e}")
        return None