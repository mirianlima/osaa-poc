import marimo as mo

app = mo.App(width="full")

@app.cell
def __():
    import marimo as mo
    import duckdb

@app.cell
def __(duckdb):
    duckdb.sql("CREATE TABLE master AS SELECT * FROM '/Users/mlima/Code/PoC/osaa-poc/datalake/staging/master/master.parquet'")

@app.cell
def __(mo, duckdb):
    database_dropdown = mo.ui.dropdown(
        label="Database",
        options=[row[0] for row in duckdb.sql("SELECT DISTINCT database FROM master").fetchall()]
    )
    
    # Correctly fetch and unpack the year range
    year_min, year_max = duckdb.sql("SELECT MIN(year), MAX(year) FROM master WHERE value IS NOT NULL").fetchone()
    
    year_range = mo.ui.range_slider(
        label="Year Range",
        start=year_min,
        stop=year_max,
        value=[year_min, year_max]
    )
    
    mo.hstack([database_dropdown, year_range])

@app.cell
def __(mo, duckdb, database_dropdown):
    if database_dropdown.value:
        indicator_options = [
            row[0] for row in duckdb.sql(
                f"""
                SELECT DISTINCT indicator_label 
                FROM master WHERE database = '{database_dropdown.value}'
                ORDER BY indicator_label
                """
            ).fetchall()
        ]
    else:
        indicator_options = []
    
    indicator_dropdown = mo.ui.dropdown(
        label="Indicator",
        options=indicator_options
    )
    
    indicator_dropdown

@app.cell
def __(mo, duckdb, database_dropdown, indicator_dropdown, year_range):
    if database_dropdown.value and indicator_dropdown.value:
        query = f"""
            SELECT country_id, indicator_id, year, value, indicator_label, database
            FROM master
            WHERE database = '{database_dropdown.value}'
            AND indicator_label = '{indicator_dropdown.value}'
            AND year BETWEEN {year_range.value[0]} AND {year_range.value[1]}
            ORDER BY year, value
        """
        filtered_data = duckdb.sql(query).fetchall()
        mo.ui.table(filtered_data, headers=["Country ID", "Indicator ID", "Year", "Value", "Indicator Label", "Database"])
    else:
        mo.md("Please select both a database and an indicator.")

if __name__ == "__main__":
    app.run()

if __name__ == "__main__":
    app.run()