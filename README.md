# OSAA Data Pipeline PoC

## Overview

This project implements a **data pipeline Proof of Concept** (PoC) for the United Nations Office of the Special Adviser on Africa (OSAA), leveraging Ibis, DuckDB, the Parquet format and S3 to create an efficient and scalable data processing system. The pipeline ingests data from various sources, transforms it, and stores it in a data lake structure, enabling easy access and analysis.

## Project Structure

```
osaa-poc/
├── datalake/                  # Local representation of the datalake
│   ├── raw/                   # Source data files (CSV)
│   │   ├── edu/               # Contains educational datasets
│   │   ├── wdi/               # World Development Indicators datasets
│   └── staging/               # Staging area for processed Parquet files
├── scratchpad/                # Temporary space for working code or notes
├── src/
│   └── pipeline/              # Core pipeline code
│       ├── etl/               # Extract, Transform, Load scripts
│       │   ├── sources/       # Source-specific data processing (e.g., WDI, EDU)
│       ├── ingest/            # Handles data ingestion from local raw csv to S3 parquet
│       ├── catalog.py         # Defines data catalog interactions
│       ├── config.py          # Stores configuration details (e.g., paths, S3 settings)
│       ├── utils.py           # Utility functions
├── .env                       # Environment variables configuration
├── justfile                   # Automates common tasks (installation, running pipelines)
├── pyproject.toml             # Project metadata and dependencies
├── requirements.txt           # Python package dependencies
```

## Key Components

- **Ibis**: A Python framework for data analysis, used to write expressive and portable data transformations. It provides a high-level abstraction over SQL databases like DuckDB, allowing for cleaner, more Pythonic data manipulation.
- **DuckDB**: A highly performant in-memory SQL engine for analytical queries, used for efficient data processing and querying, in order to process, convert, and interact with Parquet files and S3.
- **Parquet**: A columnar storage file format, used for efficient data storage and retrieval. Used as the core format for storing processed data.
- **S3**: Amazon Simple Storage Service, used as the cloud storage solution for the data lake, storing both raw (landing folder) and processed (staging folder) data.

## How It Works

### Ingestion Process
The Ingest Pipeline reads raw CSV data from `datalake/raw/<source>`, processes it, converts it into Parquet format using DuckDB, and uploads the results to an S3 bucket under the `landing/<source>` folder.

### ETL Process
The ETL Pipeline extracts data, transforms it (cleaning, filtering, joining), and outputs it into a master Parquet file and a DuckDB file, stored locally in `datalake/staging/master/` and optionally uploaded to `staging/master` folder in S3.

## Getting Started

### Prerequisites

- Python 3.9 or higher
- AWS account with S3 access
- Just (command runner) - for running predefined commands

### Setup

1. Clone the repository:
   ```
   git clone https://github.com/mirianlima/osaa-poc.git
   cd osaa-poc
   ```

2. Install dependencies using `just`:
   ```
   just install
   ```

3. Set up your `.env` file with necessary s3 credentials
    ```
    KEY_ID= <your-s3-ID-key>
    SECRET= <your-s3-secret-key>
    URI= <your-s3-bucket-path>
    ```

## Raw Data Setup

The raw data for this project is too large to be directly included in the GitHub repository. Instead, it's compressed and stored using Git Large File Storage (LFS). Follow these steps to set up the raw data correctly:

1. Make sure Git LFS is installed:
    ```
    git lfs install
    ```

2. Clone the repository (if you haven't already):
   ```
   git clone https://github.com/mirianlima/osaa-poc.git
   cd osaa-poc
   ```

3. Pull the LFS files:
   ```
   git lfs pull
   ```

4. Locate the compressed raw data file:
    The compressed file is in the root directory of the project, named `datalake.zip`.

5. Decompress the raw data:
   ```
   unzip `datalake.zip`
   ```

6. Verify the data structure:
   After decompression, you should see the following structure in your `datalake/` directory:
   ```
   datalake/
   ├── raw/
   │   ├── edu/
   │   │   ├── OPRI_DATA_NATIONAL.csv
   │   │   ├── OPRI_LABEL.csv
   │   │   ├── SDG_DATA_NATIONAL.csv
   │   │   ├── SDG_LABEL.csv
   │   ├── wdi/
   │   │   ├── WDICSV.csv
   │   │   ├── WDISeries.csv
   ```
   
Now your raw data is set up correctly, and you can proceed with running the pipeline as described below.

The raw data is also available for bulk download:
- World Bank [WDI catalog](https://datacatalog.worldbank.org/search/dataset/0037712). CSV zip download link [here](https://datacatalogfiles.worldbank.org/ddh-published/0037712/DR0045575/WDI_CSV_2024_10_24.zip?versionId=2024-10-28T13:09:29.1647687Z)
- UNESCO Institute of Statistics (UIS) Education [data catalog](https://uis.unesco.org/bdds). CSV zip direct download links for [SDG](https://uis.unesco.org/sites/default/files/documents/bdds/092024/SDG.zip) and [OPRI](https://uis.unesco.org/sites/default/files/documents/bdds/092024/OPRI.zip)

### Running the Pipeline

Use the `justfile` to run common tasks:

```
just ingest  # Run the ingestion process
just etl     # Run the full ETL process
```

## Next Steps

The next phase of this project will focus on experimenting with different visualization layers to effectively present the processed data. This may include:

- Include a Motherduck destination in the etl pipeline
- Integrate the use of:
    - Iceberg tables for better cataloguing
    - Hamilton for orchestration
    - Open Lineage for data lineage
- Integration with BI tools like Tableau or Power BI
- Experimentation with code-based data app/report/dashboard development using Quarto, Evidence, Marimo and Observable Framework.
- Exploration of data science notebooks for advanced analytics, like Marimo, Quarto, Hex and Deepnote.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contact

Mirian Lima - mirian.lima@un.org

Project Link: [https://github.com/mirianlima/osaa-poc](https://github.com/mirianlima/osaa-poc)

## Acknowledgement

This project was **heavily inspired by** the work of [Cody Peterson](https://github.com/lostmygithubaccount), specifically the [ibis-analytics](https://github.com/ibis-project/ibis-analytics) repository. While the initial direction and structure of the project were derived from Cody’s original work, significant modifications and expansions have been made to fit the needs and goals of this project, resulting in a codebase that diverges substantially from the original implementation.