from osaa_pipeline.etl.extract import DataLoader
import logging

def main():
    # Initialize the DataLoader
    loader = DataLoader()

    # Load the data
    result = loader.load_data()

    # Print out some information about the loaded data
    for source, files in result.items():
        print(f"Source: {source}")
        for name, file_info in files.items():
            if "error" in file_info:
                print(f"  Error loading {name}: {file_info['error']}")
            else:
                print(f"  Loaded {name}: {file_info['description']}")
                print(f"    Number of rows: {file_info['data'].count().execute()}")
                print(f"    Columns: {', '.join(file_info['data'].columns)}")

if __name__ == "__main__":
    main()