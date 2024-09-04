import os

def generate_file_to_s3_folder_mapping(raw_data_dir):
    """
    Generates a mapping of files to their corresponding S3 folder based on
    the directory structure in the raw_data folder.

    :param raw_data_dir: The base directory containing raw data subfolders.
    :return: A dictionary where the key is the filename and the value is the subfolder name.
    """
    file_to_s3_folder_mapping = {}

    # Traverse the raw_data directory
    for subdir, _, files in os.walk(raw_data_dir):
        # Get the subfolder name (relative to raw_data_dir)
        sub_folder = os.path.relpath(subdir, raw_data_dir)
        
        # Map each file to its corresponding subfolder
        for file_name in files:
            file_to_s3_folder_mapping[file_name] = sub_folder

    return file_to_s3_folder_mapping
