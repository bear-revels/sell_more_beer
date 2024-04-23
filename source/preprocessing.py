import pandas as pd
import os

class DataProcessor:
    def __init__(self):
        pass

    def comma_delimiter(self, data_dir):
        """
        Converts non-comma separated CSV files in the specified directory to comma-separated format.

        Args:
            data_dir (str): Path to the directory containing CSV files.
        """
        # Get a list of all files in the data directory
        files = os.listdir(data_dir)

        # Iterate through each file
        for file in files:
            # Check if the file is a CSV file
            if file.endswith(".csv"):
                file_path = os.path.join(data_dir, file)
                # Read the CSV file into a DataFrame
                with open(file_path, 'r') as f:
                    first_line = f.readline()
                if ";" in first_line:
                    df = pd.read_csv(file_path, sep=';')
                    # Write back to CSV file with comma separator
                    df.to_csv(file_path, sep=',', index=False)

    def drop_rows(self, path):
        """
        Drops fully blank rows from CSV files in the specified directory or file.

        Args:
            path (str): Path to the directory containing CSV files or path to a single CSV file.
        """
        # Check if the path is a directory or a file
        if os.path.isdir(path):
            # Get a list of all files in the directory
            files = os.listdir(path)
        else:
            files = [path]

        # Iterate through each file
        for file in files:
            # Check if the file is a CSV file
            if file.endswith(".csv"):
                file_path = os.path.join(path, file)
                # Read the CSV file into a DataFrame
                df = pd.read_csv(file_path)
                # Drop fully blank rows
                df.dropna(how='all', inplace=True)
                # Write back to CSV file
                df.to_csv(file_path, sep=',', index=False)