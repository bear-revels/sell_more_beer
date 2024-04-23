from source.preprocessing import DataProcessor
import os

def main():
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Specify the data source and paths
    data_dir = os.path.join(current_dir, "data")
    location_file = os.path.join(current_dir, "data/Locations.csv")
    database_path = os.path.join(current_dir, "data/sell_more_beer.db")

    # Assign the DataProcessor class
    processor = DataProcessor()

    # Change delimiter to comma
    processor.comma_delimiter(data_dir)

    # Drop blank rows
    processor.drop_rows(data_dir)

    # Transpose the Location file
    processor.transpose(location_file)

    # Standardize the date format to DD/MM/YYYY
    processor.format_date(data_dir)

    # Convert the Volume column from string to int
    processor.int_conversion(data_dir)

    # Create SQLite database and import data from CSV files
    processor.create_database(data_dir, database_path)

if __name__ == "__main__":
    main()