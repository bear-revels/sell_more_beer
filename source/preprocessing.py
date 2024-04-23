import os
import pandas as pd
import sqlite3

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

    def transpose(self, csv_file):
        """
        Transposes the CSV file and drops rows with NaN values in the 'Region' column.

        Args:
            csv_file (str): Path to the CSV file.
        """
        # Load the CSV into a DataFrame
        df = pd.read_csv(csv_file, sep=',', header=None)

        # Check if the DataFrame already has a 'Region' column
        if 'Region' in df.iloc[:, 1].values:
            # Assuming it's already transposed, return the DataFrame as it is
            return df.drop(columns=[0]).dropna(subset=[1])

        # Transpose the DataFrame
        transposed_df = df.transpose()

        # Set the column names to id, Region, and Country
        transposed_df.columns = transposed_df.iloc[0]
        transposed_df = transposed_df.drop(0)  # Remove the initial row of column indices

        # Drop the "Country" column
        transposed_df = transposed_df.drop(columns=["Country"])

        # Drop rows with NaN values in the "Region" column
        transposed_df = transposed_df.dropna(subset=["Region"])

        # Reset the index
        transposed_df.reset_index(drop=True, inplace=True)

        # Write the transposed DataFrame back to the same file
        transposed_df.to_csv(csv_file, sep=',', index=False)

        return transposed_df
    
    def format_date(self, data_dir):
        """
        Converts the 'Year_date' column in CSV files in the specified directory to a datetime column with format 'DD/MM/YYYY',
        adjusts the year to match the 'Year' or 'Year_text' column if they don't match, and renames 'Year_text' to 'Year'.

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
                df = pd.read_csv(file_path)
                
                # Locate 'Year_text' column and ensure it's a whole number numerical type
                if 'Year_text' in df.columns:
                    df['Year_text'] = pd.to_numeric(df['Year_text'], errors='coerce').astype(pd.Int64Dtype())  # Convert to whole number
                    df = df.rename(columns={'Year_text': 'Year'})  # Rename 'Year_text' to 'Year'
                    
                # Locate 'Year' column and ensure it's a whole number numerical type
                if 'Year' in df.columns:
                    df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype(pd.Int64Dtype())  # Convert to whole number
                    
                # Check if 'Year_date' column exists
                if 'Year_date' in df.columns:
                    # Convert 'Year_date' column to datetime
                    df['Year_date'] = pd.to_datetime(df['Year_date'], errors='coerce')
                    
                    # Check if 'Year' column exists
                    if 'Year' in df.columns:
                        # Adjust 'Year_date' to match 'Year' column if they don't match
                        df['Year_date'] = df.apply(lambda row: row['Year_date'].replace(year=row['Year'])
                                                    if not pd.isnull(row['Year']) and
                                                    not pd.isnull(row['Year_date']) and
                                                    row['Year_date'].year != row['Year']
                                                    else row['Year_date'], axis=1)
                # Write back to CSV file
                df.to_csv(file_path, index=False)

    def int_conversion(self, data_dir):
        """
        Converts float type columns in CSV files in the specified directory to integers with no decimals except for the 'Volume' column,
        which is rounded to two decimals, and object type 'Volume' columns are converted to integers and rounded to two decimals.

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
                df = pd.read_csv(file_path)

                # Iterate through each column
                for col in df.columns:
                    # Check if the column is float type and not the 'Volume' column
                    if df[col].dtype == 'float' and col != 'Volume':
                        # Convert the values to integers without decimals
                        df[col] = df[col].astype(int)
                    # Check if the column is the float type 'Volume' column
                    elif col == 'Volume' and df[col].dtype == 'float':
                        # Round the values to two decimals
                        df[col] = df[col].round(2)
                    # Check if the column is object type and named 'Volume'
                    elif col == 'Volume' and df[col].dtype == 'object':
                        # Convert the values to integers, round to two decimals, and then back to integers
                        df[col] = df[col].str.replace(',', '.').astype(float).round(2).astype(int)

                # Write back to CSV file with comma as the decimal separator
                df.to_csv(file_path, index=False, decimal=',')

    def create_database(self, csv_dir, db_path):
        """
        Create a SQLite database and import data from CSV files into tables.

        Args:
            csv_dir (str): Path to the directory containing CSV files.
            db_path (str): Path to the SQLite database file to be created.
        """
        # Create a connection to the SQLite database
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        # Get a list of all CSV files in the directory
        csv_files = [file for file in os.listdir(csv_dir) if file.endswith('.csv')]

        # Iterate through each CSV file and import its data into a table
        for file in csv_files:
            # Extract table name from file name (remove '.csv' extension)
            table_name = os.path.splitext(file)[0]
            # Read CSV file into a DataFrame
            df = pd.read_csv(os.path.join(csv_dir, file))
            # Write DataFrame to SQLite database as a table
            df.to_sql(table_name, conn, if_exists='replace', index=False)

        # Define foreign key constraints
        c.execute('''PRAGMA foreign_keys = ON''')

        # Define foreign key constraints between tables
        c.execute('''CREATE TABLE IF NOT EXISTS Channel_Volume (
                        Category TEXT,
                        FOREIGN KEY (Category) REFERENCES Categories(Category)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS Market_Sizes (
                        Subcategory TEXT,
                        Location TEXT,
                        FOREIGN KEY (Subcategory) REFERENCES Subcategories(Subcategory),
                        FOREIGN KEY (Location) REFERENCES Locations(Location)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS Company_Share_GBO_unit (
                        Subcategory_ID INTEGER,
                        Location TEXT,
                        FOREIGN KEY (Subcategory_ID) REFERENCES Subcategories(id),
                        FOREIGN KEY (Location) REFERENCES Locations(Location)
                    )''')

        # Commit changes and close connection
        conn.commit()
        conn.close()
