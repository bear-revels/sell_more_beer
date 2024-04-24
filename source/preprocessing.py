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
                        # Print column name for debugging
                        print(f"Checking column '{col}' for non-finite values...")
                        # Convert the values to integers without decimals after handling non-finite values
                        df[col] = df[col].astype(int).round(0)

                # Write back to CSV file with comma as the decimal separator
                df.to_csv(file_path, index=False, decimal=',')

    def standardize_units(self, data_dir):
        """
        Process volume units in CSV files in the specified directory and calculate volume in litres.

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

                # Check if 'Unit' and 'Volume' columns exist
                if 'Unit' in df.columns and 'Volume' in df.columns:
                    # Convert 'Volume' column to float
                    df['Volume'] = df['Volume'].str.replace(',', '.').astype(float)

                    # Process volume units only when 'Unit' column is either '000 litres' or 'million litres'
                    if ('000 litres' in df['Unit'].values) or ('million litres' in df['Unit'].values):
                        def calculate_volume(row):
                            if row['Unit'] == 'million litres':
                                return round(row['Volume'] * 1000000)
                            elif row['Unit'] == '000 litres':
                                return round(row['Volume'] * 1000)
                            else:
                                return row['Volume']

                        df['Volume_Litres'] = df.apply(calculate_volume, axis=1)

                        # Write back to CSV file with updated volume in litres
                        df.to_csv(file_path, index=False, decimal=',')

    def fix_string_columns(self, data_dir):
        """
        Fix string columns by capitalizing the first letter of each word and making the rest lowercase.

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
                    # Check if the column data type is object (string)
                    if df[col].dtype == 'object':
                        # Capitalize the first letter of each word and make the rest lowercase
                        df[col] = df[col].apply(lambda x: ' '.join([word.capitalize() for word in x.lower().split()]))
                # Write the updated DataFrame back to the CSV file
                df.to_csv(file_path, index=False)

    def drop_column(self, path, column_name):
        """
        Drops the specified column from CSV files in the specified directory or file.

        Args:
            path (str): Path to the directory containing CSV files or path to a single CSV file.
            column_name (str): Name of the column to drop.
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
                # Drop the specified column if it exists
                if column_name in df.columns:
                    df.drop(columns=[column_name], inplace=True)
                    # Write back to CSV file
                    df.to_csv(file_path, sep=',', index=False)

    def rename_column(self, path, old_column_name, new_column_name):
        """
        Renames the specified column in CSV files in the specified directory or file.

        Args:
            path (str): Path to the directory containing CSV files or path to a single CSV file.
            old_column_name (str): Current name of the column to rename.
            new_column_name (str): New name for the column.
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
                # Rename the specified column if it exists
                if old_column_name in df.columns:
                    df.rename(columns={old_column_name: new_column_name}, inplace=True)
                    # Write back to CSV file
                    df.to_csv(file_path, sep=',', index=False)
    
    def merge_dim_tables(self, data_dir):
        """
        Merge Subcategories.csv with Company_Share_GBO_Unit.csv and Market_Sizes.csv.

        Args:
            data_dir (str): Path to the directory containing CSV files.
        """
        # Define file paths for all CSV files
        subcategories_file = os.path.join(data_dir, "Subcategories.csv")
        company_share_file = os.path.join(data_dir, "Company_Share_GBO_Unit.csv")
        market_sizes_file = os.path.join(data_dir, "Market_Sizes.csv")
        categories_file = os.path.join(data_dir, "Categories.csv")
        channel_volume_file = os.path.join(data_dir, "Channel_Volume.csv")

        # Read CSV files into DataFrames
        subcategories_df = pd.read_csv(subcategories_file)
        categories_df = pd.read_csv(categories_file)
        company_share_df = pd.read_csv(company_share_file)
        market_sizes_df = pd.read_csv(market_sizes_file)
        channel_volume_df = pd.read_csv(channel_volume_file)

        # Merge with Company_Share_GBO_Unit.csv if necessary
        if 'Subcategory_Name' not in company_share_df:
            company_merge = pd.merge(company_share_df, subcategories_df[['id', 'Category', 'Name']], left_on='Subcategory_ID', right_on='id', how='left')
            company_merge.drop(columns=['id'], inplace=True)
            company_merge.rename(columns={'Category': 'Category_ID', 'Name': 'Subcategory_Name'}, inplace=True)
            company_merge = pd.merge(company_merge, categories_df[['id', 'Name']], left_on='Category_ID', right_on='id', how='left')
            company_merge.drop(columns=['id'], inplace=True)
            company_merge.rename(columns={'Name': 'Category_Name'}, inplace=True)
            company_merge.to_csv(company_share_file, index=False)

        # Merge with Market_Sizes.csv if necessary
        if 'Subcategory_Name' not in market_sizes_df:
            market_merge = pd.merge(market_sizes_df, subcategories_df[['id', 'Category', 'Name']], left_on='Subcategory', right_on='id', how='left')
            market_merge.drop(columns=['id'], inplace=True)
            market_merge.rename(columns={'Category': 'Category_ID', 'Name': 'Subcategory_Name', 'Subcategory': 'Subcategory_ID'}, inplace=True)
            market_merge = pd.merge(market_merge, categories_df[['id', 'Name']], left_on='Category_ID', right_on='id', how='left')
            market_merge.drop(columns=['id'], inplace=True)
            market_merge.rename(columns={'Name': 'Category_Name'}, inplace=True)
            market_merge.to_csv(market_sizes_file, index=False)

        # Update Channel_Volume.csv if necessary
        if 'Subcategory_Name' not in channel_volume_df:
            channel_volume_df['Subcategory_Name'] = channel_volume_df['Category']
            channel_volume_df.rename(columns={'Category': 'Category_Name'}, inplace=True)
            channel_volume_df.to_csv(channel_volume_file, index=False)

    def create_date_table(self, data_dir):
        """
        Create a date dimension table based on the range of dates present in the 'Year_date' columns of the CSV files.

        Args:
            data_dir (str): Path to the directory containing CSV files.
        """
        # Initialize an empty DataFrame to store the date dimension
        date_dimension = pd.DataFrame(columns=["Date"])

        # Get a list of all files in the data directory
        files = os.listdir(data_dir)

        # Iterate through each file
        for file in files:
            # Check if the file is a CSV file
            if file.endswith(".csv"):
                file_path = os.path.join(data_dir, file)
                # Read the CSV file into a DataFrame
                df = pd.read_csv(file_path)
                # Check if 'Date' column exists
                if 'Date' in df.columns:
                    # Get the range of dates present in the 'Date' column
                    min_date = pd.to_datetime(df['Date']).min()
                    max_date = pd.to_datetime(df['Date']).max()
                    # Generate a date range between min and max date
                    dates = pd.date_range(start=min_date, end=max_date, freq='D')
                    # Append dates to the date_dimension DataFrame
                    date_dimension = pd.concat([date_dimension, pd.DataFrame({"Date": dates})], ignore_index=True)

        # Drop duplicate dates
        date_dimension.drop_duplicates(subset=["Date"], inplace=True)

        # Extract year, quarter, month, day, and day of week from the 'Date' column
        date_dimension['Year'] = date_dimension['Date'].dt.year
        date_dimension['Quarter_Num'] = date_dimension['Date'].dt.quarter
        date_dimension['Quarter_Name'] = "Q" + date_dimension['Date'].dt.quarter.astype(str)
        date_dimension['Month_Num'] = date_dimension['Date'].dt.month
        date_dimension['Month_Name'] = date_dimension['Date'].dt.month_name()
        date_dimension['Month_MMM'] = date_dimension['Date'].dt.strftime("%b").str.upper()
        date_dimension['WeekOfYear_Num'] = date_dimension['Date'].dt.isocalendar().week
        date_dimension['DayOfMonth_Num'] = date_dimension['Date'].dt.day
        date_dimension['DayOfWeek_Num'] = date_dimension['Date'].dt.dayofweek
        date_dimension['DayOfWeek_Name'] = date_dimension['Date'].dt.strftime("%A")
        date_dimension['DayOfWeek_MMM'] = date_dimension['Date'].dt.strftime("%a").str.upper()
        date_dimension['DayOfYear_Num'] = date_dimension['Date'].dt.dayofyear

        # Sort the date_dimension DataFrame
        date_dimension.sort_values(by="Date", inplace=True)

        # Write the date_dimension DataFrame to the output CSV file
        output_file = os.path.join(data_dir, "Date_Table.csv")
        date_dimension.to_csv(output_file, index=False)

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
                        Date_year INTEGER,
                        FOREIGN KEY (Date_year) REFERENCES Date_Table(Date),
                        FOREIGN KEY (Category) REFERENCES Categories(Category)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS Market_Sizes (
                        Subcategory TEXT,
                        Location TEXT,
                        Date_year INTEGER,
                        FOREIGN KEY (Date_year) REFERENCES Date_Table(Date),
                        FOREIGN KEY (Subcategory) REFERENCES Subcategories(Subcategory),
                        FOREIGN KEY (Location) REFERENCES Locations(Location)
                    )''')

        c.execute('''CREATE TABLE IF NOT EXISTS Company_Share_GBO_unit (
                        Subcategory_ID INTEGER,
                        Location TEXT,
                        Date_year INTEGER,
                        FOREIGN KEY (Date_year) REFERENCES Date_Table(Date),
                        FOREIGN KEY (Subcategory_ID) REFERENCES Subcategories(id),
                        FOREIGN KEY (Location) REFERENCES Locations(Location)
                    )''')

        # Commit changes and close connection
        conn.commit()
        conn.close()