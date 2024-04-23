import sqlite3
import os
import pandas as pd

def create_database(csv_dir, db_path):
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

# Specify the path to the directory containing CSV files
csv_directory = "data"
# Specify the path to the SQLite database file to be created
database_path = "data/sell_more_beer.db"

# Create the SQLite database and import data from CSV files
create_database(csv_directory, database_path)