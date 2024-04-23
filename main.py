from source.preprocessing import DataProcessor
import os

def main():
    # Get the current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Change delimiter to comma and drop blank rows
    data_dir = os.path.join(current_dir, "data")
    processor = DataProcessor()
    processor.comma_delimiter(data_dir)
    processor.drop_rows(data_dir)

if __name__ == "__main__":
    main()