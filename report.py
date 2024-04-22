import os
import sqlite3
import pandas as pd
from datetime import datetime
import logging

def export_source_file_data_to_excel():
    try:
        # Constants
        db_file = "conversion.db"
        reports_folder = "reports/"
        
        # Get current date
        today_date = datetime.now().strftime("%Y-%m-%d")
        
        # Create reports folder if it doesn't exist
        os.makedirs(reports_folder, exist_ok=True)

        # Connect to SQLite database
        conn = sqlite3.connect(db_file)

        # Query to select data from SourceFile table for today's date
        query = f"SELECT * FROM SourceFile WHERE substr(updated_datetime, 1, 10) = '{today_date}'"
        
        # Read data into a DataFrame
        df = pd.read_sql_query(query, conn)

        # Close the database connection
        conn.close()

        # Write DataFrame to Excel file
        excel_file_path = os.path.join(reports_folder, f"Report_{today_date}.xlsx")
        df.to_excel(excel_file_path, index=False)

        logging.info(f"Data exported to Excel file: {excel_file_path}")
    except Exception as e:
        logging.error(f"Error exporting data to Excel file: {str(e)}")

if __name__ == "__main__":
    # Configure logging
    log_file = 'logs/file_convert.log'
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Call the function to export source file data to Excel
    export_source_file_data_to_excel()
