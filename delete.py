import os
import shutil
import sqlite3
from datetime import datetime, timedelta
import logging

def move_processed_folders_to_deleted():
    try:
        # Constants
        completed_folder = "completed/"
        deleted_folder = "deleted/"

        # Connect to SQLite database
        conn = sqlite3.connect('conversion.db')
        cursor = conn.cursor()

        # Get current date and time
        current_datetime = datetime.now()

        # Calculate the threshold datetime (24 hours ago)
        threshold_datetime = current_datetime - timedelta(days=1)

        # Get the list of processed folders that are older than 24 hours
        cursor.execute('''SELECT DISTINCT source_file, DATE(chunk_created_datetime) 
                          FROM ProcessedFile 
                          WHERE DATE(chunk_created_datetime) <= ? 
                          AND status = ?''', 
                       (threshold_datetime.date(), 'Completed'))
        folders_to_delete = cursor.fetchall()

        # If no folders need to be deleted, exit the function
        if not folders_to_delete:
            logging.info("No folders to delete. Exiting the function.")
            return

        # Path to deleted folder
        deleted_folder_path = os.path.join(deleted_folder)

        # Create the deleted folder if it doesn't exist
        os.makedirs(deleted_folder_path, exist_ok=True)
        # Move processed folders to deleted folder
        for source_file, download_datetime in folders_to_delete:
                # Path to the completed folder
                date_object = datetime.strptime(download_datetime, '%Y-%m-%d')  
                completed_folder_path = os.path.join(completed_folder, date_object.strftime("%y%m%d"))  
                # Move the folder to the deleted folder
                destination_path = os.path.join(deleted_folder_path)
                shutil.move(completed_folder_path, destination_path)
                logging.info(f"Moved folder {completed_folder_path} to deleted folder.")

        # If all folders are moved successfully, update the status in the SourceFile table to "deleted"
        cursor.execute('''UPDATE SourceFile 
                          SET status=?, updated_datetime=? 
                          WHERE source_file IN (SELECT DISTINCT source_file FROM ProcessedFile 
                                                WHERE DATE(chunk_created_datetime) <= ? 
                                                AND status = ?)''', 
                       ('Deleted', current_datetime.strftime("%Y-%m-%d %H:%M:%S"), threshold_datetime.date(), 'Completed'))
        conn.commit()
        logging.info("Updated status in SourceFile table to 'Deleted'.")

        # Close the database connection
        conn.close()
        logging.info("Processed folders moved to deleted folder successfully.")
    except Exception as e:
        logging.error(f"Error moving processed folders to deleted folder: {str(e)}")

# Set up logging
log_file = 'logs/file_convert.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Call the function to move processed folders to the deleted folder
move_processed_folders_to_deleted()
