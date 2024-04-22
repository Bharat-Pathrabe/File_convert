import os
import shutil
import sqlite3
from datetime import datetime
import logging

def move_files_to_completed():
    try:
        # Constants
        processing_folder = "processing/"
        completed_folder = "completed/"
        input_folder = "input/"

        # Get current date
        today_date = datetime.now().strftime("%y%m%d")

        # Path to today's processing folder
        processing_folder_today = os.path.join(processing_folder, today_date)
        if not os.path.exists(processing_folder_today):
            logging.info(f"No files found for today's date {today_date} in the processing folder.")
            return
        
        # Path to today's completed folder
        completed_folder_today = os.path.join(completed_folder, today_date)
        os.makedirs(completed_folder_today, exist_ok=True)

        # Connect to SQLite database
        conn = sqlite3.connect('conversion.db')
        cursor = conn.cursor()

        # Get list of files in today's processing folder
        files = os.listdir(processing_folder_today)

        # Iterate through each file
        for file_name in files:
            # Move file to completed folder
            source_path = os.path.join(processing_folder_today, file_name)
            destination_path = os.path.join(completed_folder_today, file_name)
            logging.info(f"Moving file {file_name} to completed folder...")

            # Check if source file exists
            if os.path.exists(source_path):
                try:
                    # Move file
                    shutil.move(source_path, destination_path)
                    logging.info(f"File {file_name} moved to completed folder.")

                    # Update SourceFile table
                    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    cursor.execute('''UPDATE SourceFile 
                                      SET status=?, updated_datetime=? 
                                      WHERE source_file=?''', 
                                   ('Done', now, file_name + ".wav"))
                    conn.commit()

                    # Path to chunks folder for the current file
                    file_folder = os.path.join(completed_folder_today, file_name)
                    chunks_folder = os.path.join(file_folder, "Chunks")
                    
                    # Check if chunks folder exists
                    if os.path.exists(chunks_folder):
                        # Get list of files in the chunks folder
                        chunks_files = os.listdir(chunks_folder)
                        
                        # Iterate through each chunk file
                        for chunk_file in chunks_files:
                            try:
                                # Update status and updated_datetime columns in ProcessedFile table for chunk file
                                cursor.execute('''UPDATE ProcessedFile 
                                                SET status=?, updated_datetime=? 
                                                WHERE local_file=?''', 
                                                ('Completed', now, chunk_file))
                                conn.commit()
                            except Exception as e:
                                logging.error(f"Error updating ProcessedFile table for chunk {chunk_file}: {str(e)}")
                except Exception as e:
                    logging.error(f"Error moving file {file_name}: {str(e)}")
            else:
                logging.error(f"Source file {source_path} does not exist.")

        # Close the database connection
        conn.close()
        logging.info("All files processed successfully.")
    except Exception as e:
        logging.error(f"Error moving files to completed folder: {str(e)}")

def delete_input_folder_contents():
    try:
        # Constants
        input_folder = "input/"

        # Get current date
        today_date = datetime.now().strftime("%y%m%d")

        # Path to today's input folder
        input_folder_today = os.path.join(input_folder, today_date)

        # Check if the input folder exists
        if os.path.exists(input_folder_today):
            # Iterate through files and directories in the input folder
            for item in os.listdir(input_folder_today):
                item_path = os.path.join(input_folder_today, item)
                if os.path.isfile(item_path):
                    # Delete file
                    os.remove(item_path)
                    logging.info(f"File '{item}' deleted from input folder.")
                elif os.path.isdir(item_path):
                    # Delete directory
                    shutil.rmtree(item_path)
                    logging.info(f"Directory '{item}' deleted from input folder.")
        else:
            logging.info(f"No files found for today's date {today_date} in the input folder.")
    except Exception as e:
        logging.error(f"Error deleting input folder contents: {str(e)}")

# Configure logging
log_file = 'logs/file_convert.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Call the function to move files to completed folder and update database
move_files_to_completed()

# Call the function to delete everything from the input folder
delete_input_folder_contents()
