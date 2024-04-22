import os
import shutil
from datetime import datetime
import subprocess
import sqlite3
import logging

# Set up logging
log_file = 'logs/file_convert.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

input_folder = "input/"
processing_folder = "processing/"

def create_chunks(input_file, output_folder, chunk_duration=10):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect('conversion.db')
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute('''CREATE TABLE IF NOT EXISTS ProcessedFile 
                          (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                          local_file TEXT, 
                          source_file TEXT, 
                          status TEXT, 
                          chunk_created_datetime TEXT,
                          updated_datetime TEXT)''')
        conn.commit()

        # Update database with processing status and start datetime
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Extract the filename without the path
        file_name = os.path.basename(input_file)
        cursor.execute('''SELECT * FROM SourceFile WHERE source_file=?''', (file_name,))
        row = cursor.fetchone()
        if row:
            # Entry exists, update it
            cursor.execute('''UPDATE SourceFile 
                              SET status=?, updated_datetime=? 
                              WHERE source_file=?''', 
                           ('processing', start_datetime, file_name))
        else:
            # Entry doesn't exist, create it
            cursor.execute('''INSERT INTO SourceFile (source_file, status, updated_datetime) 
                              VALUES (?, ?, ?)''', (file_name, 'processing', start_datetime))
        conn.commit()

        # Create folder for the input file
        today_date = datetime.now().strftime("%y%m%d")
        file_name_no_extension = os.path.splitext(file_name)[0]
        file_folder = os.path.join(output_folder, today_date, file_name_no_extension)
        os.makedirs(file_folder, exist_ok=True)

        # Create original folder and copy the original WAV file
        original_folder = os.path.join(file_folder, "original")
        os.makedirs(original_folder, exist_ok=True)
        original_file_destination = os.path.join(original_folder, file_name)
        shutil.copy(input_file, original_file_destination)

        # Convert folder
        convert_folder = os.path.join(file_folder, "Convert")
        os.makedirs(convert_folder, exist_ok=True)

        # Chunks folder
        chunks_folder = os.path.join(file_folder, "Chunks")
        os.makedirs(chunks_folder, exist_ok=True)

        # Check if WMA file already exists
        output_file_wma = os.path.join(convert_folder, f"{file_name_no_extension}.wma")
        if os.path.exists(output_file_wma):
            logging.info(f"Skipping conversion for '{file_name}', WMA file already exists.")
            # Update status of the source file to 'processed'
            end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('''UPDATE SourceFile 
                              SET status=?, updated_datetime=? 
                              WHERE source_file=?''', 
                           ('Completed', end_datetime, file_name))
            conn.commit()
            conn.close()
            return

        # Convert WAV to WMA using FFmpeg
        logging.info(f"Converting '{file_name}' to WMA format.")
        subprocess.run(['ffmpeg', '-y', '-i', input_file, '-acodec', 'wmav2', output_file_wma], check=True)

        # Extract audio duration
        duration = subprocess.check_output(['ffprobe', '-i', output_file_wma, '-show_entries', 'format=duration', '-v', 'quiet', '-of', 'csv=%s' % ("p=0")])
        duration = float(duration.strip())

        # Calculate number of chunks
        total_chunks = int(duration / chunk_duration)
        logging.info(f"Creating {total_chunks} chunks for '{file_name}'.")

        # Iterate through chunks
        for i in range(total_chunks):
            # Calculate start and end time for the chunk
            start_time = i * chunk_duration
            end_time = start_time + chunk_duration

            # Create filename for the chunk
            chunk_name = f"{file_name_no_extension}_{start_time}_{end_time}.wma"
            output_file_chunk = os.path.join(chunks_folder, chunk_name)

            # Extract the chunk using FFmpeg
            logging.info(f"Extracting chunk {i + 1}/{total_chunks} for '{file_name}'.")
            subprocess.run(['ffmpeg', '-y', '-i', output_file_wma, '-acodec', 'copy', '-ss', str(start_time), '-to', str(end_time), output_file_chunk], check=True)

            # Update ProcessedFile table
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute('''INSERT INTO ProcessedFile 
                              (local_file, source_file, status, chunk_created_datetime, updated_datetime) 
                              VALUES (?, ?, ?, ?, ?)''', 
                           (os.path.basename(output_file_chunk), file_name, 'Processing', now, now))
            conn.commit()

        # Update status of the source file to 'Completed'
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''UPDATE SourceFile 
                          SET status=?, updated_datetime=? 
                          WHERE source_file=?''', 
                       ('Completed', end_datetime, file_name))
        conn.commit()

        # Close the database connection
        conn.close()
    except Exception as e:
        logging.error(f"Error processing file {input_file}: {str(e)}")
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute('''UPDATE SourceFile 
                          SET status=?, updated_datetime=? 
                          WHERE source_file=?''', 
                       ('Failed', end_datetime, file_name))
        conn.commit()

        try:
            error_file_folder = os.path.join(processing_folder, today_date, file_name_no_extension)
            failed_folder = os.path.join(os.getcwd(), "failed")
            if not os.path.exists(failed_folder):
                os.makedirs(failed_folder)
            if os.path.exists(error_file_folder):
                shutil.move(error_file_folder, failed_folder)
                logging.info(f"Error file '{file_name}' moved to 'failed' folder.")
        except Exception as e:
            logging.error(f"Error moving error file to 'Failed' folder: {str(e)}")
        

def process_audio_files():
    try:
        # Get current date
        today_date = datetime.now().strftime("%y%m%d")

        # Get list of files in today's date folder in the input directory
        input_folder_today = os.path.join(input_folder, today_date)
        if not os.path.exists(input_folder_today):
            logging.info(f"No files found for today's date {today_date} in the input folder.")
            return
        
        files = os.listdir(input_folder_today)

        # Iterate through each file
        for file in files:
            # Check if it's a WAV file
            if file.endswith(".wav"):
                # Create chunks from the audio file
                create_chunks(os.path.join(input_folder_today, file), processing_folder)
    except Exception as e:
        logging.error(f"Error processing audio files: {str(e)}")

# Call the function to process audio files
process_audio_files()
