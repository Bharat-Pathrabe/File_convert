import os
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import ssl
import logging
import base64

def get_file_count(db_file, table, status_column=None, status=None, date_column=None, date=None):
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Query to get file count based on status and/or date
        if status_column and status and date_column and date:
            query = f"SELECT COUNT(*) FROM {table} WHERE {status_column} = ? AND substr({date_column}, 1, 10) = ?"
            cursor.execute(query, (status, date))
        elif status_column and status:
            query = f"SELECT COUNT(*) FROM {table} WHERE {status_column} = ?"
            cursor.execute(query, (status,))
        elif date_column and date:
            query = f"SELECT COUNT(*) FROM {table} WHERE substr({date_column}, 1, 10) = ?"
            cursor.execute(query, (date,))
        else:
            query = f"SELECT COUNT(*) FROM {table}"
            cursor.execute(query)
            
        count = cursor.fetchone()[0]

        # Close the database connection
        conn.close()

        return count
    except Exception as e:
        logging.error(f"Error getting file count: {str(e)}")
        return None

def send_email(sender_email, receiver_email, cc_emails, password, subject, body, attachment_paths=None):
    try:
        # Setup the MIME
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = receiver_email
        message['Cc'] = ", ".join(cc_emails) if cc_emails else ""
        message['Subject'] = subject

        # Add body to email
        message.attach(MIMEText(body, 'plain'))

        if attachment_paths:
            for attachment_path in attachment_paths:
                # Open file in binary mode
                with open(attachment_path, 'rb') as attachment:
                    # Add file as application/octet-stream
                    # Email client can usually download this automatically as attachment
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(attachment.read())

                # Encode file in ASCII characters to send by email    
                encoders.encode_base64(part)

                # Add header as key/value pair to attachment part
                part.add_header('Content-Disposition', f'attachment; filename= {os.path.basename(attachment_path)}')

                # Add attachment to message
                message.attach(part)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, [receiver_email] + cc_emails, message.as_string())

        logging.info("Email sent successfully")
    except Exception as e:
        logging.error(f"Error sending email: {str(e)}")

def send_daily_status_email():
    try:
        # Database file
        db_file = "conversion.db"

        # Get today's date
        today_date = datetime.now().strftime("%Y-%m-%d")

        # Get total file count
        total_files = get_file_count(db_file, "SourceFile", date_column="updated_datetime", date=today_date)

        # Get counts for processed, failed, and deleted files
        processed_files = get_file_count(db_file, "SourceFile", status_column="status", status="Done", date_column="updated_datetime", date=today_date)
        failed_files = get_file_count(db_file, "SourceFile", status_column="status", status="Failed", date_column="updated_datetime", date=today_date)
        deleted_files = get_file_count(db_file, "SourceFile", status_column="status", status="Deleted", date_column="updated_datetime", date=today_date)

        # Function to decode Base64 encoded variables
        def decode_variable(encoded_variable):
            decoded_variable = base64.b64decode(encoded_variable.encode()).decode()
            return decoded_variable

        # Email content
        sender_email = decode_variable(os.environ.get('EMAIL_SENDER'))
        receiver_email = decode_variable(os.environ.get('EMAIL_RECEIVER'))
        cc_emails = decode_variable(os.environ.get('EMAIL_CC', '')).split(',') # Split multiple emails by comma
        password = decode_variable(os.environ.get('EMAIL_PASSWORD'))
        subject = "Daily Status Report"
        body = f"Date: {today_date}\nTotal files: {total_files}\nProcessed files: {processed_files}\nFailed files: {failed_files}\nDeleted files: {deleted_files}"
        attachment_paths = [
            f"reports/Report_{today_date}.xlsx",  
            "logs/file_convert.log"  
        ]

        # Send email
        send_email(sender_email, receiver_email, cc_emails, password, subject, body, attachment_paths)
    except Exception as e:
        logging.error(f"Error sending daily status email: {str(e)}")

if __name__ == "__main__":
    # Set up logging
    log_file = 'logs/file_convert.log'
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    send_daily_status_email()
