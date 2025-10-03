import os
import pathlib
from dotenv import load_dotenv
import oracledb
import pandas as pd
from datetime import date
import logging
import sys
import smtplib
from email.message import EmailMessage

# Fetch date as at and format string
today = date.today()
today_string = today.strftime("%Y%m%d")

# Define the root directory of the main file
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Join the root directory and the folder + files
ENV_PATH = os.path.join(ROOT_DIR, "config", ".env")
SQL_PATH = os.path.join(ROOT_DIR, "sql", "ses_query.sql")
OUTPUT_PATH = os.path.join(ROOT_DIR, "output")
LOG_PATH = os.path.join(ROOT_DIR, "logs")
LOG_FILE = os.path.join(LOG_PATH, f"{today_string}_LOG_FILE.txt")

# Check log folder exists
os.makedirs(LOG_PATH, exist_ok=True)


# Set up logging file
logging.basicConfig(filename=LOG_FILE, filemode='a', level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logging.info("Job started")

# Load .env file
load_dotenv(ENV_PATH)
logging.info("Environment loaded")

# Pulll .env data for login & SMTP credentials
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
service = os.getenv("DB_SERVICE")
username = os.getenv("DB_USER")
password = os.getenv("DB_PASS")

smtp_server = os.getenv("SMTP_SERVER")
smtp_port = os.getenv("SMTP_PORT")
smtp_from = os.getenv("SMTP_FROM")
smtp_to = os.getenv("SMTP_TO", "")

# split on commas, strip spaces, and filter out empties
recipient_list = [addr.strip() for addr in smtp_to.split(" ") if addr.strip()]

# email send function
def send_email(subject, body, recipient_list, attachment_path=None):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = smtp_from
    msg['To'] = recipient_list
    msg.set_content(body)
    logging.info(f"Email compose start: {len(recipient_list)} recipient(s)")

        # If there's an attachment, add it to the email
    if attachment_path:
        try:
            filename = os.path.basename(attachment_path)
            with open(attachment_path, "rb") as f:
                data = f.read()
            msg.add_attachment(
                data,
                maintype="application",
                subtype="octet-stream",
                filename=filename
            )
            logging.info(f"Attachment added: {filename} ({len(data)} bytes)")
        except Exception as e:
            logging.exception(f"Failed to attach file {attachment_path}")

    try:
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as s:
            s.send_message(msg)
            logging.info("Email send success")
            return True
    except Exception as e:
        print("Send failed. ")
        logging.exception("Email send failed")
        print(type(e).__name__, str(e))    
        return False
    


dsn = host + ":" + port + "/" + service
logging.info(f"DSN built: {dsn}")

# Db connection function
def db_conn(username, password, dsn):
    try:
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        print("Connection Successful.")
        logging.info("DB Connection Successful")
        return connection
    except oracledb.Error as e:
        print(f"Error connecting to Oracle database: {e}")
        return None

def write_csv(df):
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    TEMP_FILE = os.path.join(OUTPUT_PATH, f"_{today_string}_tmp_QH_ServiceEntry.csv")
    FINAL_FILE = os.path.join(OUTPUT_PATH, f"{today_string}_QH_ServiceEntry.csv")
    df.to_csv(TEMP_FILE, index=False)
    os.replace(TEMP_FILE, FINAL_FILE)
    print(f"Wrote {len(df)} rows ->")
    print(f"Wrote to {FINAL_FILE}")
    logging.info(f"Wrote {len(df)} rows to {FINAL_FILE}")
    return FINAL_FILE
    
conn = db_conn(username, password, dsn)
cur = None
success = False

# Main Runner
try:
    if conn:
        cur = conn.cursor()
        with open(SQL_PATH, "r", encoding="utf-8") as f:
            file_content = f.read()
        df = pd.read_sql_query(file_content, conn)
        logging.info(f"Query returned {df.shape[0]} rows and {df.shape[1]} columns")
        # Write DF to CSV
        output_path = write_csv(df)
        success = True
        # Print updates
        print("Shape", df.shape)
        subject = f"Service Entry Report — {today_string}"
        body = f"Rows: {len(df)}\nFile created successfully."
        ok = send_email(subject, body, recipient_list, attachment_path=output_path)
except oracledb.Error as e:
    print(f"Error executing query {e}")
    logging.exception("Job failed")
finally:
    if cur:
        cur.close()
        print("Cursor closed.")
    if conn:
        conn.close()
        print("Connection closed.")

logging.shutdown()
sys.exit(0 if success else 1)
