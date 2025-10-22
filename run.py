import os
import pathlib
from dotenv import load_dotenv
import oracledb
import pandas as pd
from datetime import datetime, timedelta, date
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
mode = os.getenv("RUN_MODE", "DAILY").upper()

YEAR_START_MONTH = int(os.getenc("YEAR_START_MONTH", "1"))

# split on commas, strip spaces, and filter out empties
recipient_list = [addr.strip() for addr in smtp_to.split(" ") if addr.strip()]

def what_is_due(today: date):
    weekly_due = (today.weekday() == 0) # Monday
    monthly_due = (today.day == 1) # 1st of month
    return weekly_due, monthly_due

# Compute start and end date
def compute_dates(mode):
    """
    Computes the start and end dates for a given mode.

    Args:
        mode (str): The desired date range mode ('DAILY', 'WEEKLY', 'MONTHLY').

    Returns:
        tuple: A tuple containing the start and end datetime objects,
               both normalized to midnight.
    """
    # Get the current local date and normalize to midnight.
    # Note: Using `datetime.now()` assumes the system's timezone is correct.
    # For robust timezone handling, a library like `pytz` would be needed.

    now = datetime.now()
    midnight_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if mode.upper() == "DAILY":
        start_date = midnight_today
        end_date = midnight_today + timedelta(days=1)
    elif mode.upper() == "WEEKLY":
        start_date = midnight_today - timedelta(days=7)
        end_date = midnight_today
    elif mode.upper() == "MONTHLY":
        first_of_this_month = midnight_today.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        start_date = last_month_end.replace(day=1)
        end_date = first_of_this_month
    elif mode.upper() == "MONTHLY_MTD":
        start_date = midnight_today.replace(day=1)
        end_date = midnight_today + timedelta(days=1)
    else:
        raise ValueError("Invalid mode. Choose from 'DAILY', 'WEEKLY', or 'MONTHLY'.")
    
    logging.info(f"window mode: {start_date}, {end_date}")
    return start_date, end_date

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
    today = date.today()
    weekly_due, monthly_due = what_is_due(today)
    logging.info(f"Due today = Weekly: {weekly_due}, Monthly: {monthly_due}")
    if not weekly_due and not monthly_due:
        logging.info("Nothing due today")
        sys.exit(0)
    if conn:
        cur = conn.cursor()
        with open(SQL_PATH, "r", encoding="utf-8") as f:
            file_content = f.read()
        # Compute date range
        start_dt, end_dt = compute_dates(mode)
        params = {"start_date_ts": start_dt, "end_date_ts": end_dt}
        df = pd.read_sql_query(file_content, conn, params=params)
        logging.info(f"Window ({mode}): {start_dt} → {end_dt} | Rows: {len(df):,}")
        logging.info(f"Query returned {df.shape[0]} rows and {df.shape[1]} columns")
        # Write DF to CSV
        output_path = write_csv(df)
        success = True
        # Print updates
        print("Shape", df.shape)
        subject = f"Service Entry Report — {today_string}"
        body = f"""\
        Total Rows: {len(df)}

        The Service Entry Sheet report has been generated successfully, and the file is attached.

        Please note:
        - This report was generated automatically.
        - For any questions or concerns regarding the process, completeness or data accuracy, contact Jordan or Jeet at: ap_operations@health.qld.gov.au

        Thank you,
        AP Operations - Queensland Health
        """
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
