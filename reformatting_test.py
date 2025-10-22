import os
import re
import logging
import sys
import smtplib
from datetime import datetime, timedelta, date, timezone
from email.message import EmailMessage

from dotenv import load_dotenv
import oracledb
import pandas as pd

# =========================================================
# Constants and paths
# =========================================================
TODAY = date.today()
TODAY_STR = TODAY.strftime("%Y%m%d")

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

ENV_PATH = os.path.join(ROOT_DIR, "config", ".env")
SQL_PATH = os.path.join(ROOT_DIR, "sql", "ses_query.sql")
OUTPUT_PATH = os.path.join(ROOT_DIR, "output")
LOG_PATH = os.path.join(ROOT_DIR, "logs")
LOG_FILE = os.path.join(LOG_PATH, f"{TODAY_STR}_LOG_FILE.txt")

os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.info("Job started")

# =========================================================
# Environment
# =========================================================
load_dotenv(ENV_PATH)
logging.info("Environment loaded")

# DB
DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = os.getenv("DB_PORT", "")
DB_SERVICE = os.getenv("DB_SERVICE", "")
DB_USER = os.getenv("DB_USER", "")
DB_PASS = os.getenv("DB_PASS", "")

# SMTP
SMTP_SERVER = os.getenv("SMTP_SERVER", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "25"))
SMTP_FROM = os.getenv("SMTP_FROM", "")
SMTP_TO = os.getenv("SMTP_TO", "")

RUN_MODE = os.getenv("RUN_MODE", "DAILY").upper()

# Allow commas or spaces for recipients
RECIPIENT_LIST = [a for a in (x.strip() for x in re.split(r"[,\s]+", SMTP_TO)) if a]

# =========================================================
# Helpers
# =========================================================
def compute_dates(mode: str) -> tuple[datetime, datetime]:
    """
    Compute [start, end) window at local midnight for the given mode.
    mode: DAILY, WEEKLY, MONTHLY, or MONTHLY_MTD
    """
    now = datetime.now()
    midnight_today = now.replace(hour=0, minute=0, second=0, microsecond=0)

    m = mode.upper()
    if m == "DAILY":
        start_date = midnight_today
        end_date = midnight_today + timedelta(days=1)
    elif m == "WEEKLY":
        start_date = midnight_today - timedelta(days=7)
        end_date = midnight_today
    elif m == "MONTHLY":
        first_of_this_month = midnight_today.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        start_date = last_month_end.replace(day=1)
        end_date = first_of_this_month
    elif m == "MONTHLY_MTD":
        start_date = midnight_today.replace(day=1)
        end_date = midnight_today + timedelta(days=1)
    else:
        raise ValueError("Invalid mode. Use DAILY, WEEKLY, MONTHLY, or MONTHLY_MTD.")

    logging.info(f"Window mode: {start_date} -> {end_date}")
    return start_date, end_date


def build_ses_email_bodies(row_count: int, generated_at: datetime | None = None) -> tuple[str, str]:
    """
    Return (text_body, html_body) for the SES email. ASCII only.
    """
    if generated_at is None:
        generated_at = datetime.now(timezone.utc)
    gen_str = generated_at.strftime("%Y-%m-%d %H:%M:%S %Z")

    text_body = (
        "Service Entry Sheet Report\n\n"
        f"Total Rows: {row_count}\n"
        f"Time Generated: {gen_str}\n\n"
        "The report has been generated successfully. The file is attached.\n\n"
        "Please note:\n"
        "- This report was generated automatically.\n"
        "- For questions about process, completeness, or data accuracy, email ap_operations@health.qld.gov.au\n\n"
        "Thank you,\n"
        "AP Operations, Queensland Health\n"
    )

    html_body = f"""\
<!DOCTYPE html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Server Entry Sheet Report</title>
    <style>
      body {{ margin: 0; padding: 0; background-color: #f9fafb; }}
      .container {{ font-family: Arial, Helvetica, sans-serif; font-size: 14px; color: #1f2937; padding: 16px; }}
      .card {{ background-color: #ffffff; border: 1px solid #e5e7eb; border-radius: 6px; padding: 16px; }}
      h2 {{ margin: 0 0 8px 0; font-size: 18px; color: #111827; }}
      p {{ margin: 8px 0; line-height: 1.45; }}
      table {{ border-collapse: collapse; width: 100%; margin: 12px 0 16px 0; }}
      th, td {{ border: 1px solid #e5e7eb; padding: 8px; text-align: left; font-size: 13px; }}
      .note {{ background-color: #f3f4f6; border: 1px solid #e5e7eb; border-radius: 4px; padding: 10px; margin-top: 12px; }}
      .footer {{ margin-top: 16px; font-size: 12px; color: #6b7280; }}
      a {{ color: #2563eb; text-decoration: none; }}
    </style>
  </head>
  <body>
    <div class="container">
      <div class="card">
        <h2>Service Entry Sheet Report</h2>
        <p>The report has been generated successfully. The file is attached.</p>

        <table role="presentation" aria-hidden="true">
          <tr>
            <th>Total Rows</th>
            <td>{row_count}</td>
          </tr>
          <tr>
            <th>Time Generated</th>
            <td>{gen_str}</td>
          </tr>
        </table>

        <div class="note">
          <p><strong>Please note:</strong></p>
          <ul style="margin: 6px 0 0 18px;">
            <li>This report was generated automatically.</li>
            <li>For questions about process, completeness, or data accuracy, email <a href="mailto:ap_operations@health.qld.gov.au">ap_operations@health.qld.gov.au</a>.</li>
          </ul>
        </div>

        <p class="footer">
          Thank you,<br>
          AP Operations, Queensland Health
        </p>
      </div>
    </div>
  </body>
</html>
"""
    return text_body, html_body


def send_email(
    subject: str,
    body_text: str,
    recipient_list: list[str] | str,
    attachment_path: str | None = None,
    body_html: str | None = None,
) -> bool:
    """
    Send email with plain text and optional HTML alternative.
    Attaches file if attachment_path is provided.
    """
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    if isinstance(recipient_list, (list, tuple)):
        msg["To"] = ", ".join(recipient_list)
    else:
        msg["To"] = recipient_list

    # Plain text
    msg.set_content(body_text)

    # Optional HTML
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    logging.info(f"Email compose start: {msg['To']}")

    # Attachment
    if attachment_path:
        try:
            filename = os.path.basename(attachment_path)
            with open(attachment_path, "rb") as f:
                data = f.read()

            # Prefer text/csv for .csv files
            if filename.lower().endswith(".csv"):
                maintype, subtype = "text", "csv"
            else:
                maintype, subtype = "application", "octet-stream"

            msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=filename)
            logging.info(f"Attachment added: {filename} ({len(data)} bytes)")
        except Exception:
            logging.exception(f"Failed to attach file {attachment_path}")

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as s:
            s.send_message(msg)
            logging.info("Email send success")
            return True
    except Exception as e:
        print("Send failed.")
        logging.exception("Email send failed")
        print(type(e).__name__, str(e))
        return False


def db_conn(username: str, password: str, dsn: str):
    try:
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        print("Connection successful.")
        logging.info("DB connection successful")
        return connection
    except oracledb.Error as e:
        print(f"Error connecting to Oracle database: {e}")
        logging.exception("DB connection failed")
        return None


def write_csv(df: pd.DataFrame) -> str:
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    tmp_file = os.path.join(OUTPUT_PATH, f"_{TODAY_STR}_tmp_QH_ServiceEntry.csv")
    final_file = os.path.join(OUTPUT_PATH, f"{TODAY_STR}_QH_ServiceEntry.csv")
    df.to_csv(tmp_file, index=False)
    os.replace(tmp_file, final_file)
    print(f"Wrote {len(df)} rows ->")
    print(f"Wrote to {final_file}")
    logging.info(f"Wrote {len(df)} rows to {final_file}")
    return final_file


# =========================================================
# Main
# =========================================================
def main() -> int:
    dsn = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE}"
    logging.info(f"DSN built: {dsn}")

    conn = db_conn(DB_USER, DB_PASS, dsn)
    cur = None
    success = False

    try:
        if not conn:
            raise RuntimeError("No DB connection. Aborting.")

        cur = conn.cursor()
        with open(SQL_PATH, "r", encoding="utf-8") as f:
            sql_text = f.read()

        # Date window
        start_dt, end_dt = compute_dates(RUN_MODE)
        params = {"start_date_ts": start_dt, "end_date_ts": end_dt}

        # Query
        df = pd.read_sql_query(sql_text, conn, params=params)
        logging.info(f"Window ({RUN_MODE}): {start_dt} -> {end_dt} | Rows: {len(df)}")
        logging.info(f"Query returned {df.shape[0]} rows and {df.shape[1]} columns")

        # Output
        output_path = write_csv(df)
        success = True

        # Email
        subject = f"Service Entry Report - {TODAY_STR}"
        text_body, html_body = build_ses_email_bodies(len(df))
        ok = send_email(
            subject=subject,
            body_text=text_body,
            body_html=html_body,
            recipient_list=RECIPIENT_LIST if RECIPIENT_LIST else SMTP_TO,
            attachment_path=output_path,
        )
        if not ok:
            logging.error("Email send returned False")

    except oracledb.Error as e:
        print(f"Error executing query: {e}")
        logging.exception("Job failed with Oracle error")
    except Exception as e:
        print(f"Unhandled error: {e}")
        logging.exception("Job failed with unhandled exception")
    finally:
        if cur:
            cur.close()
            print("Cursor closed.")
        if conn:
            conn.close()
            print("Connection closed.")

    logging.shutdown()
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
