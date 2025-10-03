# 🧾 Service Entry Daily Export Pipeline

A lightweight, production-grade Python automation that connects to an **Oracle database**, runs a **parameterized SQL query**, exports the result set to a **CSV file**, writes structured **daily logs**, and emails the output as an attachment to a distribution list.  
Designed to run unattended as a scheduled job (e.g. Windows Task Scheduler or cron).

---

## 📌 Overview

This project was built to automate the daily extraction of **Service Entry records** from Queensland Health’s Oracle data warehouse.  

Each morning, the pipeline:
1. Connects to the Oracle database using credentials stored in a `.env` file.  
2. Executes the SQL query stored in `/sql/service_entry.sql`.  
3. Loads the result into a Pandas DataFrame.  
4. Writes the data to a **date-stamped CSV** using atomic writes.  
5. Logs all operations in a daily log file under `/logs`.  
6. Sends an **email with the CSV attached** to configured recipients via SMTP.  
7. Exits with a proper status code (0 = success, 1 = failure) for scheduler monitoring.

---

## 🧠 SQL Query

Below is the exact SQL file executed daily:

```sql
SELECT 
    h.DOCID AS "Document Number",
    h.BELNR_FI AS "Accounting Document Number",
    h.BUKRS AS "Company Code",
    l.OPT_VIM_1LOG_STATUS AS "Invoice Status",
    l.OPT_VIM_1LOG_FUNC_TEXT AS "Activity",
    l.OPT_VIM_1LOG_ACTUAL_ROLE AS "Actual Role",
    l.OPT_VIM_1LOG_ACTUAL_AGENT AS "Actual Agent",
    l.OPT_VIM_1LOG_START_DATE_TIME AS "Start Date & Time",
    l.OPT_VIM_1LOG_END_DATE_TIME AS "End Date & Time",
    l.OPT_VIM_1LOG_WORKITEM_ID AS "Work Item ID",
    t.PROC_TYPE AS "Process Type Number",
    t.OBJTXT AS "Process Type Text"
FROM
DSS.VIM_OPT_VIM_1LOG_VW l
JOIN 
DSS.VIM_1HEAD_2HEAD_VW h 
    ON l.OPT_VIM_1LOG_DOCID = h.DOCID
JOIN
DSS.VIM_STG_T800T_VW t
    ON l.OPT_VIM_1LOG_PROCESS_TYPE = t.PROC_TYPE
WHERE
l.OPT_VIM_1LOG_FUNC_TEXT = 'Bypassed Rule -QH - Service Entry Requir'
ORDER BY
h.DOCID, l.OPT_VIM_1LOG_START_DATE_TIME;
```
---

## 🧱 Project Structure
service-entry-pipeline/
├─ sql/
│  └─ service_entry.sql        # SQL executed daily
├─ config/
│  └─ .env.example            # Template for credentials & settings
├─ output/                    # CSV files written here (gitignored)
├─ logs/                      # Daily logs (gitignored)
├─ run.py                     # Main entry point
├─ requirements.txt
├─ README.md
└─ .gitignore
---

## ⚡ Example Outputs

✅ Email Notification with Attachment

📎 Insert screenshot of Outlook email with attached CSV file here

📊 Daily Log File

📝 Insert screenshot of a log file showing query, write, and email steps

📂 Generated CSV File

🗂 Insert screenshot of the CSV opened in Excel showing column headers and sample data

---

## ⚙️ Setup
1️⃣ Clone the repo
git clone https://github.com/YOUR_USERNAME/service-entry-pipeline.git
cd service-entry-pipeline

2️⃣ Create a virtual environment
python -m venv .venv
.venv\Scripts\activate   # Windows
pip install -r requirements.txt

3️⃣ Configure environment variables

Copy .env.example → .env and update with real credentials:
DB_HOST=your_host
DB_PORT=1521
DB_SERVICE=your_service
DB_USER=your_user
DB_PASS=your_password

SMTP_SERVER=smtp.example.com
SMTP_PORT=25
MAIL_FROM=you@example.com
MAIL_TO=recipient1@example.com, recipient2@example.com
⚠️ .env is excluded from version control via .gitignore — never commit credentials.

---

## 🧪 Run Locally

python run.py
Expected output:

A CSV file is created in /output

Logs are written to /logs/YYYYMMDD_LOG_FILE.txt

An email with the CSV attached is sent to recipients

🕒 Schedule Daily Execution

You can schedule this job using Windows Task Scheduler:

Program: C:\path\to\venv\Scripts\python.exe

Arguments: run.py

Start in: C:\path\to\project\service-entry-pipeline

Trigger: Daily, e.g. 6:00 AM

Action: Run whether user is logged in or not

Settings: Stop after 1 hour, retry on failure

📨 Email Functionality

The send_email helper uses Python’s built-in smtplib and EmailMessage to:

Send to multiple recipients

Attach the generated CSV file

Log send attempts and errors

Work with unauthenticated internal relays (port 25)

🪵 Logging

Each run creates a log file in /logs with entries like:
2025-10-03 14:07:50,071 [INFO] Job started
2025-10-03 14:07:50,423 [INFO] DB Connection Successful
2025-10-03 14:08:05,912 [INFO] Query returned 3517 rows and 12 columns
2025-10-03 14:08:05,936 [INFO] Wrote 3517 rows to /output/20251003_QH_ServiceEntry.csv
2025-10-03 14:08:06,124 [INFO] Attachment added: 20251003_QH_ServiceEntry.csv (145kb)
2025-10-03 14:08:06,345 [INFO] Email send success

🧠 Tech Stack

🐍 Python 3.10+

🧠 oracledb — Oracle database connector

📝 pandas — DataFrame querying & export

🔐 python-dotenv — Secure config handling

📧 smtplib / EmailMessage — SMTP mailer

🧪 Optional: Demo Mode

You can add a USE_SQLITE=1 flag in .env to run the pipeline without Oracle, using a bundled SQLite file and fake data. (Nice for recruiters to try without DB access.)

📜 License

MIT License — feel free to fork and adapt for your own Oracle reporting pipelines.

🧍 Author

Jordan Kitto
Senior Systems Officer | Data & Automation Enthusiast
