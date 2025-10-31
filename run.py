import os
import sys
import logging
import smtplib
from email.message import EmailMessage
import pandas as pd
import oracledb
from datetime import datetime, timedelta, date, time, timezone
from dotenv import load_dotenv

# ─────────────────────────────
# 1. Config Loader
# ─────────────────────────────
class EnvConfig:
    """Handles environment setup, file paths, and logging configuration."""
    def __init__(self):
        self.root = os.path.dirname(os.path.abspath(__file__))
        self.env = os.path.join(self.root, "config", ".env")
        self.sql_path = os.path.join(self.root, "sql", "ses_query.sql")
        self.output = os.path.join(self.root, "output")
        self.logs = os.path.join(self.root, "logs")
        self.today = date.today()
        self.today_string = self.today.strftime("%Y%m%d")
        self.log_file = os.path.join(self.logs, f"{self.today_string}_LOG_FILE.txt")
        self.lock_file = os.path.join(self.root, "ses.lock")

        # Create folders
        os.makedirs(self.output, exist_ok=True)
        os.makedirs(self.logs, exist_ok=True)

        # Setup logging
        logging.basicConfig(
            filename=self.log_file,
            filemode='a',
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
        logging.info("Job started")

        # Load environment variables
        load_dotenv(self.env)
        logging.info("Environment loaded")

        # DB + SMTP configs
        self.db = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "service": os.getenv("DB_SERVICE"),
            "user": os.getenv("DB_USER"),
            "pass": os.getenv("DB_PASS"),
        }
        self.smtp = {
            "server": os.getenv("SMTP_SERVER"),
            "port": int(os.getenv("SMTP_PORT", "25")),
            "from": os.getenv("SMTP_FROM"),
            "to": [a.strip() for a in os.getenv("SMTP_TO", "").split(",") if a.strip()]
        }
        self.mode = os.getenv("RUN_MODE", "DAILY").upper()
        self.year_start_month = int(os.getenv("YEAR_START_MONTH", "1"))


# ─────────────────────────────
# 2. Oracle Client
# ─────────────────────────────
class OracleClient:
    def __init__(self, cfg: EnvConfig):
        self.cfg = cfg
        self.conn = None
        self.dsn = f"{cfg.db['host']}:{cfg.db['port']}/{cfg.db['service']}"

    def connect(self):
        try:
            self.conn = oracledb.connect(
                user=self.cfg.db["user"],
                password=self.cfg.db["pass"],
                dsn=self.dsn
            )
            logging.info("Oracle connection successful")
        except Exception as e:
            logging.exception("Oracle connection failed")
            raise

    def query(self, sql_text, params=None):
        df = pd.read_sql_query(sql_text, self.conn, params=params)
        logging.info(f"Query returned {len(df):,} rows and {df.shape[1]} columns")
        return df

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Oracle connection closed")


# ─────────────────────────────
# 3. File Manager
# ─────────────────────────────
class FileManager:
    def __init__(self, cfg: EnvConfig):
        self.cfg = cfg

    def base_dir(self, sub=""):
        path = os.path.join(self.cfg.output, sub)
        os.makedirs(path, exist_ok=True)
        return path

    def file_path(self, sub, suffix):
        return os.path.join(
            self.base_dir(sub),
            f"{self.cfg.today_string}_QH_ServiceEntry_{suffix.upper()}.csv"
        )

    def temp_path(self, sub, suffix):
        return os.path.join(
            self.base_dir(sub),
            f"_{self.cfg.today_string}_tmp_QH_ServiceEntry_{suffix.upper()}.csv"
        )

    def done_marker(self, sub, suffix):
        return os.path.join(
            self.base_dir(sub),
            f"_DONE_{self.cfg.today_string}_{suffix.upper()}.txt"
        )

    def should_skip(self, sub, suffix):
        if os.path.exists(self.file_path(sub, suffix)) or os.path.exists(self.done_marker(sub, suffix)):
            logging.info(f"{suffix.capitalize()} already generated, skipping.")
            return True
        return False

    def write_csv(self, df, sub, suffix):
        temp = self.temp_path(sub, suffix)
        final = self.file_path(sub, suffix)
        df.to_csv(temp, index=False)
        os.replace(temp, final)
        open(self.done_marker(sub, suffix), "w").write("done")
        logging.info(f"Wrote {len(df):,} rows → {final}")
        return final


# ─────────────────────────────
# 4. Email Client
# ─────────────────────────────
class EmailClient:
    def __init__(self, cfg: EnvConfig):
        self.cfg = cfg

    def build_body(self, sections, generated_at: datetime | None = None, tzinfo=timezone.utc):
        """
        sections: list of dicts like:
        {"title": "Weekly", "window": "07 Oct 2025 → 14 Oct 2025", "rows": 1593}
        {"title": "Monthly (YTD through Oct 2025)", "window": "01 Jan 2025 → 01 Nov 2025", "rows": 8214}
        """
        if generated_at is None:
            generated_at = datetime.now(timezone.utc)
        if generated_at.tzinfo is None:
            generated_at = generated_at.replace(tzinfo=timezone.utc)
        gen_str = generated_at.astimezone(tzinfo).strftime("%Y-%m-%d %H:%M:%S %Z")

        # ---------- Plain text ----------
        text_lines = ["Service Entry Sheet Report", ""]
        for s in sections:
            text_lines.append(s["title"])
            text_lines.append(f"• Window: {s['window']}")
            text_lines.append(f"• Rows: {s['rows']:,}")
            text_lines.append("")  # spacer
        text_lines.extend([
            "Notes:",
            "- This report was generated automatically.",
            "- For questions about process, completeness, or data accuracy, email ap_operations@health.qld.gov.au",
            "",
            f"Time Generated: {gen_str}",
            "",
            "Thank you,",
            "AP Operations, Queensland Health",
        ])
        text_body = "\n".join(text_lines)

        # ---------- HTML ----------
        def card_html(title, window, rows):
            return f"""
            <div class="card">
            <h3>{title}</h3>
            <table role="presentation" aria-hidden="true">
                <tr><th>Window</th><td>{window}</td></tr>
                <tr><th>Rows</th><td>{rows:,}</td></tr>
            </table>
            </div>
            """

        cards_html = "\n".join(card_html(s["title"], s["window"], s["rows"]) for s in sections)

        html_body = f"""\
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="utf-8">
        <title>Service Entry Sheet Report</title>
        <style>
        body {{ margin:0; padding:0; background:#f9fafb; }}
        .container {{ font-family: Arial, Helvetica, sans-serif; font-size:14px; color:#1f2937; padding:16px; }}
        .heading {{ font-size:22px; margin:0 0 12px 0; color:#111827; }}
        .card {{ background:#ffffff; border:1px solid #e5e7eb; border-radius:6px; padding:16px; margin:12px 0; }}
        h3 {{ margin:0 0 8px 0; font-size:16px; }}
        table {{ border-collapse:collapse; width:100%; }}
        th, td {{ border:1px solid #e5e7eb; padding:8px; text-align:left; font-size:13px; }}
        .note {{ background:#f3f4f6; border:1px solid #e5e7eb; border-radius:4px; padding:10px; margin-top:12px; }}
        .footer {{ margin-top:16px; font-size:12px; color:#6b7280; }}
        a {{ color:#2563eb; text-decoration:none; }}
        </style>
    </head>
    <body>
        <div class="container">
        <h2 class="heading">Service Entry Sheet Report</h2>

        {cards_html}

        <div class="card">
            <table role="presentation" aria-hidden="true">
            <tr><th>Time Generated</th><td>{gen_str}</td></tr>
            </table>
        </div>

        <div class="note">
            <p><strong>Please note:</strong></p>
            <ul style="margin:6px 0 0 18px;">
            <li>This report was generated automatically.</li>
            <li>For questions about process, completeness, or data accuracy, email
                <a href="mailto:ap_operations@health.qld.gov.au">ap_operations@health.qld.gov.au</a>.</li>
            </ul>
        </div>

        <p class="footer">
            Thank you,<br>
            AP Operations, Queensland Health
        </p>
        </div>
    </body>
    </html>
    """
        return text_body, html_body

    def send(self, subject, text_body, html_body, attachments):
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.cfg.smtp["from"]
        msg["To"] = ", ".join(self.cfg.smtp["to"])
        msg.set_content(text_body)
        msg.add_alternative(html_body, subtype="html")

        for apath in attachments:
            with open(apath, "rb") as f:
                data = f.read()
            msg.add_attachment(
                data,
                maintype="application",
                subtype="octet-stream",
                filename=os.path.basename(apath),
            )

        with smtplib.SMTP(self.cfg.smtp["server"], self.cfg.smtp["port"], timeout=30) as s:
            s.send_message(msg)
            logging.info(f"Email sent successfully to {len(self.cfg.smtp['to'])} recipients")


# ─────────────────────────────
# 5. Main Runner
# ─────────────────────────────
class ServiceEntryRunner:
    def __init__(self, cfg: EnvConfig):
        self.cfg = cfg
        self.db = OracleClient(cfg)
        self.fm = FileManager(cfg)
        self.email = EmailClient(cfg)
        self.today = cfg.today

    def what_is_due(self):
        return (self.today.weekday() == 0, self.today.day == 1)

    def weekly_window(self):
        end = datetime.combine(self.today, time.min)
        start = end - timedelta(days=7)
        return start, end

    def monthly_window(self):
        start = datetime.combine(date(self.today.year, self.cfg.year_start_month, 1), time.min)
        end = datetime.combine(self.today, time.min)
        return start, end

    def run(self):
        if os.path.exists(self.cfg.lock_file):
            logging.info("Lock file detected, skipping run.")
            return

        open(self.cfg.lock_file, "w").close()
        weekly_due, monthly_due = self.what_is_due()
        if not weekly_due and not monthly_due:
            logging.info("No reports due today.")
            os.remove(self.cfg.lock_file)
            return

        self.db.connect()
        with open(self.cfg.sql_path, "r", encoding="utf-8") as f:
            sql_text = f.read()

        # Collect both: attachments for the email + structured sections for the body
        attachments, sections = [], []

        # Weekly
        if weekly_due and not self.fm.should_skip("weekly", "WEEKLY"):
            ws, we = self.weekly_window()
            df_w = self.db.query(sql_text, {"start_date_ts": ws, "end_date_ts": we})
            attachments.append(self.fm.write_csv(df_w, "weekly", "WEEKLY"))

            sections.append({
                "title": "Weekly",
                "window": f"{ws:%d %b %Y} → {we:%d %b %Y}",
                "rows": len(df_w),
            })

        # Monthly
        if monthly_due and not self.fm.should_skip("monthly", "MONTHLY"):
            ms, me = self.monthly_window()
            df_m = self.db.query(sql_text, {"start_date_ts": ms, "end_date_ts": me})
            attachments.append(self.fm.write_csv(df_m, "monthly", "MONTHLY"))

            prev_label = (self.today.replace(day=1) - timedelta(days=1)).strftime("%b %Y")
            sections.append({
                "title": f"Monthly (YTD through {prev_label})",
                "window": f"{ms:%d %b %Y} → {me:%d %b %Y}",
                "rows": len(df_m),
            })

        # If nothing generated
        if not attachments:
            logging.info("All reports already generated for today. Nothing to send.")
            os.remove(self.cfg.lock_file)
            return

        # Subject line
        subject = "Service Entry Sheet Report"
        if weekly_due and monthly_due:
            subject += " — Weekly & Monthly"
        elif weekly_due:
            subject += " — Weekly"
        elif monthly_due:
            subject += " — Monthly"

        # Build bodies (text + HTML) from structured sections
        text_body, html_body = self.email.build_body(
            sections,
            generated_at=datetime.now(timezone.utc)
        )

        # Send
        self.email.send(subject, text_body, html_body, attachments)


        self.db.close()
        if os.path.exists(self.cfg.lock_file):
            os.remove(self.cfg.lock_file)
        logging.info("Run completed successfully")

# ─────────────────────────────
# Entry Point
# ─────────────────────────────
if __name__ == "__main__":
    cfg = EnvConfig()
    ServiceEntryRunner(cfg).run()
