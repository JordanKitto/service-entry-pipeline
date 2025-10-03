import smtplib
from email.message import EmailMessage

# Server variables
SMTP_SERVER = "qhsmtp.health.qld.gov.au"
SMTP_PORT = 25

# Send/recieve variables
SENDER = "jordan.kitto@healht.qld.gov.au"
RECIPIENT = "ap_operations@health.qld.gov.au"

# Email variables
project = "Service Entry Report"
frequency = "Daily"
subproject = "A0025"
start_date = "2025-10-01"
start_time = "08:30"
end_date = start_date
end_time = "8:42"

SUBJECT = "Testing automatic email notification"
BODY = " 01/10/2025 -  This first email is a test to see if python is able to send an email"

# Fomatting email
msg = EmailMessage()
msg["From"] = SENDER
msg["To"] = RECIPIENT
msg["Subject"] = SUBJECT
msg.set_content(BODY)

# Sending email and catching errors
try:
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
        # No STARTTLS and no login for first test
        server.send_message(msg)
        print("Email sent. check your inbox")
except Exception as e:
    print("Send failed. ")
    print(type(e).__name__, str(e))




