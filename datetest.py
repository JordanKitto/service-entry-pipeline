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
    else:
        raise ValueError("Invalid mode. Choose from 'DAILY', 'WEEKLY', or 'MONTHLY'.")
    
    logging.info(f"window mode: {start_date}, {end_date}")
    return start_date, end_date

for m in ["DAILY", "WEEKLY", "MONTHLY"]:
    s, e = compute_dates(m)
    print(f"{m}: {s} → {e}")