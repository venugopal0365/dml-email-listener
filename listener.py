import psycopg2
import psycopg2.extensions
import smtplib
import json
import time
import os
from email.message import EmailMessage

# ─── DATABASE CONFIG ──────────────────────────────────────────

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise Exception("DATABASE_URL not set")

print("Database URL loaded.")

# ─── EMAIL CONFIG ─────────────────────────────────────────────

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL")

print("Email configuration loaded.")


def send_email(
    to_email,
    owner,
    operation,
    schema,
    table,
    column,
    old_value,
    new_value,
    timestamp,
):

    try:

        msg = EmailMessage()

        msg["Subject"] = f"[DB Alert] {operation} on {schema}.{table}"
        msg["From"] = FROM_EMAIL

        if isinstance(to_email, str):
            email_list = [e.strip() for e in to_email.split(",")]
        else:
            email_list = to_email

        msg["To"] = ", ".join(email_list)

        msg.set_content(
            f"""
Hello {owner},

Operation : {operation}
Schema    : {schema}
Table     : {table}

Column    : {column}
Old Value : {old_value}
New Value : {new_value}

Time      : {timestamp}

-- Automated DB Alert
"""
        )

        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:

            smtp.starttls()
            smtp.login(SMTP_USER, SMTP_PASS)

            smtp.send_message(
                msg,
                from_addr=FROM_EMAIL,
                to_addrs=email_list
            )

        print("✓ Email sent to:", email_list)

    except Exception as e:
        print("Email error:", e)


def start_listener():

    while True:

        try:

            print("Connecting to PostgreSQL...")

            conn = psycopg2.connect(DATABASE_URL)

            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )

            cur = conn.cursor()

            cur.execute("LISTEN dml_alert;")

            print("Listening for DML events on channel: dml_alert")
            print("-" * 50)

            while True:

                conn.poll()

                while conn.notifies:

                    notify = conn.notifies.pop(0)

                    print("Notification received:", notify.payload)

                    payload = json.loads(notify.payload)

                    emails = [
                        "samdanis017@gmail.com",
                        "venu0365@gmail.com"
                    ]

                    send_email(
                        to_email=emails,
                        owner=payload.get("owner"),
                        operation=payload.get("operation"),
                        schema=payload.get("schema"),
                        table=payload.get("table"),
                        column=payload.get("column"),
                        old_value=payload.get("old_value"),
                        new_value=payload.get("new_value"),
                        timestamp=payload.get("timestamp")
                    )

                time.sleep(0.5)

        except Exception as e:

            print("Database connection lost:", e)
            print("Retrying in 5 seconds...")

            time.sleep(5)


if __name__ == "__main__":
    start_listener()
