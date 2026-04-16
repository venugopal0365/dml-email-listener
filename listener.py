import psycopg2
import psycopg2.extensions
import smtplib
import json
import time
import os
from email.message import EmailMessage

# ─── DATABASE CONFIG ──────────────────────────────────────────

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Safe port handling
try:
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
except Exception:
    DB_PORT = 5432

# Validate required variables
required_db_vars = {
    "DB_HOST": DB_HOST,
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD,
}

for var, value in required_db_vars.items():
    if not value:
        raise Exception(f"Missing environment variable: {var}")

print("Database configuration loaded.")

# ─── EMAIL CONFIG ─────────────────────────────────────────────

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
FROM_EMAIL = os.getenv("FROM_EMAIL")

try:
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
except Exception:
    SMTP_PORT = 587

required_email_vars = {
    "SMTP_HOST": SMTP_HOST,
    "SMTP_USER": SMTP_USER,
    "SMTP_PASS": SMTP_PASS,
    "FROM_EMAIL": FROM_EMAIL,
}

for var, value in required_email_vars.items():
    if not value:
        raise Exception(f"Missing environment variable: {var}")

print("Email configuration loaded.")

# ──────────────────────────────────────────────────────────────
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

        # Multiple email handling
        if isinstance(to_email, str):
            email_list = [e.strip() for e in to_email.split(",")]
        elif isinstance(to_email, list):
            email_list = to_email
        else:
            email_list = [str(to_email)]

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

            smtp.login(
                SMTP_USER,
                SMTP_PASS
            )

            smtp.send_message(
                msg,
                from_addr=FROM_EMAIL,
                to_addrs=email_list
            )

        print("✓ Email sent to:", email_list)

    except Exception as e:
        print("Email error:", e)


# ──────────────────────────────────────────────────────────────
def connect_db():

    print("Connecting to PostgreSQL...")

    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )


# ──────────────────────────────────────────────────────────────
def start_listener():

    while True:

        try:

            conn = connect_db()

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

                    print("\nNotification received:")
                    print("Raw payload:", notify.payload)

                    try:
                        payload = json.loads(notify.payload)
                    except Exception:
                        print("Payload is not JSON")
                        continue

                    print(
                        f"[EVENT] {payload.get('operation')} "
                        f"on {payload.get('schema')}."
                        f"{payload.get('table')}"
                    )

                    print("Owner:", payload.get("owner"))
                    print("Email:", payload.get("email"))
                    print("Column:", payload.get("column"))
                    print("Old Value:", payload.get("old_value"))
                    print("New Value:", payload.get("new_value"))
                    print("Time:", payload.get("timestamp"))

                    if not payload.get("email"):
                        print("No email found — skipping")
                        continue

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
                        timestamp=payload.get("timestamp"),
                    )

                time.sleep(0.5)

        except psycopg2.OperationalError as e:

            print("Database connection lost:", e)
            print("Retrying in 5 seconds...")
            time.sleep(5)

        except Exception as e:

            print("Listener error:", e)
            time.sleep(5)


if __name__ == "__main__":

    start_listener()
