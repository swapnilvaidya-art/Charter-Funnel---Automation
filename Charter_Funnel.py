import os
import time
import json
import math
import requests
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV VARIABLES --------------------
sec = os.getenv("SWAPNIL_SECRET_KEY")
User_name = os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URL = os.getenv("METABASE_URL")
QUERY_URL = os.getenv("CHARTER_FUNNEL_QUERY")
SAK = os.getenv("SHEET_ACCESS_KEY")

if not sec or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)

creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)

# -------------------- METABASE LOGIN --------------------
print("🔐 Creating Metabase session...")

res = requests.post(
    MB_URL,
    headers={"Content-Type": "application/json"},
    json={"username": User_name, "password": sec},
    timeout=60
)

res.raise_for_status()
token = res.json()['id']

METABASE_HEADERS = {
    "Content-Type": "application/json",
    "X-Metabase-Session": token
}

print("✅ Metabase session created")

# -------------------- FETCH WITH RETRY --------------------
def fetch_with_retry(url, headers, retries=5):
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, timeout=180)
            response.raise_for_status()
            return response
        except Exception as e:
            wait_time = 10 * attempt
            print(f"[Metabase] Attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

# -------------------- SANITIZE DATAFRAME --------------------
def sanitize_df(df):
    df.replace([np.inf, -np.inf], None, inplace=True)
    df = df.fillna("")
    df = df.astype(str)
    df = df.replace("None", "")
    return df

# -------------------- SAFE SHEET UPDATE --------------------
def safe_update_sheet(worksheet, df, clear_range, retries=5):
    print(f"🔄 Updating worksheet: {worksheet.title}")

    for attempt in range(1, retries + 1):
        try:
            rows = len(df) + 1
            cols = len(df.columns)

            # Clear only A:R
            worksheet.batch_clear([clear_range])

            # Prepare values
            header = df.columns.tolist()
            data_rows = df.values.tolist()

            def sanitize_row(row):
                return [str(v) if v is not None else "" for v in row]

            data_rows = [sanitize_row(row) for row in data_rows]
            values = [header] + data_rows

            worksheet.update(
                f"A1:{chr(64 + cols)}{rows}",
                values,
                value_input_option="USER_ENTERED"
            )

            print(f"✅ Sheet updated successfully: {worksheet.title}")
            return True

        except Exception as e:
            wait_time = 15 * attempt
            print(f"[Sheets] Attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

# -------------------- MAIN EXECUTION --------------------
print("📥 Fetching Charter Funnel query from Metabase...")

response = fetch_with_retry(QUERY_URL, METABASE_HEADERS)
df = pd.DataFrame(response.json())

if df.empty:
    print("⚠️ WARNING: Query returned empty dataset.")
else:
    print(f"📊 Rows fetched: {len(df)}")
    df = sanitize_df(df)

    print("🔗 Connecting to Google Sheets...")
    sheet = gc.open_by_key(SAK)
    ws = sheet.worksheet("CM Dump")

    print("⬆️ Updating CM Dump...")
    safe_update_sheet(ws, df, "A:R")

# -------------------- TIMER SUMMARY --------------------
end_time = time.time()
elapsed = end_time - start_time
mins, secs = divmod(elapsed, 60)

print(f"⏱ Total execution time: {int(mins)}m {int(secs)}s")
print("🎯 Charter Funnel Automation Completed Successfully!")
