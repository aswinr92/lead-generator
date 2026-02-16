import gspread
from google.oauth2.service_account import Credentials

try:
    creds = Credentials.from_service_account_file(
        'config/google_credentials.json',
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    client = gspread.authorize(creds)
    print("✅ Authentication successful!")
    print(f"Service account: {creds.service_account_email}")
except FileNotFoundError:
    print("❌ Error: google_credentials.json not found in config/ directory")
except Exception as e:
    print(f"❌ Authentication failed: {e}")