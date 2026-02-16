# Google Sheets API Setup Guide

This guide walks you through setting up Google Sheets API access for the wedding vendor scraper.

## Prerequisites

- A Google account
- Internet connection
- 10 minutes of your time

---

## Step-by-Step Setup

### 1. Create a Google Cloud Project

1. Navigate to [Google Cloud Console](https://console.cloud.google.com/)
2. Sign in with your Google account
3. Click the project dropdown at the top (next to "Google Cloud")
4. Click **"New Project"**
5. Enter project name: **"Wedding Vendor Scraper"**
6. Click **"Create"**
7. Wait for the project to be created (takes ~30 seconds)
8. Select your new project from the dropdown

### 2. Enable Google Sheets API

1. In the left sidebar, navigate to **"APIs & Services" > "Library"**
2. In the search bar, type: **"Google Sheets API"**
3. Click on **"Google Sheets API"** in the results
4. Click the blue **"Enable"** button
5. Wait for the API to be enabled (~10 seconds)

### 3. Create a Service Account

1. In the left sidebar, navigate to **"APIs & Services" > "Credentials"**
2. Click **"Create Credentials"** at the top
3. Select **"Service Account"** from the dropdown
4. Fill in the service account details:
   - **Service account name**: `vendor-scraper`
   - **Service account ID**: (auto-generated, leave as is)
   - **Description**: `Service account for wedding vendor scraper`
5. Click **"Create and Continue"**
6. Grant role: Select **"Editor"** from the role dropdown
   - Alternatively, use **"Project" > "Editor"** for broader access
7. Click **"Continue"**
8. Skip the optional "Grant users access" section
9. Click **"Done"**

### 4. Create and Download JSON Key

1. You'll see your new service account in the list
2. Click on the service account email (looks like `vendor-scraper@...iam.gserviceaccount.com`)
3. Navigate to the **"Keys"** tab at the top
4. Click **"Add Key" > "Create new key"**
5. Select **"JSON"** as the key type
6. Click **"Create"**
7. The JSON file will automatically download to your computer
   - The file name will be something like: `wedding-vendor-scraper-abc123.json`

### 5. Save Credentials to Project

1. **Rename the downloaded file** to: `google_credentials.json`
2. **Move the file** to your project's `config/` directory:
   ```
   wedding-vendor-scraper/
   └── config/
       └── google_credentials.json  <-- Place it here
   ```
3. **Verify the path**: The full path should be:
   ```
   E:/wedding-vendor-scraper/config/google_credentials.json
   ```

---

## Verification

### Test Authentication

Run this simple Python script to verify your setup:

```python
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
```

### Run the Export Script

```bash
python export_to_sheets.py
```

If setup is correct, you should see:
- ✅ Credentials validated
- ✅ CSV data loaded
- ✅ Export options displayed

---

## Troubleshooting

### Error: "Permission denied" or "Insufficient permissions"

**Cause**: The service account doesn't have access to the spreadsheet.

**Solution**:
1. Open the Google Sheet in your browser
2. Click the **"Share"** button (top-right)
3. Paste the service account email:
   - Format: `vendor-scraper@wedding-vendor-scraper-123456.iam.gserviceaccount.com`
   - You can find this email in the JSON credentials file under `"client_email"`
4. Set permission to **"Editor"**
5. Click **"Send"** (uncheck "Notify people" if you want)
6. Try exporting again

### Error: "File not found: config/google_credentials.json"

**Cause**: Credentials file is missing or in the wrong location.

**Solution**:
1. Verify the file exists: `ls config/google_credentials.json`
2. Check the file name is exactly `google_credentials.json` (no spaces, correct extension)
3. Ensure it's in the `config/` directory, not the root directory
4. If the path in `config.yaml` is different, update it or move the file

### Error: "API has not been used in project before" or "API quota exceeded"

**Cause**: Google Sheets API not enabled, or rate limit hit.

**Solution for API not enabled**:
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Select your project
3. Navigate to **"APIs & Services" > "Library"**
4. Search for "Google Sheets API"
5. Click **"Enable"**

**Solution for quota exceeded**:
1. Wait 60 seconds and try again (soft rate limit)
2. If persistent, check quota limits in Google Cloud Console:
   - Navigate to **"APIs & Services" > "Dashboard"**
   - Click on "Google Sheets API"
   - View quota usage
3. Request quota increase if needed (rare for normal use)

### Error: "Invalid credentials" or "Credentials are not valid"

**Cause**: JSON file is corrupted or incorrect.

**Solution**:
1. Delete the current `google_credentials.json`
2. Go back to Google Cloud Console
3. Navigate to your service account
4. Create a new JSON key (see Step 4 above)
5. Download and save the new key
6. Try again

### Error: "ModuleNotFoundError: No module named 'gspread'"

**Cause**: Dependencies not installed.

**Solution**:
```bash
pip install -r requirements.txt
```

### Error: Network or connection issues

**Cause**: No internet connection or firewall blocking Google APIs.

**Solution**:
1. Check your internet connection
2. Try accessing https://sheets.googleapis.com/ in your browser
3. If behind a corporate firewall, check with your IT department
4. Try using a VPN if regional restrictions apply

---

## Security Best Practices

### DO:
- ✅ Keep `google_credentials.json` private and secure
- ✅ Add `config/google_credentials.json` to `.gitignore` (already done)
- ✅ Restrict service account permissions to minimum required
- ✅ Rotate credentials periodically (every 90 days recommended)
- ✅ Use service accounts instead of personal OAuth tokens for automation
- ✅ Store credentials outside version control

### DON'T:
- ❌ Commit `google_credentials.json` to Git
- ❌ Share credentials in plaintext via email/chat
- ❌ Grant more permissions than needed (e.g., "Owner" role)
- ❌ Use the same credentials across multiple projects
- ❌ Store credentials in public repositories
- ❌ Hardcode credentials in source code

### If Credentials are Compromised:

1. **Immediately revoke the key**:
   - Go to Google Cloud Console
   - Navigate to service account > Keys tab
   - Delete the compromised key
2. **Create a new key** (follow Step 4 above)
3. **Update the project** with the new credentials
4. **Review access logs** in Google Cloud Console for suspicious activity
5. **Consider rotating all project credentials**

---

## Understanding Service Accounts

**What is a service account?**
- A special type of Google account for applications (not humans)
- Has its own email address (e.g., `vendor-scraper@...iam.gserviceaccount.com`)
- Used for server-to-server authentication
- No password - uses public/private key pairs (in the JSON file)

**Why use service accounts for this project?**
- No manual OAuth flow required (no browser popups)
- Works in automated scripts and servers
- Credentials are portable (can move between machines)
- Better for production deployments
- Granular permission control

**How does the JSON key work?**
- Contains a private key for authentication
- The key proves the script is authorized to act as the service account
- Google validates the signature in the key
- If valid, grants access to resources the service account can access

---

## Additional Resources

- [Google Sheets API Documentation](https://developers.google.com/sheets/api)
- [gspread Library Documentation](https://docs.gspread.org/)
- [Service Account Documentation](https://cloud.google.com/iam/docs/service-accounts)
- [Google Cloud Console](https://console.cloud.google.com/)

---

## Getting Help

If you encounter issues not covered here:

1. Check the error message carefully - it often contains the solution
2. Verify all steps in this guide were followed exactly
3. Try the verification script to isolate the problem
4. Search for the specific error message online
5. Check [gspread GitHub Issues](https://github.com/burnash/gspread/issues) for similar problems

---

**Setup complete!** You're now ready to export vendor data to Google Sheets. 🎉

Run `python export_to_sheets.py` to get started.
