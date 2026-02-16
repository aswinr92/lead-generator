"""
Backfill Social Media Classification for Google Sheets

One-time migration script that reads all vendors from the live Google Sheet,
classifies website/instagram/facebook URLs, fetches follower counts, and
writes the new columns back — preserving all existing data (onboarding_status etc).

Usage:
    python processors/backfill_social_media.py --sheet-id YOUR_SHEET_ID
"""

import argparse
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

# Allow running from project root or from processors/
sys.path.insert(0, str(Path(__file__).parent.parent))

from processors.data_cleaner import VendorDataCleaner
from processors.social_media_enricher import SocialMediaEnricher

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
CREDENTIALS_FILE = 'config/google_credentials.json'
NEW_COLUMNS = [
    'instagram', 'facebook', 'website_type',
    'digital_presence', 'instagram_followers', 'facebook_followers'
]


def _authenticate() -> gspread.Client:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def load_sheet(sheet_id: str) -> tuple:
    """Returns (worksheet, DataFrame, list_of_existing_headers)."""
    client = _authenticate()
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.get_worksheet(0)

    all_values = worksheet.get_all_values()
    if not all_values:
        raise ValueError("Sheet is empty")

    headers = all_values[0]
    rows = all_values[1:]
    df = pd.DataFrame(rows, columns=headers)
    print(f"✅ Loaded {len(df)} vendors ({len(headers)} columns)")
    return worksheet, df, headers


def ensure_columns(worksheet: gspread.Worksheet, headers: list) -> list:
    """
    Add any missing NEW_COLUMNS to the sheet header row.
    Returns the updated full headers list.
    """
    updated = headers.copy()
    added = []
    for col in NEW_COLUMNS:
        if col not in updated:
            updated.append(col)
            added.append(col)

    if added:
        print(f"   Adding new columns: {added}")
        worksheet.update('A1', [updated])
    else:
        print("   All new columns already exist in sheet")

    return updated


def backfill(sheet_id: str, skip_followers: bool = False):
    print("\n" + "=" * 60)
    print("🔄 BACKFILL: Social Media Classification + Follower Counts")
    print("=" * 60)

    # Step 1: Load data
    print("\n📥 Loading Google Sheet...")
    worksheet, df, original_headers = load_sheet(sheet_id)

    # Step 2: Apply classification only (not full re-clean to preserve manual edits)
    print("\n🔍 Classifying social media URLs...")
    cleaner = VendorDataCleaner()
    df = cleaner._classify_and_split_social_media(df)

    ig_count = (df['instagram'].fillna('') != '').sum()
    fb_count = (df['facebook'].fillna('') != '').sum()
    web_count = (df['website_type'] == 'website').sum()
    none_count = (df['digital_presence'] == 'none').sum()

    print(f"   Real websites:    {web_count}")
    print(f"   Instagram pages:  {ig_count}")
    print(f"   Facebook pages:   {fb_count}")
    print(f"   No presence:      {none_count}")

    # Step 3: Fetch follower counts
    if skip_followers:
        print("\n⏭️  Skipping follower count fetching (--skip-followers)")
    else:
        print("\n📱 Fetching follower counts (best-effort, may take a while)...")
        enricher = SocialMediaEnricher()
        df = enricher.enrich_dataframe(df)

        ig_fetched = (df['instagram_followers'].fillna('') != '').sum()
        fb_fetched = (df['facebook_followers'].fillna('') != '').sum()
        print(f"   Instagram followers fetched: {ig_fetched}/{ig_count}")
        print(f"   Facebook followers fetched:  {fb_fetched}/{fb_count}")

    # Step 4: Ensure columns exist in sheet and get updated header list
    print("\n📤 Updating Google Sheet...")
    updated_headers = ensure_columns(worksheet, original_headers)

    # Step 5: Build rows using updated column order, write data rows
    rows_to_write = []
    for _, row in df.iterrows():
        cells = []
        for col in updated_headers:
            val = row.get(col, '')
            if val is None or (isinstance(val, float) and pd.isna(val)):
                val = ''
            cells.append(str(val) if not isinstance(val, str) else val)
        rows_to_write.append(cells)

    # Write data in batches of 500 to avoid API limits
    batch_size = 500
    for i in range(0, len(rows_to_write), batch_size):
        batch = rows_to_write[i:i + batch_size]
        start_row = i + 2  # +1 for 1-indexed, +1 for header row
        worksheet.update(f'A{start_row}', batch)
        print(f"   Wrote rows {start_row} to {start_row + len(batch) - 1}")

    print(f"\n✅ Backfill complete! {len(df)} vendors updated.")
    print(f"\n🔗 View your sheet:")
    print(f"   https://docs.google.com/spreadsheets/d/{sheet_id}/edit")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Backfill social media classification for existing Google Sheet vendors'
    )
    parser.add_argument('--sheet-id', required=True, help='Google Sheet ID to update')
    parser.add_argument(
        '--skip-followers',
        action='store_true',
        help='Skip follower count fetching (classification only)'
    )
    args = parser.parse_args()

    try:
        backfill(args.sheet_id, skip_followers=args.skip_followers)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted. Partial results may have been saved.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
