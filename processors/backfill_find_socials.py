"""
Backfill: Find Social Media Profiles for All Vendors

Comprehensively discovers Instagram and Facebook for every vendor in the
Google Sheet — not just ones that listed it as their "website" on Google Maps.

Discovery methods (in priority order):
  1. Already in sheet  → tagged 'listed'
  2. Scraped from their real website → tagged 'website_link'  (HIGH confidence)
  3. DuckDuckGo search: site:instagram.com "Name" City        (MEDIUM confidence)

After discovery, fetches follower counts for all found profiles.

Usage:
    # Full run (website scraping + search + followers)
    python processors/backfill_find_socials.py --sheet-id YOUR_SHEET_ID

    # Classification + website scraping only (no search, no followers — fastest)
    python processors/backfill_find_socials.py --sheet-id ID --no-search --no-followers

    # Skip website scraping, only search
    python processors/backfill_find_socials.py --sheet-id ID --no-website

    # Re-run followers only (social media already found)
    python processors/backfill_find_socials.py --sheet-id ID --no-website --no-search
"""

import argparse
import sys
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from processors.data_cleaner import VendorDataCleaner
from processors.social_media_finder import SocialMediaFinder
from processors.social_media_enricher import SocialMediaEnricher

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
CREDENTIALS_FILE = 'config/google_credentials.json'

# All new columns this script manages
NEW_COLUMNS = [
    'instagram', 'facebook',
    'instagram_found_via', 'facebook_found_via',
    'website_type', 'digital_presence',
    'instagram_followers', 'facebook_followers',
]


# ------------------------------------------------------------------ #
# Google Sheets helpers
# ------------------------------------------------------------------ #

def authenticate() -> gspread.Client:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def load_sheet(sheet_id: str):
    """Returns (worksheet, DataFrame, original_header_list)."""
    client = authenticate()
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.get_worksheet(0)

    all_values = worksheet.get_all_values()
    if not all_values:
        raise ValueError("Sheet is empty")

    headers = all_values[0]
    rows = all_values[1:]
    df = pd.DataFrame(rows, columns=headers)

    print(f"✅ Loaded {len(df)} vendors  ({len(headers)} columns)")
    return worksheet, df, headers


def ensure_columns(worksheet: gspread.Worksheet, headers: list) -> list:
    """Add any missing NEW_COLUMNS to the sheet header row. Returns full header list."""
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
        print("   All columns already exist in sheet")

    return updated


def write_back(worksheet: gspread.Worksheet, df: pd.DataFrame, headers: list,
               batch_size: int = 500):
    """Write all data rows back to the sheet in batches."""
    rows_to_write = []
    for _, row in df.iterrows():
        cells = []
        for col in headers:
            val = row.get(col, '')
            if val is None or (isinstance(val, float) and pd.isna(val)):
                val = ''
            cells.append(str(val) if not isinstance(val, str) else val)
        rows_to_write.append(cells)

    for i in range(0, len(rows_to_write), batch_size):
        batch = rows_to_write[i:i + batch_size]
        start_row = i + 2  # row 1 = header
        worksheet.update(f'A{start_row}', batch)
        print(f"   Wrote rows {start_row} → {start_row + len(batch) - 1}", end='\r')

    print(f"\n   ✅ {len(rows_to_write)} rows written to sheet")


# ------------------------------------------------------------------ #
# Main backfill logic
# ------------------------------------------------------------------ #

def backfill(sheet_id: str,
             use_website: bool = True,
             use_search: bool = True,
             fetch_followers: bool = True,
             max_workers: int = 5,
             google_api_key: str = None,
             google_cse_id: str = None):

    print("\n" + "=" * 65)
    print("🔍 SOCIAL MEDIA DISCOVERY BACKFILL")
    print("=" * 65)
    print(f"   Website scraping:  {'✓' if use_website else '✗'}")
    print(f"   Search discovery:  {'✓' if use_search else '✗'}")
    print(f"   Follower counts:   {'✓' if fetch_followers else '✗'}")
    print(f"   Workers:           {max_workers}")
    if google_api_key:
        print(f"   Search engine:     Google Custom Search API (fast)")
    else:
        print(f"   Search engine:     DuckDuckGo (free)")

    # ── Step 1: Load data ──────────────────────────────────────────────
    print("\n📥 Loading Google Sheet...")
    worksheet, df, original_headers = load_sheet(sheet_id)

    # ── Step 2: Classify existing URLs ────────────────────────────────
    print("\n🔍 Classifying existing website URLs...")
    cleaner = VendorDataCleaner()
    df = cleaner._classify_and_split_social_media(df)

    already_ig = (df['instagram'].fillna('') != '').sum()
    already_fb = (df['facebook'].fillna('') != '').sum()
    print(f"   Already had Instagram: {already_ig}")
    print(f"   Already had Facebook:  {already_fb}")

    # Mark pre-existing social as 'listed' (if not already marked)
    if 'instagram_found_via' not in df.columns:
        df['instagram_found_via'] = ''
    if 'facebook_found_via' not in df.columns:
        df['facebook_found_via'] = ''

    ig_present = df['instagram'].fillna('') != ''
    fb_present = df['facebook'].fillna('') != ''
    df.loc[ig_present & (df['instagram_found_via'].fillna('') == ''), 'instagram_found_via'] = 'listed'
    df.loc[fb_present & (df['facebook_found_via'].fillna('') == ''), 'facebook_found_via'] = 'listed'

    # ── Step 3: Find social media for all vendors ──────────────────────
    if use_website or use_search:
        print("\n🌐 Discovering social media profiles...")
        finder = SocialMediaFinder(
            google_api_key=google_api_key,
            google_cse_id=google_cse_id,
        )
        df = finder.find_for_dataframe(
            df,
            use_website=use_website,
            use_search=use_search,
            max_workers=max_workers,
        )

    # ── Step 4: Update digital_presence after new social discovered ───
    has_website = df['website'].fillna('').str.startswith('http', na=False)
    has_social = (df['instagram'].fillna('') != '') | (df['facebook'].fillna('') != '')
    df['digital_presence'] = 'none'
    df.loc[has_social & ~has_website, 'digital_presence'] = 'social_only'
    df.loc[has_website, 'digital_presence'] = 'full_website'

    # ── Step 5: Fetch follower counts ─────────────────────────────────
    if fetch_followers:
        print("\n📊 Fetching follower counts...")
        enricher = SocialMediaEnricher()
        df = enricher.enrich_dataframe(df, max_workers=min(max_workers, 3))

    # ── Step 6: Summary ────────────────────────────────────────────────
    new_ig = (df['instagram'].fillna('') != '').sum()
    new_fb = (df['facebook'].fillna('') != '').sum()
    has_followers_ig = (df['instagram_followers'].fillna('').astype(str).str.strip().replace('', None).notna()).sum()
    has_followers_fb = (df['facebook_followers'].fillna('').astype(str).str.strip().replace('', None).notna()).sum()

    ig_by_source = df[df['instagram'].fillna('') != '']['instagram_found_via'].value_counts().to_dict()
    fb_by_source = df[df['facebook'].fillna('') != '']['facebook_found_via'].value_counts().to_dict()

    print("\n" + "─" * 65)
    print("📊 DISCOVERY SUMMARY")
    print("─" * 65)
    print(f"\n   INSTAGRAM")
    print(f"   Before: {already_ig:>5}  →  After: {new_ig:>5}  (+{new_ig - already_ig})")
    for source, count in ig_by_source.items():
        confidence = {'listed': 'high (was in sheet)', 'website_link': 'high', 'maps_profile': 'high', 'search': 'medium — verify'}.get(source, source)
        print(f"     via {source:<16}: {count:>4}  [{confidence}]")

    print(f"\n   FACEBOOK")
    print(f"   Before: {already_fb:>5}  →  After: {new_fb:>5}  (+{new_fb - already_fb})")
    for source, count in fb_by_source.items():
        confidence = {'listed': 'high (was in sheet)', 'website_link': 'high', 'maps_profile': 'high', 'search': 'medium — verify'}.get(source, source)
        print(f"     via {source:<16}: {count:>4}  [{confidence}]")

    digital_counts = df['digital_presence'].value_counts().to_dict()
    print(f"\n   DIGITAL PRESENCE")
    print(f"     Real website:   {digital_counts.get('full_website', 0):>5}")
    print(f"     Social only:    {digital_counts.get('social_only', 0):>5}")
    print(f"     No presence:    {digital_counts.get('none', 0):>5}")

    if fetch_followers:
        print(f"\n   FOLLOWER COUNTS")
        print(f"     Instagram with count: {has_followers_ig}")
        print(f"     Facebook with count:  {has_followers_fb}")

    # ── Step 7: Write back to Google Sheet ────────────────────────────
    print("\n📤 Writing back to Google Sheet...")
    final_headers = ensure_columns(worksheet, original_headers)
    write_back(worksheet, df, final_headers)

    print(f"\n✅ Backfill complete!")
    print(f"\n🔗 https://docs.google.com/spreadsheets/d/{sheet_id}/edit")

    if ig_by_source.get('search', 0) > 0 or fb_by_source.get('search', 0) > 0:
        print(f"\n⚠️  {ig_by_source.get('search', 0) + fb_by_source.get('search', 0)} profiles were found via search")
        print(f"   These are medium-confidence — verify the `instagram_found_via='search'` rows")
        print(f"   in your sheet before using them in outreach.")


# ------------------------------------------------------------------ #
# Entry point
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Find Instagram/Facebook for all vendors and write back to Google Sheet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # RECOMMENDED for 10K vendors: discover only (no followers), 5 parallel workers
  python processors/backfill_find_socials.py --sheet-id ID --no-followers --workers 5

  # With Google CSE (fastest — no delays, ~$5/1000 queries beyond 100 free/day):
  python processors/backfill_find_socials.py --sheet-id ID --no-followers --workers 10
      --google-api-key AIza... --google-cse-id 123...

  # Website scraping only (high-confidence, no search)
  python processors/backfill_find_socials.py --sheet-id ID --no-search --workers 10

  # Just refresh follower counts after discovery is done
  python processors/backfill_find_socials.py --sheet-id ID --no-website --no-search

  # Dry run: discover only, no follower fetching
  python processors/backfill_find_socials.py --sheet-id ID --no-followers
        """
    )
    parser.add_argument('--sheet-id', required=True,
                        help='Google Sheet ID (from the URL)')
    parser.add_argument('--no-website', action='store_true',
                        help='Skip vendor website scraping')
    parser.add_argument('--no-search', action='store_true',
                        help='Skip search discovery')
    parser.add_argument('--no-followers', action='store_true',
                        help='Skip follower count fetching (recommended for initial run)')
    parser.add_argument('--workers', type=int, default=5, metavar='N',
                        help='Parallel worker threads (default: 5). '
                             'Use 10+ with --google-api-key, keep at 3-5 for DuckDuckGo.')
    parser.add_argument('--google-api-key', default=None, metavar='KEY',
                        help='Google Custom Search API key (faster than DuckDuckGo, '
                             'costs ~$5/1000 queries beyond free 100/day tier)')
    parser.add_argument('--google-cse-id', default=None, metavar='CSE_ID',
                        help='Google Custom Search Engine ID (required with --google-api-key). '
                             'Create at https://cse.google.com — set to search the whole web)')
    args = parser.parse_args()

    try:
        backfill(
            sheet_id=args.sheet_id,
            use_website=not args.no_website,
            use_search=not args.no_search,
            fetch_followers=not args.no_followers,
            max_workers=args.workers,
            google_api_key=args.google_api_key,
            google_cse_id=args.google_cse_id,
        )
    except KeyboardInterrupt:
        print('\n\n⚠️  Interrupted — partial results may have been cached locally.')
        print('   Re-run the same command to resume (cached results are reused).')
        sys.exit(1)
    except Exception as e:
        print(f'\n❌ Error: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
