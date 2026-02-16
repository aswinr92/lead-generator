"""
Backfill Social Media from Google Maps Business Profiles

Re-visits each vendor's Google Maps business page with Selenium and extracts
Instagram/Facebook links directly from their Google Business Profile.

Why this works:
  - We already have the Google Maps URL for every vendor (the `url` column)
  - Google Business Profile allows owners to link their Instagram/Facebook
  - These social links appear in the rendered page HTML (same data as
    Google Search Knowledge Panel)
  - Much more reliable than DuckDuckGo for Indian business profiles

Usage:
    # Full run — all vendors missing Instagram
    python processors/backfill_from_maps.py --sheet-id YOUR_SHEET_ID

    # Test on first 50 vendors (no sheet write)
    python processors/backfill_from_maps.py --sheet-id ID --limit 50 --dry-run

    # Visible browser (useful for debugging)
    python processors/backfill_from_maps.py --sheet-id ID --no-headless

    # Re-run only vendors with no Instagram AND no Facebook
    python processors/backfill_from_maps.py --sheet-id ID --missing-both
"""

import re
import json
import time
import random
import argparse
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
try:
    from fake_useragent import UserAgent
    _HAS_FAKE_UA = True
except ImportError:
    _HAS_FAKE_UA = False

sys.path.insert(0, str(Path(__file__).parent.parent))

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
CREDENTIALS_FILE = 'config/google_credentials.json'

# Non-profile paths to ignore when extracting social URLs
IG_NON_PROFILES = {
    'p', 'reel', 'reels', 'stories', 'explore', 'tv', 'accounts',
    'sharer', 'share', 'badges', 'about', 'directory', 'legal',
    'privacy', 'api', 'static', 'graphql', 'instagram'
}
FB_NON_PROFILES = {
    'sharer', 'share', 'dialog', 'plugins', 'login', 'oauth', 'ajax',
    'permalink', 'photo', 'video', 'story', 'groups', 'events',
    'marketplace', 'gaming', 'watch', 'notes', 'help', 'about',
    'policies', 'sitemap', 'privacy', 'terms', 'ads', 'static', 'facebook'
}


# ------------------------------------------------------------------ #
# Chrome driver setup (reuses same pattern as main scraper)
# ------------------------------------------------------------------ #

def _setup_driver(headless: bool = True) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-images')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    prefs = {
        'profile.managed_default_content_settings.images': 2,
        'profile.default_content_setting_values.notifications': 2,
    }
    options.add_experimental_option('prefs', prefs)

    if _HAS_FAKE_UA:
        ua = UserAgent()
        options.add_argument(f'user-agent={ua.random}')
    else:
        options.add_argument(
            'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


# ------------------------------------------------------------------ #
# Social media extraction from rendered HTML
# ------------------------------------------------------------------ #

def extract_social_from_html(html: str) -> dict:
    """
    Scan rendered page HTML for Instagram and Facebook profile links.
    Returns {'instagram': url_or_none, 'facebook': url_or_none}.
    """
    result = {'instagram': None, 'facebook': None}

    # Instagram: look for instagram.com/username patterns
    ig_matches = re.findall(
        r'(?:href=["\'])?(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9._]{3,60})/?["\']?',
        html, re.IGNORECASE
    )
    for username in ig_matches:
        username_clean = username.strip('/"\'').split('?')[0]
        if username_clean.lower() not in IG_NON_PROFILES and len(username_clean) >= 3:
            result['instagram'] = f'https://www.instagram.com/{username_clean.lower()}/'
            break

    # Facebook: look for facebook.com/page patterns
    fb_matches = re.findall(
        r'(?:href=["\'])?(?:https?://)?(?:www\.)?facebook\.com/([A-Za-z0-9._\-]{2,80})/?["\']?',
        html, re.IGNORECASE
    )
    for path in fb_matches:
        path_clean = path.strip('/"\'').split('?')[0]
        if path_clean.lower() not in FB_NON_PROFILES and len(path_clean) >= 2:
            result['facebook'] = f'https://www.facebook.com/{path_clean}'
            break

    return result


def scrape_vendor_maps_page(driver: webdriver.Chrome, maps_url: str,
                             page_wait: float = 2.5) -> dict:
    """
    Visit a vendor's Google Maps business page and extract social media links.

    Returns: {'instagram': url_or_none, 'facebook': url_or_none, 'error': bool}
    """
    result = {'instagram': None, 'facebook': None, 'error': False}

    if not maps_url or not str(maps_url).startswith('http'):
        result['error'] = True
        return result

    try:
        driver.get(str(maps_url))
        time.sleep(page_wait)  # Wait for JS to render

        # Try to click "About" tab if present — social profiles live there
        try:
            about_tab = driver.find_element(
                By.XPATH,
                '//button[@role="tab" and contains(., "About")] | '
                '//div[@role="tab" and contains(., "About")]'
            )
            about_tab.click()
            time.sleep(1.0)
        except Exception:
            pass  # No About tab — use the current page HTML

        html = driver.page_source
        social = extract_social_from_html(html)
        result.update(social)

    except WebDriverException as e:
        if 'net::ERR' in str(e) or 'timeout' in str(e).lower():
            result['error'] = True
        # Other WebDriver errors: log but don't crash

    return result


# ------------------------------------------------------------------ #
# Google Sheets helpers
# ------------------------------------------------------------------ #

def authenticate() -> gspread.Client:
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def load_sheet(sheet_id: str):
    """Returns (worksheet, DataFrame, headers)."""
    client = authenticate()
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.get_worksheet(0)
    all_values = worksheet.get_all_values()
    if not all_values:
        raise ValueError('Sheet is empty')
    headers = all_values[0]
    df = pd.DataFrame(all_values[1:], columns=headers)
    print(f'✅ Loaded {len(df)} vendors  ({len(headers)} columns)')
    return worksheet, df, headers


def write_back(worksheet: gspread.Worksheet, df: pd.DataFrame,
               headers: list, batch_size: int = 500):
    """Write all rows back to the sheet."""
    rows = []
    for _, row in df.iterrows():
        cells = []
        for col in headers:
            val = row.get(col, '')
            if val is None or (isinstance(val, float) and pd.isna(val)):
                val = ''
            cells.append(str(val) if not isinstance(val, str) else val)
        rows.append(cells)

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        start_row = i + 2
        worksheet.update(values=batch, range_name=f'A{start_row}')
        print(f'   Wrote rows {start_row}–{start_row + len(batch) - 1}', end='\r')

    print(f'\n   ✅ {len(rows)} rows written')


# ------------------------------------------------------------------ #
# Cache helpers (so it's safe to interrupt and resume)
# ------------------------------------------------------------------ #

CACHE_FILE = Path('cache/maps_social_cache.json')


def load_maps_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_maps_cache(cache: dict):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ------------------------------------------------------------------ #
# Main backfill logic
# ------------------------------------------------------------------ #

def backfill(sheet_id: str,
             headless: bool = True,
             missing_both: bool = False,
             limit: int = None,
             dry_run: bool = False,
             page_wait: float = 2.5,
             save_every: int = 25):

    print('\n' + '=' * 65)
    print('📍 GOOGLE MAPS SOCIAL MEDIA BACKFILL')
    print('=' * 65)
    print(f'   Headless:     {headless}')
    print(f'   Dry run:      {dry_run}')
    print(f'   Missing both: {missing_both} (only process vendors with no IG AND no FB)')
    if limit:
        print(f'   Limit:        {limit} vendors')

    # Load sheet
    print('\n📥 Loading Google Sheet...')
    worksheet, df, headers = load_sheet(sheet_id)

    # Ensure social columns exist in df
    for col in ('instagram', 'facebook', 'instagram_found_via', 'facebook_found_via'):
        if col not in df.columns:
            df[col] = ''

    # Identify which vendors to process
    has_ig = df['instagram'].fillna('') != ''
    has_fb = df['facebook'].fillna('') != ''
    has_maps_url = df['url'].fillna('').str.startswith('http')

    if missing_both:
        needs_work = (~has_ig) & (~has_fb) & has_maps_url
    else:
        needs_work = (~has_ig) & has_maps_url  # missing Instagram (most valuable)

    work_idx = df.index[needs_work].tolist()
    if limit:
        work_idx = work_idx[:limit]

    total = len(work_idx)
    print(f'\n   Total vendors:          {len(df)}')
    print(f'   Have Instagram:         {has_ig.sum()}')
    print(f'   Have Facebook:          {has_fb.sum()}')
    print(f'   Have Maps URL:          {has_maps_url.sum()}')
    print(f'   To process (missing IG):  {total}')

    if total == 0:
        print('\n✅ Nothing to do — all vendors already have Instagram.')
        return

    # Load cache (allows resuming interrupted runs)
    cache = load_maps_cache()
    cached_hits = sum(1 for k in cache if cache[k].get('instagram') or cache[k].get('facebook'))
    print(f'\n   Cache: {len(cache)} entries ({cached_hits} with social found)')

    # Start Selenium
    print('\n🌐 Starting Chrome...')
    driver = _setup_driver(headless=headless)

    found_ig = 0
    found_fb = 0
    errors = 0

    try:
        for i, idx in enumerate(work_idx, 1):
            row = df.loc[idx]
            maps_url = str(row.get('url', '')).strip()
            name = str(row.get('name', ''))[:40]
            cache_key = maps_url.lower().rstrip('/')

            # Use cache if available
            if cache_key in cache:
                cached = cache[cache_key]
                ig = cached.get('instagram') or ''
                fb = cached.get('facebook') or ''
            else:
                # Scrape the Maps page
                result = scrape_vendor_maps_page(driver, maps_url, page_wait=page_wait)
                ig = result['instagram'] or ''
                fb = result['facebook'] or ''
                if result['error']:
                    errors += 1
                cache[cache_key] = {'instagram': ig, 'facebook': fb}

            # Update DataFrame (only fill in missing values)
            updated = False
            if ig and not df.at[idx, 'instagram']:
                df.at[idx, 'instagram'] = ig
                df.at[idx, 'instagram_found_via'] = 'maps_profile'
                found_ig += 1
                updated = True
            if fb and not df.at[idx, 'facebook']:
                df.at[idx, 'facebook'] = fb
                df.at[idx, 'facebook_found_via'] = 'maps_profile'
                found_fb += 1
                updated = True

            status = f"IG:{ig.split('instagram.com/')[-1].rstrip('/') if ig else '—':<18} FB:{'✓' if fb else '—'}"
            marker = '✓' if updated else ' '
            print(f'   [{i:4}/{total}] {marker} {name:<40} {status}', end='\r')

            # Periodic cache save (resumable)
            if i % save_every == 0:
                save_maps_cache(cache)

            # Small random delay to avoid looking like a bot
            time.sleep(random.uniform(0.3, 0.8))

    except KeyboardInterrupt:
        print('\n\n⚠️  Interrupted — saving cache...')
    finally:
        save_maps_cache(cache)
        driver.quit()
        print('\n')

    # Summary
    print('─' * 65)
    print('📊 MAPS BACKFILL SUMMARY')
    print('─' * 65)
    print(f'   Vendors processed:  {i}')
    print(f'   Instagram found:    {found_ig}')
    print(f'   Facebook found:     {found_fb}')
    print(f'   Errors/skipped:     {errors}')

    dp = df['digital_presence'].value_counts().to_dict() if 'digital_presence' in df.columns else {}
    if dp:
        print(f'\n   Digital presence after:')
        print(f'     Real website:  {dp.get("full_website", 0)}')
        print(f'     Social only:   {dp.get("social_only", 0)}')
        print(f'     No presence:   {dp.get("none", 0)}')

    if dry_run:
        print('\n⚠️  DRY RUN — no changes written to sheet.')
        print(f'   Would have written {found_ig} Instagram + {found_fb} Facebook profiles.')
        return

    if found_ig + found_fb == 0:
        print('\n   No new social profiles found — sheet unchanged.')
        return

    # Update digital_presence column
    has_website = df['website'].fillna('').str.startswith('http', na=False)
    has_social = (df['instagram'].fillna('') != '') | (df['facebook'].fillna('') != '')
    df['digital_presence'] = 'none'
    df.loc[has_social & ~has_website, 'digital_presence'] = 'social_only'
    df.loc[has_website, 'digital_presence'] = 'full_website'

    print('\n📤 Writing back to Google Sheet...')
    write_back(worksheet, df, headers)

    print(f'\n✅ Done! Found {found_ig} Instagram + {found_fb} Facebook profiles from Google Maps.')
    print(f'\n🔗 https://docs.google.com/spreadsheets/d/{sheet_id}/edit')
    print('\n💡 Tip: Run backfill_find_socials.py --no-website --no-search next to get')
    print('        follower counts for newly discovered profiles.')


# ------------------------------------------------------------------ #
# Entry point
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Extract social media links from vendor Google Maps business profiles',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Standard run — all vendors missing Instagram
  python processors/backfill_from_maps.py --sheet-id YOUR_SHEET_ID

  # Test on first 50 vendors without writing to sheet
  python processors/backfill_from_maps.py --sheet-id ID --limit 50 --dry-run

  # Only vendors with no Instagram AND no Facebook
  python processors/backfill_from_maps.py --sheet-id ID --missing-both

  # Debug with visible browser
  python processors/backfill_from_maps.py --sheet-id ID --no-headless --limit 10

  # Faster page loading (less reliable) / Slower (more reliable)
  python processors/backfill_from_maps.py --sheet-id ID --page-wait 1.5
  python processors/backfill_from_maps.py --sheet-id ID --page-wait 4

Note: Safe to Ctrl+C and resume — progress is cached in cache/maps_social_cache.json
        """
    )
    parser.add_argument('--sheet-id', required=True)
    parser.add_argument('--no-headless', action='store_true',
                        help='Show browser window (useful for debugging)')
    parser.add_argument('--missing-both', action='store_true',
                        help='Only process vendors missing both Instagram AND Facebook')
    parser.add_argument('--limit', type=int, default=None, metavar='N',
                        help='Process only the first N vendors (for testing)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Find profiles but do NOT write back to sheet')
    parser.add_argument('--page-wait', type=float, default=2.5, metavar='SEC',
                        help='Seconds to wait for page JS to render (default: 2.5)')
    args = parser.parse_args()

    try:
        backfill(
            sheet_id=args.sheet_id,
            headless=not args.no_headless,
            missing_both=args.missing_both,
            limit=args.limit,
            dry_run=args.dry_run,
            page_wait=args.page_wait,
        )
    except KeyboardInterrupt:
        print('\n\n⚠️  Interrupted. Re-run the same command to resume.')
        sys.exit(1)
    except Exception as e:
        print(f'\n❌ Error: {e}')
        import traceback
        traceback.print_exc()
        sys.exit(1)
