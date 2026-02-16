"""
Social Media Finder
Proactively discovers Instagram and Facebook profiles for vendors via two methods:

1. Website scraping  (HIGH confidence) — fetch the vendor's own website and extract
   social media links from the HTML (header icons, footer links, etc.)

2. DuckDuckGo search (MEDIUM confidence) — search `site:instagram.com "Name" City`
   and extract the best matching profile URL.

3. Google Custom Search API (OPTIONAL, FAST) — requires --google-api-key and
   --google-cse-id. Much faster than DuckDuckGo and no delays needed.
   Setup: https://developers.google.com/custom-search/v1/overview
   Cost: 100 free queries/day, then $5 per 1,000 additional queries.

Results are tagged with `found_via` so you know what needs manual verification:
    'listed'       — vendor put it as their Google Maps website (already in sheet)
    'website_link' — found by scraping their real website (high confidence)
    'search'       — found via DuckDuckGo or Google search (medium confidence, verify)
"""

import re
import json
import time
import random
import threading
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
import pandas as pd


class SocialMediaFinder:
    """
    Finds Instagram and Facebook profiles for wedding vendors.

    Priority order per vendor:
    1. Already in the data (listed)
    2. Scraped from their real website (website_link)
    3. Found via DuckDuckGo or Google search (search)

    Thread-safe: use max_workers > 1 for parallel processing.
    """

    BROWSER_UA = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    )
    MOBILE_UA = (
        'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) '
        'AppleWebKit/605.1.15 (KHTML, like Gecko) '
        'Version/16.0 Mobile/15E148 Safari/604.1'
    )

    # Paths that appear in social media URLs but are NOT profile pages
    IG_NON_PROFILES = {
        'p', 'reel', 'reels', 'stories', 'explore', 'tv', 'accounts',
        'sharer', 'share', 'badges', 'about', 'directory', 'legal',
        'privacy', 'api', 'static', 'graphql'
    }
    FB_NON_PROFILES = {
        'sharer', 'share', 'dialog', 'plugins', 'login', 'oauth', 'ajax',
        'permalink', 'photo', 'video', 'story', 'groups', 'events',
        'marketplace', 'gaming', 'watch', 'notes', 'help', 'about',
        'policies', 'sitemap', 'privacy', 'terms', 'ads', 'static'
    }

    def __init__(self, cache_file: str = 'cache/social_finder_cache.json',
                 search_delay: float = 1.5,
                 google_api_key: str = None,
                 google_cse_id: str = None):
        """
        Args:
            cache_file:      Path for persistent cache (avoids re-fetching)
            search_delay:    Seconds to wait between DuckDuckGo requests (per worker)
            google_api_key:  Google Custom Search API key (optional, much faster)
            google_cse_id:   Google Custom Search Engine ID (required with api key)
        """
        self.cache_file = Path(cache_file)
        self.cache = self._load_cache()
        self.search_delay = search_delay
        self.google_api_key = google_api_key
        self.google_cse_id = google_cse_id

        # Thread-local sessions — each worker thread gets its own HTTP session
        self._local = threading.local()
        # Lock only needed for cache writes (dict reads are safe under GIL)
        self._cache_lock = threading.Lock()

        if google_api_key:
            print(f"   🔍 Search engine: Google Custom Search API (fast)")
        else:
            print(f"   🔍 Search engine: DuckDuckGo (free, {search_delay}s delay per worker)")

    # ------------------------------------------------------------------ #
    # Thread-local session
    # ------------------------------------------------------------------ #

    def _get_session(self) -> requests.Session:
        """Returns a thread-local requests.Session (one per worker thread)."""
        if not hasattr(self._local, 'session'):
            session = requests.Session()
            session.headers.update({
                'User-Agent': self.BROWSER_UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
            })
            self._local.session = session
        return self._local.session

    # ------------------------------------------------------------------ #
    # Cache management
    # ------------------------------------------------------------------ #

    def _load_cache(self) -> dict:
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {'website': {}, 'search_ig': {}, 'search_fb': {}}

    def save_cache(self):
        with self._cache_lock:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)

    # ------------------------------------------------------------------ #
    # URL helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _clean_ig_url(username: str) -> str:
        return f'https://www.instagram.com/{username.strip("/")}/'.lower()

    @staticmethod
    def _clean_fb_url(path: str) -> str:
        path = path.split('?')[0].strip('/')
        return f'https://www.facebook.com/{path}'

    def _extract_ig_username(self, raw: str) -> Optional[str]:
        """Pull the username out of any instagram.com URL."""
        m = re.search(r'instagram\.com/([A-Za-z0-9._]{1,60})/?', raw)
        if m:
            username = m.group(1)
            if username.lower() not in self.IG_NON_PROFILES:
                return username
        return None

    def _extract_fb_path(self, raw: str) -> Optional[str]:
        """Pull the page path out of any facebook.com URL."""
        m = re.search(r'facebook\.com/([A-Za-z0-9._\-]{2,80})/?', raw)
        if m:
            path = m.group(1)
            if path.lower().split('/')[0] not in self.FB_NON_PROFILES:
                return path
        return None

    # ------------------------------------------------------------------ #
    # Method 1: Scrape vendor's real website
    # ------------------------------------------------------------------ #

    def find_from_website(self, website_url: str) -> dict:
        """
        Fetch the vendor's website and extract Instagram/Facebook links.
        Returns: {instagram, facebook, found_via: 'website_link'}
        """
        result = {'instagram': None, 'facebook': None, 'found_via': 'website_link'}

        cache_key = website_url.lower().rstrip('/')

        with self._cache_lock:
            cached = self.cache.get('website', {}).get(cache_key)
        if cached is not None:
            result.update(cached)
            return result

        try:
            resp = self._get_session().get(
                website_url, timeout=5, allow_redirects=True  # reduced from 12s
            )
            if resp.status_code != 200:
                with self._cache_lock:
                    self.cache.setdefault('website', {})[cache_key] = {'instagram': None, 'facebook': None}
                return result

            html = resp.text

            # --- Instagram ---
            ig_matches = re.findall(
                r'href=["\'](?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9._]{1,60})/?["\']',
                html, re.IGNORECASE
            )
            for username in ig_matches:
                if username.lower() not in self.IG_NON_PROFILES:
                    result['instagram'] = self._clean_ig_url(username)
                    break

            # --- Facebook ---
            fb_matches = re.findall(
                r'href=["\'](?:https?://)?(?:www\.)?facebook\.com/([A-Za-z0-9._\-]{2,80})/?(?:\?[^"\']*)?["\']',
                html, re.IGNORECASE
            )
            for path in fb_matches:
                path_clean = path.split('?')[0]
                if path_clean.lower() not in self.FB_NON_PROFILES:
                    result['facebook'] = self._clean_fb_url(path_clean)
                    break

        except Exception:
            pass

        cached_result = {'instagram': result['instagram'], 'facebook': result['facebook']}
        with self._cache_lock:
            self.cache.setdefault('website', {})[cache_key] = cached_result
        return result

    # ------------------------------------------------------------------ #
    # Method 2a: DuckDuckGo search discovery
    # ------------------------------------------------------------------ #

    def _duckduckgo_search(self, query: str) -> str:
        """Return raw HTML from a DuckDuckGo HTML search."""
        try:
            resp = self._get_session().post(
                'https://html.duckduckgo.com/html/',
                data={'q': query, 'b': '', 'kl': 'in-en'},
                headers={'User-Agent': self.BROWSER_UA},
                timeout=8  # reduced from 15s
            )
            return resp.text if resp.status_code == 200 else ''
        except Exception:
            return ''

    # ------------------------------------------------------------------ #
    # Method 2b: Google Custom Search API (optional, faster + no delays)
    # ------------------------------------------------------------------ #

    def _google_cse_search(self, query: str) -> list[str]:
        """
        Query Google Custom Search API. Returns list of result URLs.
        Requires google_api_key and google_cse_id to be set.
        """
        if not self.google_api_key or not self.google_cse_id:
            return []
        try:
            resp = self._get_session().get(
                'https://www.googleapis.com/customsearch/v1',
                params={
                    'key': self.google_api_key,
                    'cx': self.google_cse_id,
                    'q': query,
                    'num': 5,
                },
                timeout=8
            )
            if resp.status_code == 200:
                data = resp.json()
                return [item.get('link', '') for item in data.get('items', [])]
        except Exception:
            pass
        return []

    def find_instagram_via_search(self, name: str, city: str) -> Optional[str]:
        """
        Search for vendor's Instagram profile.
        Uses Google CSE if configured, otherwise DuckDuckGo.
        Returns profile URL (medium confidence) or None.
        """
        cache_key = f"{name.lower()}|{city.lower()}"
        with self._cache_lock:
            ig_cache = self.cache.setdefault('search_ig', {})
            if cache_key in ig_cache:
                return ig_cache[cache_key]

        query = f'site:instagram.com "{name}" {city}'
        result = None

        if self.google_api_key:
            # Google CSE: no delay needed
            urls = self._google_cse_search(query)
            for url in urls:
                username = self._extract_ig_username(url)
                if username:
                    result = self._clean_ig_url(username)
                    break
        else:
            # DuckDuckGo: add delay to avoid rate limiting
            html = self._duckduckgo_search(query)
            if html:
                matches = re.findall(
                    r'instagram\.com/([A-Za-z0-9._]{3,60})/?',
                    html, re.IGNORECASE
                )
                for username in matches:
                    if username.lower() not in self.IG_NON_PROFILES:
                        result = self._clean_ig_url(username)
                        break
            time.sleep(self.search_delay + random.uniform(0, 0.5))

        with self._cache_lock:
            self.cache.setdefault('search_ig', {})[cache_key] = result
        return result

    def find_facebook_via_search(self, name: str, city: str) -> Optional[str]:
        """
        Search for vendor's Facebook page.
        Uses Google CSE if configured, otherwise DuckDuckGo.
        Returns page URL (medium confidence) or None.
        """
        cache_key = f"{name.lower()}|{city.lower()}"
        with self._cache_lock:
            fb_cache = self.cache.setdefault('search_fb', {})
            if cache_key in fb_cache:
                return fb_cache[cache_key]

        query = f'site:facebook.com "{name}" {city}'
        result = None

        if self.google_api_key:
            urls = self._google_cse_search(query)
            for url in urls:
                path = self._extract_fb_path(url)
                if path:
                    result = self._clean_fb_url(path)
                    break
        else:
            html = self._duckduckgo_search(query)
            if html:
                matches = re.findall(
                    r'facebook\.com/([A-Za-z0-9._\-]{2,80})/?(?:\?[^"\s]*)?',
                    html, re.IGNORECASE
                )
                for path in matches:
                    path_clean = path.split('?')[0]
                    if path_clean.lower() not in self.FB_NON_PROFILES:
                        result = self._clean_fb_url(path_clean)
                        break
            time.sleep(self.search_delay + random.uniform(0, 0.5))

        with self._cache_lock:
            self.cache.setdefault('search_fb', {})[cache_key] = result
        return result

    # ------------------------------------------------------------------ #
    # High-level: find for a single vendor row
    # ------------------------------------------------------------------ #

    def find_for_vendor(self, row: pd.Series,
                        use_website: bool = True,
                        use_search: bool = True) -> dict:
        """
        Find Instagram and Facebook for a single vendor.
        Thread-safe: safe to call from multiple worker threads simultaneously.

        Returns:
            {
              'instagram':           url or '',
              'facebook':            url or '',
              'instagram_found_via': 'listed'|'website_link'|'search'|'',
              'facebook_found_via':  'listed'|'website_link'|'search'|'',
            }
        """
        result = {
            'instagram': str(row.get('instagram') or '').strip(),
            'facebook': str(row.get('facebook') or '').strip(),
            'instagram_found_via': '',
            'facebook_found_via': '',
        }

        if result['instagram']:
            result['instagram_found_via'] = 'listed'
        if result['facebook']:
            result['facebook_found_via'] = 'listed'

        website = str(row.get('website') or '').strip()
        name = str(row.get('name') or '').strip()
        city = str(row.get('city') or '').strip()

        # Method 1: scrape their real website
        if use_website and website.startswith('http'):
            ws_result = self.find_from_website(website)
            if not result['instagram'] and ws_result.get('instagram'):
                result['instagram'] = ws_result['instagram']
                result['instagram_found_via'] = 'website_link'
            if not result['facebook'] and ws_result.get('facebook'):
                result['facebook'] = ws_result['facebook']
                result['facebook_found_via'] = 'website_link'

        # Method 2: search (only if still missing and name/city available)
        if use_search and name and city:
            if not result['instagram']:
                ig_url = self.find_instagram_via_search(name, city)
                if ig_url:
                    result['instagram'] = ig_url
                    result['instagram_found_via'] = 'search'

            if not result['facebook']:
                fb_url = self.find_facebook_via_search(name, city)
                if fb_url:
                    result['facebook'] = fb_url
                    result['facebook_found_via'] = 'search'

        return result

    # ------------------------------------------------------------------ #
    # High-level: find for whole DataFrame (parallel)
    # ------------------------------------------------------------------ #

    def find_for_dataframe(self, df: pd.DataFrame,
                           use_website: bool = True,
                           use_search: bool = True,
                           save_every: int = 50,
                           max_workers: int = 1) -> pd.DataFrame:
        """
        Run social media discovery for all vendors in a DataFrame.

        Skips vendors that already have both instagram and facebook populated.
        Safe to re-run (cached + skips complete rows).

        Args:
            df:           Vendor DataFrame
            use_website:  Scrape vendor websites for social links
            use_search:   Use search (DuckDuckGo or Google CSE) for missing profiles
            save_every:   Persist cache every N vendors processed
            max_workers:  Parallel worker threads (default 1 = sequential)
                          Recommended: 5 for DuckDuckGo, 10+ for Google CSE

        Returns:
            Updated DataFrame with new/updated social media columns
        """
        df = df.copy()

        for col in ('instagram', 'facebook', 'instagram_found_via', 'facebook_found_via'):
            if col not in df.columns:
                df[col] = ''

        needs_ig = df['instagram'].fillna('') == ''
        needs_fb = df['facebook'].fillna('') == ''
        needs_work = needs_ig | needs_fb
        work_idx = df.index[needs_work].tolist()

        has_website = (df['website'].fillna('').str.startswith('http')).sum()
        total = len(work_idx)

        engine = 'Google CSE' if self.google_api_key else 'DuckDuckGo'
        print(f"\n   Vendors to process:     {total}")
        print(f"   Already have Instagram: {(~needs_ig).sum()}")
        print(f"   Already have Facebook:  {(~needs_fb).sum()}")
        print(f"   Have real website:      {has_website}")
        print(f"   Method 1 (website scrape): {'enabled' if use_website else 'disabled'}")
        print(f"   Method 2 (search/{engine}): {'enabled' if use_search else 'disabled'}")
        print(f"   Workers:                {max_workers}")

        if total == 0:
            print("   Nothing to process — all vendors already have social media.")
            return df

        found_ig = 0
        found_fb = 0
        counter_lock = threading.Lock()
        results = {}
        processed_count = [0]

        def process_vendor(idx):
            row = df.loc[idx]
            return idx, self.find_for_vendor(row, use_website=use_website, use_search=use_search)

        if max_workers <= 1:
            # Sequential mode
            for i, idx in enumerate(work_idx, 1):
                row = df.loc[idx]
                vendor_result = self.find_for_vendor(row, use_website=use_website, use_search=use_search)

                df.at[idx, 'instagram'] = vendor_result['instagram']
                df.at[idx, 'facebook'] = vendor_result['facebook']
                df.at[idx, 'instagram_found_via'] = vendor_result['instagram_found_via']
                df.at[idx, 'facebook_found_via'] = vendor_result['facebook_found_via']

                if vendor_result['instagram']:
                    found_ig += 1
                if vendor_result['facebook']:
                    found_fb += 1

                name = str(row.get('name', ''))[:30]
                status_ig = vendor_result['instagram_found_via'] or '—'
                status_fb = vendor_result['facebook_found_via'] or '—'
                print(f"   [{i:4}/{total}] {name:<32} IG:{status_ig:<14} FB:{status_fb}", end='\r')

                if i % save_every == 0:
                    self.save_cache()

        else:
            # Parallel mode
            print(f"   Running {max_workers} workers in parallel...\n")

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_vendor, idx): idx for idx in work_idx}

                for future in as_completed(futures):
                    try:
                        idx, vendor_result = future.result()
                    except Exception as e:
                        continue

                    results[idx] = vendor_result

                    with counter_lock:
                        processed_count[0] += 1
                        count = processed_count[0]
                        if vendor_result['instagram']:
                            found_ig += 1
                        if vendor_result['facebook']:
                            found_fb += 1

                        if count % save_every == 0:
                            self.save_cache()

                        name = str(df.loc[idx].get('name', ''))[:28]
                        status_ig = vendor_result['instagram_found_via'] or '—'
                        status_fb = vendor_result['facebook_found_via'] or '—'
                        print(
                            f"   [{count:4}/{total}] {name:<30} "
                            f"IG:{status_ig:<14} FB:{status_fb}   ",
                            end='\r'
                        )

            # Apply all results to DataFrame (single-threaded, no lock needed)
            for idx, vendor_result in results.items():
                df.at[idx, 'instagram'] = vendor_result['instagram']
                df.at[idx, 'facebook'] = vendor_result['facebook']
                df.at[idx, 'instagram_found_via'] = vendor_result['instagram_found_via']
                df.at[idx, 'facebook_found_via'] = vendor_result['facebook_found_via']

        self.save_cache()
        print(f"\n\n   ✅ Discovery complete")
        print(f"   Instagram found: {found_ig} new profiles")
        print(f"   Facebook found:  {found_fb} new profiles")

        return df
