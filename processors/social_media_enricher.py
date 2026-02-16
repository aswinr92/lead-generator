"""
Social Media Enricher
Fetches follower counts from Instagram and Facebook profiles.
Best-effort approach with caching and graceful failure handling.

NOTE: Instagram aggressively blocks scraping — expect a low hit rate (~20-30%).
      Run with --no-followers for the initial discovery pass, then enrich separately.
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


class SocialMediaEnricher:
    """Enriches vendor data with follower counts from social media profiles."""

    INSTAGRAM_UA = (
        'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) '
        'AppleWebKit/605.1.15 (KHTML, like Gecko) '
        'Version/16.0 Mobile/15E148 Safari/604.1'
    )
    BROWSER_UA = (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    )

    def __init__(self, cache_file: str = 'cache/social_media_cache.json'):
        self.cache_file = Path(cache_file)
        self.cache = self._load_cache()
        self._cache_lock = threading.Lock()
        self._local = threading.local()

    def _get_session(self) -> requests.Session:
        """Thread-local session."""
        if not hasattr(self._local, 'session'):
            session = requests.Session()
            session.headers.update({'Accept-Encoding': 'gzip, deflate, br'})
            self._local.session = session
        return self._local.session

    def _load_cache(self) -> dict:
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {'instagram': {}, 'facebook': {}}

    def _save_cache(self):
        with self._cache_lock:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)

    def _extract_instagram_username(self, url: str) -> Optional[str]:
        match = re.search(r'instagram\.com/([^/?#]+)', url.rstrip('/'))
        if match:
            username = match.group(1)
            if username.lower() not in ('p', 'reel', 'stories', 'explore', 'tv', 'accounts'):
                return username
        return None

    def get_instagram_followers(self, url: str) -> Optional[int]:
        """
        Fetch follower count for a public Instagram profile.
        Returns int or None. Caches both hits and misses.
        """
        username = self._extract_instagram_username(url)
        if not username:
            return None

        cache_key = username.lower()
        with self._cache_lock:
            ig_cache = self.cache.setdefault('instagram', {})
            if cache_key in ig_cache:
                return ig_cache[cache_key]

        followers = None
        try:
            resp = self._get_session().get(
                f'https://www.instagram.com/{username}/',
                headers={
                    'User-Agent': self.INSTAGRAM_UA,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                },
                timeout=6  # reduced from 10s
            )
            if resp.status_code == 200:
                html = resp.text
                for pattern in (
                    r'"follower_count"\s*:\s*(\d+)',
                    r'"edge_followed_by"\s*:\s*\{"count"\s*:\s*(\d+)',
                    r'followers_count":"(\d+)',
                ):
                    m = re.search(pattern, html)
                    if m:
                        followers = int(m.group(1).replace(',', ''))
                        break
        except Exception:
            pass

        with self._cache_lock:
            self.cache.setdefault('instagram', {})[cache_key] = followers
        return followers

    def get_facebook_followers(self, url: str) -> Optional[int]:
        """
        Fetch follower/like count for a public Facebook page.
        Returns int or None. Caches both hits and misses.
        """
        cache_key = url.lower().rstrip('/')
        with self._cache_lock:
            fb_cache = self.cache.setdefault('facebook', {})
            if cache_key in fb_cache:
                return fb_cache[cache_key]

        followers = None
        try:
            resp = self._get_session().get(
                url,
                headers={
                    'User-Agent': self.BROWSER_UA,
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                },
                timeout=6  # reduced from 10s
            )
            if resp.status_code == 200:
                html = resp.text
                for pattern in (
                    r'"follower_count"\s*:\s*(\d+)',
                    r'([\d,]+)\s+(?:people\s+)?follow',
                    r'"followers_count"\s*:\s*(\d+)',
                ):
                    m = re.search(pattern, html, re.IGNORECASE)
                    if m:
                        followers = int(m.group(1).replace(',', ''))
                        break
        except Exception:
            pass

        with self._cache_lock:
            self.cache.setdefault('facebook', {})[cache_key] = followers
        return followers

    def enrich_dataframe(self, df: pd.DataFrame,
                         save_every: int = 20,
                         max_workers: int = 3) -> pd.DataFrame:
        """
        Enrich vendor DataFrame with Instagram and Facebook follower counts.

        Skips rows already enriched (safe to re-run).

        Args:
            df:           DataFrame with 'instagram' and 'facebook' columns
            save_every:   Persist cache every N requests
            max_workers:  Parallel workers (default 3; Instagram rate-limits
                          aggressively so keep this low)

        Returns:
            DataFrame with 'instagram_followers' and 'facebook_followers' filled in
        """
        df = df.copy()

        if 'instagram_followers' not in df.columns:
            df['instagram_followers'] = ''
        if 'facebook_followers' not in df.columns:
            df['facebook_followers'] = ''

        # Only process rows not yet enriched
        ig_rows = [
            idx for idx in df.index[df['instagram'].fillna('') != ''].tolist()
            if str(df.at[idx, 'instagram_followers']).strip() in ('', 'nan', 'None')
        ]
        fb_rows = [
            idx for idx in df.index[df['facebook'].fillna('') != ''].tolist()
            if str(df.at[idx, 'facebook_followers']).strip() in ('', 'nan', 'None')
        ]

        total = len(ig_rows) + len(fb_rows)
        if total == 0:
            print("   No social media profiles to enrich (all already done or empty)")
            return df

        print(f"   Fetching follower counts: {len(ig_rows)} Instagram + {len(fb_rows)} Facebook")
        print(f"   Workers: {max_workers} (Note: Instagram blocks most scraping — expect ~20-30% hit rate)")

        counter_lock = threading.Lock()
        ig_results = {}
        fb_results = {}
        processed_count = [0]
        hit_count = [0]

        def fetch_ig(idx):
            url = str(df.at[idx, 'instagram'])
            time.sleep(random.uniform(0.5, 1.5))  # per-worker rate limiting
            return idx, self.get_instagram_followers(url)

        def fetch_fb(idx):
            url = str(df.at[idx, 'facebook'])
            time.sleep(random.uniform(0.3, 0.8))
            return idx, self.get_facebook_followers(url)

        def _run_batch(items, fetch_fn, results_dict, label):
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(fetch_fn, idx): idx for idx in items}
                for future in as_completed(futures):
                    try:
                        idx, followers = future.result()
                    except Exception:
                        continue

                    results_dict[idx] = followers

                    with counter_lock:
                        processed_count[0] += 1
                        if followers is not None:
                            hit_count[0] += 1
                        count = processed_count[0]
                        if count % save_every == 0:
                            self._save_cache()
                        name = str(df.loc[idx].get('name', ''))[:30]
                        print(
                            f"   [{count:4}/{total}] {label} {name:<32} → {followers or 'N/A'}   ",
                            end='\r'
                        )

        _run_batch(ig_rows, fetch_ig, ig_results, 'IG')
        _run_batch(fb_rows, fetch_fb, fb_results, 'FB')

        # Apply results
        for idx, followers in ig_results.items():
            df.at[idx, 'instagram_followers'] = followers if followers is not None else ''
        for idx, followers in fb_results.items():
            df.at[idx, 'facebook_followers'] = followers if followers is not None else ''

        self._save_cache()
        print(f"\n   ✅ Enrichment complete — {hit_count[0]}/{total} profiles returned a follower count")
        return df
