"""
google_search.py — Find Instagram profile URLs via Google Search.

Strategy (in priority order):
  1. Google Custom Search JSON API  — reliable, no blocking, ~$5/1000 queries
     Requires: GOOGLE_API_KEY + GOOGLE_CSE_ID env vars or constructor args.
  2. Requests-based google.com/search scraping — free, polite delays applied.
     Respects 3–7 s inter-query delay per the spec.

Results are deduplicated instagram.com profile URLs (not reels/posts/explore).
"""

import re
import time
import random
import logging
import requests
from typing import Optional

log = logging.getLogger(__name__)

# Instagram paths that are NOT profile pages
_IG_NON_PROFILES = {
    'p', 'reel', 'reels', 'stories', 'explore', 'tv', 'accounts',
    'sharer', 'share', 'badges', 'about', 'directory', 'legal',
    'privacy', 'api', 'static', 'graphql', 'instagram',
}

_BROWSER_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/122.0.0.0 Safari/537.36'
)

# Regex to extract instagram.com/username from any text/HTML
_IG_URL_RE = re.compile(
    r'(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9._]{3,60})/?',
    re.IGNORECASE
)

# Google redirect URL pattern in search result HTML
_GOOGLE_HREF_RE = re.compile(r'/url\?q=(https?://[^&"]+)')


def _clean_ig_url(username: str) -> Optional[str]:
    """Return canonical profile URL or None if username is a non-profile path."""
    u = username.strip('/"\'').split('?')[0].lower()
    if u in _IG_NON_PROFILES or len(u) < 3:
        return None
    return f'https://www.instagram.com/{u}/'


def _extract_ig_urls_from_text(text: str) -> list[str]:
    """Pull all unique Instagram profile URLs out of arbitrary text/HTML."""
    seen = set()
    results = []
    for username in _IG_URL_RE.findall(text):
        url = _clean_ig_url(username)
        if url and url not in seen:
            seen.add(url)
            results.append(url)
    return results


class GoogleSearcher:
    """
    Searches Google for Instagram profile URLs for a given vendor.

    Args:
        api_key:   Google Custom Search API key (optional)
        cse_id:    Google Custom Search Engine ID (optional, required with api_key)
        min_delay: Minimum seconds between search requests (default 3)
        max_delay: Maximum seconds between search requests (default 7)
    """

    def __init__(self,
                 api_key: str = None,
                 cse_id: str = None,
                 min_delay: float = 3.0,
                 max_delay: float = 7.0):
        self.api_key = api_key
        self.cse_id = cse_id
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._session = self._make_session()
        self._last_request_at: float = 0.0

        if api_key:
            log.info('GoogleSearcher: using Google Custom Search API')
        else:
            log.info('GoogleSearcher: using google.com HTML scraping (polite delays)')

    # ------------------------------------------------------------------ #
    # Session
    # ------------------------------------------------------------------ #

    @staticmethod
    def _make_session() -> requests.Session:
        session = requests.Session()
        session.headers.update({
            'User-Agent': _BROWSER_UA,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.google.com/',
        })
        return session

    # ------------------------------------------------------------------ #
    # Rate limiting
    # ------------------------------------------------------------------ #

    def _polite_delay(self):
        """Sleep so that requests are at least min_delay seconds apart."""
        elapsed = time.time() - self._last_request_at
        gap = random.uniform(self.min_delay, self.max_delay)
        if elapsed < gap:
            time.sleep(gap - elapsed)
        self._last_request_at = time.time()

    # ------------------------------------------------------------------ #
    # Google Custom Search API
    # ------------------------------------------------------------------ #

    def _cse_search(self, query: str) -> list[str]:
        """
        Query Google Custom Search JSON API.
        Returns list of result URLs (up to 10).
        """
        try:
            self._polite_delay()
            resp = self._session.get(
                'https://www.googleapis.com/customsearch/v1',
                params={
                    'key': self.api_key,
                    'cx': self.cse_id,
                    'q': query,
                    'num': 10,
                    'gl': 'in',
                    'hl': 'en',
                },
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                return [item['link'] for item in data.get('items', []) if 'link' in item]
            elif resp.status_code == 429:
                log.warning('Google CSE: rate limited — backing off 30s')
                time.sleep(30)
            else:
                log.warning(f'Google CSE: HTTP {resp.status_code} for query: {query}')
        except Exception as e:
            log.debug(f'Google CSE error: {e}')
        return []

    # ------------------------------------------------------------------ #
    # Google HTML scraping fallback
    # ------------------------------------------------------------------ #

    def _html_search(self, query: str) -> list[str]:
        """
        Scrape google.com/search results page for URLs.
        Returns list of result URLs extracted from href attributes.
        """
        try:
            self._polite_delay()
            resp = self._session.get(
                'https://www.google.com/search',
                params={'q': query, 'num': 10, 'hl': 'en', 'gl': 'in'},
                timeout=12,
            )
            if resp.status_code == 200:
                # Google encodes result URLs as /url?q=https://...
                raw_urls = _GOOGLE_HREF_RE.findall(resp.text)
                # Decode URL encoding
                from urllib.parse import unquote
                return [unquote(u) for u in raw_urls]
            elif resp.status_code == 429 or 'sorry' in resp.url.lower():
                log.warning('Google HTML: rate limited / CAPTCHA — backing off 60s')
                time.sleep(60)
            else:
                log.warning(f'Google HTML: HTTP {resp.status_code}')
        except Exception as e:
            log.debug(f'Google HTML error: {e}')
        return []

    # ------------------------------------------------------------------ #
    # Public interface
    # ------------------------------------------------------------------ #

    def search_one(self, query: str) -> list[str]:
        """
        Run a single search query and return all URLs found.
        Uses CSE if configured, otherwise HTML scraping.
        """
        log.debug(f'Searching: {query}')
        if self.api_key and self.cse_id:
            return self._cse_search(query)
        return self._html_search(query)

    def find_instagram_candidates(self, queries: list[str],
                                   max_candidates: int = 5) -> list[str]:
        """
        Run multiple search queries and return unique Instagram profile URLs.

        Args:
            queries:        List of search query strings (from normalize.search_variants)
            max_candidates: Stop collecting after this many unique profiles found

        Returns:
            Ordered list of unique instagram.com profile URLs (best match first).
        """
        seen: set[str] = set()
        candidates: list[str] = []

        for query in queries:
            if len(candidates) >= max_candidates:
                break

            urls = self.search_one(query)
            new_ig = _extract_ig_urls_from_text(' '.join(urls))

            for url in new_ig:
                if url not in seen:
                    seen.add(url)
                    candidates.append(url)
                    log.debug(f'  → candidate: {url}')

            if new_ig:
                # Found results for this query — no need to try weaker queries
                break

        log.info(f'Search returned {len(candidates)} candidate(s)')
        return candidates[:max_candidates]
