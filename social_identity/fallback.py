"""
fallback.py — Last-resort Instagram discovery when Google Search finds nothing.

Two methods, both using requests only (no Selenium):

  Method A — Vendor website scraping
    Fetch the vendor's own website HTML and look for instagram.com links.
    High confidence when found (business listed it themselves).

  Method B — Google Maps URL (requests-based)
    Fetch the Google Maps business page HTML.
    Limited effectiveness without JavaScript rendering — Google Maps is
    a JS app — but some social data appears in JSON-LD schema blocks
    embedded in the initial HTML load.

Returns the same profile dict shape as extract_profile.fetch_instagram_profile,
with an extra 'source' key: 'fallback_website' or 'fallback_maps'.
"""

import re
import logging
import requests
from typing import Optional

from social_identity.extract_profile import (
    fetch_instagram_profile,
    username_from_url,
    _MOBILE_UA,
)

log = logging.getLogger(__name__)

_BROWSER_UA = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/122.0.0.0 Safari/537.36'
)

_IG_NON_PROFILES = {
    'p', 'reel', 'reels', 'stories', 'explore', 'tv', 'accounts',
    'sharer', 'share', 'badges', 'about', 'directory', 'legal',
    'privacy', 'api', 'static', 'graphql', 'instagram',
}

# Instagram URL in href attribute
_IG_HREF_RE = re.compile(
    r'href=["\'](?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9._]{3,60})/?["\']',
    re.IGNORECASE
)
# Instagram URL in JSON-LD or plain text
_IG_PLAIN_RE = re.compile(
    r'(?:https?://)?(?:www\.)?instagram\.com/([A-Za-z0-9._]{3,60})/?',
    re.IGNORECASE
)


def _first_valid_ig_username(html: str) -> Optional[str]:
    """Extract first valid Instagram username from HTML."""
    # Prefer href matches (direct links)
    for pattern in (_IG_HREF_RE, _IG_PLAIN_RE):
        for username in pattern.findall(html):
            u = username.lower().strip('/"\'').split('?')[0]
            if u not in _IG_NON_PROFILES and len(u) >= 3:
                return u
    return None


# ------------------------------------------------------------------ #
# Method A — vendor website
# ------------------------------------------------------------------ #

def find_from_website(website_url: str,
                       session: Optional[requests.Session] = None) -> Optional[dict]:
    """
    Scrape the vendor's real website and return a profile dict if Instagram found.
    Returns None if no Instagram link found or website is unreachable.
    """
    if not website_url or not str(website_url).strip().startswith('http'):
        return None

    if session is None:
        session = requests.Session()

    try:
        resp = session.get(
            website_url,
            headers={
                'User-Agent': _BROWSER_UA,
                'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            },
            timeout=6,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None

        username = _first_valid_ig_username(resp.text)
        if not username:
            return None

        ig_url = f'https://www.instagram.com/{username}/'
        log.debug(f'Fallback website found: {ig_url}')

        # Fetch the profile to get full metadata
        profile = fetch_instagram_profile(ig_url, session=session)
        if profile:
            profile['source'] = 'fallback_website'
        return profile

    except Exception as e:
        log.debug(f'Website fallback error for {website_url}: {e}')
        return None


# ------------------------------------------------------------------ #
# Method B — Google Maps business page (requests, no JS)
# ------------------------------------------------------------------ #

def find_from_maps(maps_url: str,
                    session: Optional[requests.Session] = None) -> Optional[dict]:
    """
    Fetch a Google Maps business page with plain requests and look for
    Instagram links in the initial HTML (JSON-LD, schema.org blocks).

    Low success rate without JavaScript rendering, but catches businesses
    that have embedded their social profiles in Google's structured data.
    Returns None if nothing found.
    """
    if not maps_url or not str(maps_url).strip().startswith('http'):
        return None

    if session is None:
        session = requests.Session()

    try:
        resp = session.get(
            maps_url,
            headers={
                'User-Agent': _BROWSER_UA,
                'Accept': 'text/html,application/xhtml+xml,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
            },
            timeout=8,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None

        html = resp.text

        # Look in JSON-LD blocks first (most structured)
        json_ld_blocks = re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.DOTALL | re.IGNORECASE
        )
        for block in json_ld_blocks:
            username = _first_valid_ig_username(block)
            if username:
                ig_url = f'https://www.instagram.com/{username}/'
                log.debug(f'Fallback maps (JSON-LD) found: {ig_url}')
                profile = fetch_instagram_profile(ig_url, session=session)
                if profile:
                    profile['source'] = 'fallback_maps'
                return profile

        # Scan full HTML as last resort
        username = _first_valid_ig_username(html)
        if username:
            ig_url = f'https://www.instagram.com/{username}/'
            log.debug(f'Fallback maps (HTML scan) found: {ig_url}')
            profile = fetch_instagram_profile(ig_url, session=session)
            if profile:
                profile['source'] = 'fallback_maps'
            return profile

    except Exception as e:
        log.debug(f'Maps fallback error for {maps_url}: {e}')

    return None


# ------------------------------------------------------------------ #
# Combined fallback — try website first, then Maps
# ------------------------------------------------------------------ #

def run_fallback(vendor: dict,
                  session: Optional[requests.Session] = None) -> Optional[dict]:
    """
    Try all fallback methods for a vendor and return the first profile found.

    Priority:
      1. Vendor's own website (most reliable — they put the link there)
      2. Google Maps business page (low hit rate without JS but worth trying)

    Returns profile dict with 'source' key, or None if nothing found.
    """
    if session is None:
        session = requests.Session()

    # Method A: website
    website = vendor.get('website', '') or ''
    if website.startswith('http'):
        profile = find_from_website(website, session=session)
        if profile:
            log.info(f"Fallback hit (website) for {vendor.get('business_name') or vendor.get('name')}")
            return profile

    # Method B: Google Maps
    maps_url = vendor.get('google_maps_url', '') or vendor.get('url', '') or ''
    if maps_url.startswith('http'):
        profile = find_from_maps(maps_url, session=session)
        if profile:
            log.info(f"Fallback hit (maps) for {vendor.get('business_name') or vendor.get('name')}")
            return profile

    log.debug(f"Fallback: no Instagram found for {vendor.get('business_name') or vendor.get('name')}")
    return None
