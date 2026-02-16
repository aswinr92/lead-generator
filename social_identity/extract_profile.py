"""
extract_profile.py — Fetch an Instagram profile and extract metadata.

Uses Instagram's og: meta tags which are served in the initial HTML
response — no JavaScript rendering required.

Extracted fields:
  username       — from the URL
  title          — og:title  e.g. "Shobha Bridal (@shobhabridal) • Instagram"
  display_name   — parsed from title (before the @handle part)
  bio            — og:description — contains follower count + bio text
  followers      — integer parsed from bio, or None
  verified       — True if a verified badge class is detectable in HTML
  url            — canonical instagram.com URL
"""

import re
import time
import random
import logging
import requests
from typing import Optional

log = logging.getLogger(__name__)

_MOBILE_UA = (
    'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) '
    'AppleWebKit/605.1.15 (KHTML, like Gecko) '
    'Version/16.6 Mobile/15E148 Safari/604.1'
)

# Patterns for og: meta tags
_OG_TITLE_RE = re.compile(r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']',
                           re.IGNORECASE)
_OG_DESC_RE  = re.compile(r'<meta\s+property=["\']og:description["\']\s+content=["\']([^"\']+)["\']',
                           re.IGNORECASE)

# "1,234 Followers" or "12.3K Followers" in the og:description
_FOLLOWERS_RE = re.compile(
    r'([\d,]+(?:\.\d+)?[KkMm]?)\s+Followers',
    re.IGNORECASE
)

# Instagram verified badge: look for isVerified or verified_badge in JSON blobs
_VERIFIED_RE = re.compile(r'"is_verified"\s*:\s*true', re.IGNORECASE)


def _parse_follower_count(text: str) -> Optional[int]:
    """
    Parse "1,234 Followers", "12.3K Followers", "1.2M Followers" → int.
    Returns None if no match.
    """
    m = _FOLLOWERS_RE.search(text)
    if not m:
        return None
    raw = m.group(1).replace(',', '').strip()
    try:
        if raw[-1].upper() == 'K':
            return int(float(raw[:-1]) * 1_000)
        if raw[-1].upper() == 'M':
            return int(float(raw[:-1]) * 1_000_000)
        return int(float(raw))
    except (ValueError, IndexError):
        return None


def _parse_display_name(og_title: str) -> str:
    """
    Extract display name from og:title.
    "Shobha Bridal (@shobhabridal) • Instagram photos and videos"
    → "Shobha Bridal"
    """
    m = re.match(r'^(.+?)\s*\(@', og_title)
    if m:
        return m.group(1).strip()
    # Fallback: strip " • Instagram..."
    return re.sub(r'\s*[•·]\s*Instagram.*$', '', og_title, flags=re.IGNORECASE).strip()


def username_from_url(url: str) -> Optional[str]:
    """
    Extract the Instagram username from a profile URL.
    "https://www.instagram.com/shobhabridal/" → "shobhabridal"
    """
    m = re.search(r'instagram\.com/([A-Za-z0-9._]{3,60})/?', url)
    return m.group(1).lower() if m else None


def fetch_instagram_profile(url: str,
                              session: Optional[requests.Session] = None,
                              timeout: int = 8) -> Optional[dict]:
    """
    Fetch an Instagram profile page and return extracted metadata.

    Returns a dict:
        {
          'url':          canonical profile URL,
          'username':     str,
          'display_name': str,
          'title':        raw og:title,
          'bio':          raw og:description,
          'followers':    int or None,
          'verified':     bool,
        }
    Returns None if the profile is unavailable (404, private, error).
    """
    username = username_from_url(url)
    if not username:
        log.debug(f'Cannot extract username from: {url}')
        return None

    canonical_url = f'https://www.instagram.com/{username}/'

    if session is None:
        session = requests.Session()

    try:
        # Small jitter to avoid hammering Instagram
        time.sleep(random.uniform(0.5, 1.5))

        resp = session.get(
            canonical_url,
            headers={
                'User-Agent': _MOBILE_UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
            },
            timeout=timeout,
            allow_redirects=True,
        )

        if resp.status_code == 404:
            log.debug(f'Profile not found: {canonical_url}')
            return None
        if resp.status_code != 200:
            log.debug(f'HTTP {resp.status_code} for {canonical_url}')
            return None

        html = resp.text

        title_m = _OG_TITLE_RE.search(html)
        desc_m  = _OG_DESC_RE.search(html)

        og_title = title_m.group(1) if title_m else ''
        og_desc  = desc_m.group(1)  if desc_m  else ''

        # Instagram sometimes serves a login wall — detect it
        if 'Log in to Instagram' in og_title or not og_title:
            log.debug(f'Login wall hit for {canonical_url}')
            # Still return partial data — username is still valid
            return {
                'url':          canonical_url,
                'username':     username,
                'display_name': '',
                'title':        og_title,
                'bio':          og_desc,
                'followers':    None,
                'verified':     False,
            }

        return {
            'url':          canonical_url,
            'username':     username,
            'display_name': _parse_display_name(og_title),
            'title':        og_title,
            'bio':          og_desc,
            'followers':    _parse_follower_count(og_desc),
            'verified':     bool(_VERIFIED_RE.search(html)),
        }

    except requests.Timeout:
        log.debug(f'Timeout fetching {canonical_url}')
        return None
    except Exception as e:
        log.debug(f'Error fetching {canonical_url}: {e}')
        return None


def fetch_profiles_batch(urls: list[str],
                          session: Optional[requests.Session] = None) -> list[dict]:
    """
    Fetch multiple Instagram profiles.
    Skips None results (unavailable profiles).
    """
    if session is None:
        session = requests.Session()
    results = []
    for url in urls:
        profile = fetch_instagram_profile(url, session=session)
        if profile:
            results.append(profile)
    return results
