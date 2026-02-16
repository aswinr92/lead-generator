"""
score.py — Score Instagram profile candidates against a vendor record.

Scoring dimensions (total 100 points):
  Name similarity    60 pts  rapidfuzz token_sort_ratio on normalised names
  City match         20 pts  city keyword found in bio or display_name
  Category match     10 pts  category keywords found in bio
  Follower plausibility 10 pts  has followers (any count > 0) = more credible

Candidates are ranked and top 2 returned for AI verification.
"""

import logging
from typing import Optional

try:
    from rapidfuzz import fuzz as _fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False
    import difflib

from social_identity.normalize import normalize_name, extract_keywords

log = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Fuzzy string similarity
# ------------------------------------------------------------------ #

def _name_similarity(a: str, b: str) -> float:
    """
    Return 0–100 similarity between two business name strings.
    Uses rapidfuzz.token_sort_ratio if available, else difflib.
    """
    a = normalize_name(a).lower()
    b = normalize_name(b).lower()
    if not a or not b:
        return 0.0
    if _HAS_RAPIDFUZZ:
        return _fuzz.token_sort_ratio(a, b)
    # difflib fallback (slightly less accurate)
    return difflib.SequenceMatcher(None, a, b).ratio() * 100


# ------------------------------------------------------------------ #
# Scoring
# ------------------------------------------------------------------ #

def score_candidate(vendor: dict, profile: dict) -> float:
    """
    Return a 0–100 score for how well an Instagram profile matches a vendor.

    Vendor dict keys used: business_name, city, category
    Profile dict keys used: display_name, title, bio, followers
    """
    score = 0.0

    # --- Name similarity (60 pts) ---
    vendor_name = vendor.get('business_name', '') or vendor.get('name', '')
    profile_name = profile.get('display_name', '') or profile.get('title', '')
    name_sim = _name_similarity(vendor_name, profile_name)
    score += name_sim * 0.60

    # --- City match (20 pts) ---
    city = (vendor.get('city', '') or '').lower().strip()
    bio_text = (
        (profile.get('bio', '') or '') + ' ' +
        (profile.get('display_name', '') or '') + ' ' +
        (profile.get('title', '') or '')
    ).lower()

    if city and city in bio_text:
        score += 20.0
    elif city:
        # Partial city match (e.g. "Thiruvananthapuram" vs "Trivandrum")
        city_tokens = city.split()
        if any(t in bio_text for t in city_tokens if len(t) >= 4):
            score += 10.0

    # --- Category keywords (10 pts) ---
    category = vendor.get('category', '') or ''
    keywords = extract_keywords(category)
    kw_hits = sum(1 for kw in keywords if kw.lower() in bio_text)
    if kw_hits >= 2:
        score += 10.0
    elif kw_hits == 1:
        score += 5.0

    # --- Follower plausibility (10 pts) ---
    # A profile with followers > 0 is more likely real / active
    followers = profile.get('followers')
    if isinstance(followers, int) and followers > 0:
        score += 10.0

    return round(min(score, 100.0), 1)


def rank_candidates(vendor: dict, profiles: list[dict]) -> list[dict]:
    """
    Score all candidates and return them sorted descending by score.
    Adds 'match_score' key to each profile dict.
    Returns top 2 only (per spec).
    """
    scored = []
    for profile in profiles:
        s = score_candidate(vendor, profile)
        log.debug(
            f"  score={s:.1f}  @{profile.get('username', '?')}  "
            f"name={profile.get('display_name', '?')!r}"
        )
        scored.append({**profile, 'match_score': s})

    scored.sort(key=lambda p: p['match_score'], reverse=True)
    return scored[:2]


def meets_minimum_threshold(profile: dict, min_score: float = 40.0) -> bool:
    """
    Return True if a candidate's score is high enough to bother sending to AI.
    Saves API calls by skipping obviously wrong profiles.
    """
    return profile.get('match_score', 0.0) >= min_score
