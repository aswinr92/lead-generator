"""
normalize.py — Business name normalization for search queries.

Strips legal suffixes, punctuation, and common filler words so that
"Shobha Bridal Studio Pvt. Ltd." becomes "Shobha Bridal Studio"
and search queries are tight and accurate.
"""

import re
import unicodedata


# Legal/corporate suffixes common in India
_LEGAL_SUFFIXES = re.compile(
    r'\b(?:pvt\.?\s*ltd\.?|private\s+limited|llp|llc|inc\.?|'
    r'co\.?|corp\.?|enterprises?|solutions?|services?|&?\s*sons?|'
    r'traders?|brothers?|br[os]+\.?)\b',
    re.IGNORECASE
)

# Punctuation to strip (keep hyphens inside words)
_PUNCT = re.compile(r'[^\w\s\-]')
_MULTI_SPACE = re.compile(r'\s+')


def normalize_name(name: str) -> str:
    """
    Return a cleaned business name suitable for use in search queries.

    Steps:
      1. Unicode NFKC normalisation (handles fancy quotes, ligatures)
      2. Strip legal suffixes (Pvt Ltd, LLP, etc.)
      3. Remove punctuation except word-hyphens
      4. Collapse whitespace
      5. Title-case

    Example:
      "SHOBHA'S BRIDAL & BEAUTY STUDIO (PVT. LTD.)" → "Shobhas Bridal Beauty Studio"
    """
    if not name or not isinstance(name, str):
        return ''

    text = unicodedata.normalize('NFKC', name.strip())
    text = _LEGAL_SUFFIXES.sub(' ', text)
    text = _PUNCT.sub(' ', text)
    text = _MULTI_SPACE.sub(' ', text).strip()
    return text.title()


def search_variants(business_name: str, city: str) -> list[str]:
    """
    Return the three Google search queries to try for this vendor.

    Query order matters — most specific first:
      1. site:instagram.com "{name}" "{city}"
      2. site:instagram.com "{name}" wedding {city}
      3. "{name}" "{city}" instagram
    """
    name = normalize_name(business_name)
    city_clean = city.strip().title() if city else ''

    variants = [
        f'site:instagram.com "{name}" "{city_clean}"',
        f'site:instagram.com "{name}" wedding {city_clean}',
        f'"{name}" "{city_clean}" instagram',
    ]
    # If city is empty, drop city from queries
    if not city_clean:
        variants = [
            f'site:instagram.com "{name}"',
            f'"{name}" instagram',
        ]
    return variants


def extract_keywords(category: str) -> list[str]:
    """
    Return searchable keywords for a vendor category.
    Used in scoring to check bio relevance.
    """
    category_map = {
        'wedding planner':    ['wedding', 'planner', 'events', 'planning'],
        'photographer':       ['photo', 'photography', 'photographer', 'wedding'],
        'videographer':       ['video', 'films', 'cinema', 'wedding'],
        'caterer':            ['catering', 'caterer', 'food', 'cuisine'],
        'decorator':          ['decor', 'decoration', 'floral', 'wedding'],
        'makeup artist':      ['makeup', 'bridal', 'beauty', 'mua'],
        'mehendi artist':     ['mehendi', 'henna', 'bridal'],
        'dj':                 ['dj', 'music', 'events'],
        'live band':          ['band', 'music', 'live'],
        'bridal wear':        ['bridal', 'lehenga', 'saree', 'wedding', 'wear'],
        'jewelry':            ['jewelry', 'jewellery', 'bridal', 'gold'],
        'florist':            ['flowers', 'floral', 'florist', 'wedding'],
        'wedding venue':      ['venue', 'banquet', 'hall', 'resort', 'wedding'],
        'event management':   ['events', 'management', 'wedding', 'planning'],
        'choreographer':      ['dance', 'choreography', 'sangeet'],
        'lighting':           ['lighting', 'lights', 'events', 'wedding'],
    }
    key = (category or '').lower().strip()
    for pattern, kws in category_map.items():
        if pattern in key or key in pattern:
            return kws
    return [key] if key else ['wedding']
