"""
ai_verify.py — Verify Instagram profile match using Claude AI.

Sends vendor details + profile metadata to Claude claude-haiku-4-5 and asks
for a structured YES / LIKELY / NO decision with a confidence score.

Acceptance thresholds (per spec):
  YES    confidence >= 70  →  status=found,       verified=true
  LIKELY confidence >= 85  →  status=found,       verified=true
  YES    confidence <  70  →  status=needs_review, verified=unknown
  LIKELY confidence <  85  →  status=needs_review, verified=unknown
  NO                        →  rejected, try next candidate

Returns a dict:
  {
    'match':      'YES' | 'LIKELY' | 'NO',
    'confidence': int 0–100,
    'reason':     str,
  }
"""

import json
import logging
import re
from typing import Optional

log = logging.getLogger(__name__)

# Acceptance rules: (match_value, min_confidence) → accepted
_ACCEPT_RULES = [
    ('YES',    70),
    ('LIKELY', 85),
]


def _build_prompt(vendor: dict, profile: dict) -> str:
    name     = vendor.get('business_name') or vendor.get('name', '')
    city     = vendor.get('city', '')
    category = vendor.get('category', '')
    website  = vendor.get('website', '')
    address  = vendor.get('address', '')

    username    = profile.get('username', '')
    disp_name   = profile.get('display_name', '')
    bio         = profile.get('bio', '')
    followers   = profile.get('followers', 'unknown')
    verified    = profile.get('verified', False)
    match_score = profile.get('match_score', 'N/A')

    return f"""You are verifying whether an Instagram profile belongs to a specific business.

BUSINESS DETAILS:
- Name: {name}
- City: {city}
- Category: {category}
- Address: {address}
- Website: {website or 'not listed'}

INSTAGRAM PROFILE:
- Username: @{username}
- Display name: {disp_name}
- Bio / description: {bio or 'not available'}
- Followers: {followers}
- Instagram verified badge: {verified}
- Name similarity score: {match_score}/100

TASK:
Decide if this Instagram profile belongs to the business above.

IMPORTANT SIGNALS:
- City/location in bio strongly suggests a match
- Category keywords in bio (e.g. bridal, photography, catering) add confidence
- Display name closely matching business name is strong evidence
- Generic names or very different categories are negative signals

Respond with ONLY valid JSON, nothing else:
{{"match": "YES|LIKELY|NO", "confidence": 0-100, "reason": "one sentence"}}

Rules:
- YES   = you are confident this is the same business
- LIKELY = probably the same but not certain
- NO    = different business or unrelated profile"""


def is_accepted(result: dict) -> bool:
    """Return True if the AI result meets the acceptance thresholds."""
    match = result.get('match', 'NO').upper()
    confidence = int(result.get('confidence', 0))
    for accepted_match, min_conf in _ACCEPT_RULES:
        if match == accepted_match and confidence >= min_conf:
            return True
    return False


def result_to_status(result: dict) -> str:
    """Convert AI result to instagram_status value."""
    if is_accepted(result):
        return 'found'
    match = result.get('match', 'NO').upper()
    if match in ('YES', 'LIKELY'):
        return 'needs_review'
    return 'not_found'


def result_to_verified(result: dict) -> str:
    """Convert AI result to instagram_verified value."""
    if is_accepted(result):
        return 'true'
    match = result.get('match', 'NO').upper()
    if match == 'NO':
        return 'false'
    return 'unknown'


def verify_with_ai(vendor: dict, profile: dict,
                   api_key: str,
                   model: str = 'claude-haiku-4-5-20251001') -> Optional[dict]:
    """
    Ask Claude whether this Instagram profile belongs to the vendor.

    Returns:
        {'match': 'YES'|'LIKELY'|'NO', 'confidence': int, 'reason': str}
        or None if the API call fails.

    Args:
        vendor:   Vendor dict (business_name, city, category, website, address)
        profile:  Profile dict from extract_profile (username, display_name, bio, etc.)
        api_key:  Anthropic API key
        model:    Claude model ID (haiku for cost, sonnet for accuracy)
    """
    try:
        import anthropic
    except ImportError:
        log.error('anthropic package not installed. Run: pip install anthropic')
        return None

    if not api_key:
        log.warning('No Anthropic API key provided — skipping AI verification')
        return None

    prompt = _build_prompt(vendor, profile)
    log.debug(
        f"AI verify: @{profile.get('username')} for "
        f"{vendor.get('business_name') or vendor.get('name')}"
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=model,
            max_tokens=150,
            temperature=0,  # Deterministic for consistency
            messages=[{'role': 'user', 'content': prompt}],
        )
        raw = response.content[0].text.strip()
        log.debug(f'AI response: {raw}')

        # Strip markdown code fences if present
        raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
        raw = re.sub(r'\s*```$', '', raw, flags=re.MULTILINE)

        data = json.loads(raw)

        # Validate and normalise
        match = str(data.get('match', 'NO')).upper()
        if match not in ('YES', 'LIKELY', 'NO'):
            match = 'NO'

        confidence = int(data.get('confidence', 0))
        confidence = max(0, min(100, confidence))

        reason = str(data.get('reason', ''))[:200]

        return {'match': match, 'confidence': confidence, 'reason': reason}

    except json.JSONDecodeError as e:
        log.warning(f'AI returned invalid JSON: {e}  raw={raw!r}')
        return None
    except Exception as e:
        log.warning(f'AI verification error: {e}')
        return None
