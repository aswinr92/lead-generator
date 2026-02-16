"""
pipeline.py — Orchestrates all 7 steps of Instagram discovery for one vendor.

Step flow:
  1  normalize_name()          → cleaned name + 3 search queries
  2  searcher.find_instagram_candidates() → candidate URLs from Google
  3  fetch_profiles_batch()    → og:title, bio, followers for each URL
  4  rank_candidates()         → top 2 by rapidfuzz + city/category score
  5  verify_with_ai()          → YES/LIKELY/NO + confidence 0-100
  6  run_fallback()            → website + maps scan if search empty
  7  writer.write_result()     → immediate write to Google Sheets

process_vendor() handles one vendor end-to-end and returns a result dict.
run_pipeline()   is the outer loop: loads sheet, iterates, logs progress.
"""

import logging
import requests
from typing import Optional

from social_identity.normalize     import normalize_name, search_variants
from social_identity.google_search import GoogleSearcher
from social_identity.extract_profile import fetch_profiles_batch
from social_identity.score          import rank_candidates, meets_minimum_threshold
from social_identity.ai_verify      import (
    verify_with_ai, is_accepted, result_to_status, result_to_verified
)
from social_identity.fallback       import run_fallback
from social_identity.sheets_update  import SheetsWriter

log = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Single-vendor processor
# ------------------------------------------------------------------ #

def process_vendor(vendor: dict,
                   searcher: GoogleSearcher,
                   session: requests.Session,
                   anthropic_key: Optional[str],
                   dry_run: bool = False) -> dict:
    """
    Run all 7 pipeline steps for a single vendor.

    Returns a result dict suitable for SheetsWriter.build_result().
    """
    name = vendor.get('business_name') or vendor.get('name', '')
    city = vendor.get('city', '')
    log.info(f'Processing: {name!r} ({city})')

    # ── Step 1: Normalize ──────────────────────────────────────────
    queries = search_variants(name, city)
    log.debug(f'Search queries: {queries}')

    # ── Steps 2–4: Search → Extract → Score ───────────────────────
    candidate_urls = searcher.find_instagram_candidates(queries)
    profiles = fetch_profiles_batch(candidate_urls, session=session)
    ranked = rank_candidates(vendor, profiles)

    best_profile = None
    ai_result = None

    # ── Step 5: AI verification (top candidate first, second if first fails) ─
    if anthropic_key:
        for candidate in ranked:
            if not meets_minimum_threshold(candidate):
                log.debug(f'  Skipping low-score candidate: score={candidate.get("match_score")}')
                continue

            ai_result = verify_with_ai(vendor, candidate, api_key=anthropic_key)
            if ai_result is None:
                continue  # API error — try next

            log.info(
                f'  AI: match={ai_result["match"]} '
                f'conf={ai_result["confidence"]} '
                f'@{candidate.get("username")} '
                f'reason={ai_result["reason"]!r}'
            )

            if ai_result['match'] in ('YES', 'LIKELY'):
                best_profile = candidate
                break  # Accept this candidate
            # NO → try next candidate

    elif ranked and meets_minimum_threshold(ranked[0]):
        # No API key: accept the top-scored candidate if score >= 60
        best_profile = ranked[0]
        # Synthesise a pseudo-AI result based on score
        score = ranked[0].get('match_score', 0)
        ai_result = {
            'match':      'YES' if score >= 70 else 'LIKELY',
            'confidence': int(score),
            'reason':     f'Score-only (no AI key): {score}/100',
        }
        log.info(f'  No AI key — accepting by score: {score}')

    # ── Step 6: Fallback ──────────────────────────────────────────
    if best_profile is None:
        log.debug(f'  Search yielded no match — running fallback')
        fallback_profile = run_fallback(vendor, session=session)
        if fallback_profile:
            # Score the fallback profile
            ranked_fb = rank_candidates(vendor, [fallback_profile])
            if ranked_fb and meets_minimum_threshold(ranked_fb[0], min_score=30.0):
                best_profile = ranked_fb[0]
                source = fallback_profile.get('source', 'fallback')
                confidence = int(best_profile.get('match_score', 50))
                ai_result = {
                    'match':      'LIKELY',
                    'confidence': confidence,
                    'reason':     f'Found via {source} (no search results)',
                }
                log.info(
                    f'  Fallback hit ({source}): '
                    f'@{best_profile.get("username")} '
                    f'score={confidence}'
                )

    # ── Step 7: Build result ───────────────────────────────────────
    if best_profile and ai_result:
        status   = result_to_status(ai_result)
        verified = result_to_verified(ai_result)
        result   = SheetsWriter.build_result(
            instagram_url=best_profile['url'],
            confidence=ai_result['confidence'],
            status=status,
            followers=best_profile.get('followers'),
            verified=verified,
        )
        result['_profile']  = best_profile   # internal, not written to sheet
        result['_ai']       = ai_result
    else:
        result = SheetsWriter.build_result(status='not_found')

    log.info(
        f'  Result: status={result["instagram_status"]} '
        f'conf={result["instagram_confidence"]} '
        f'url={result.get("instagram_url") or "—"}'
    )
    return result


# ------------------------------------------------------------------ #
# Main pipeline loop
# ------------------------------------------------------------------ #

def run_pipeline(sheet_id: str,
                 google_api_key: Optional[str] = None,
                 google_cse_id: Optional[str] = None,
                 anthropic_key: Optional[str] = None,
                 limit: Optional[int] = None,
                 dry_run: bool = False,
                 min_delay: float = 3.0,
                 max_delay: float = 7.0):
    """
    Full pipeline: load sheet → process each vendor → write results immediately.

    Args:
        sheet_id:        Google Sheets ID
        google_api_key:  Google CSE API key (optional, faster)
        google_cse_id:   Google CSE ID (required with api_key)
        anthropic_key:   Anthropic API key for AI verification (optional)
        limit:           Process at most N vendors (for testing)
        dry_run:         Discover profiles but do NOT write to sheet
        min_delay:       Min seconds between Google searches
        max_delay:       Max seconds between Google searches
    """
    log.info('=' * 60)
    log.info('INSTAGRAM DISCOVERY PIPELINE')
    log.info('=' * 60)
    log.info(f'  Sheet:         {sheet_id}')
    log.info(f'  Google search: {"CSE API" if google_api_key else "HTML scraping"}')
    log.info(f'  AI verify:     {"Claude" if anthropic_key else "score-only (no key)"}')
    log.info(f'  Dry run:       {dry_run}')
    if limit:
        log.info(f'  Limit:         {limit}')

    # Initialise shared objects
    writer   = SheetsWriter(sheet_id)
    searcher = GoogleSearcher(
        api_key=google_api_key,
        cse_id=google_cse_id,
        min_delay=min_delay,
        max_delay=max_delay,
    )
    session  = requests.Session()

    # Load vendors (skips already-processed rows automatically)
    vendors = writer.load_vendors()
    if limit:
        vendors = vendors[:limit]

    total    = len(vendors)
    found    = 0
    reviewed = 0
    not_found = 0

    log.info(f'Vendors to process: {total}')

    for i, vendor in enumerate(vendors, 1):
        name = vendor.get('business_name') or vendor.get('name', 'Unknown')
        log.info(f'[{i}/{total}] {name}')

        try:
            result = process_vendor(
                vendor,
                searcher=searcher,
                session=session,
                anthropic_key=anthropic_key,
                dry_run=dry_run,
            )
        except KeyboardInterrupt:
            log.warning('Interrupted by user')
            break
        except Exception as e:
            log.error(f'  Error processing {name}: {e}', exc_info=True)
            result = SheetsWriter.build_result(status='not_found')

        # Tally
        status = result.get('instagram_status', 'not_found')
        if status == 'found':
            found += 1
        elif status == 'needs_review':
            reviewed += 1
        else:
            not_found += 1

        # Write immediately (unless dry run)
        if not dry_run:
            try:
                writer.write_result(vendor['row_index'], result)
            except Exception as e:
                log.error(f'  Sheet write failed for row {vendor["row_index"]}: {e}')

        # Progress line (also visible in non-verbose runs)
        url_short = (result.get('instagram_url') or '').replace('https://www.instagram.com/', '@').rstrip('/')
        print(
            f'[{i:4}/{total}] {status:<12} conf={result["instagram_confidence"]:>3}  '
            f'{url_short or "—":<30}  {name[:40]}',
            flush=True,
        )

    # Final summary
    log.info('')
    log.info('─' * 60)
    log.info('PIPELINE COMPLETE')
    log.info(f'  Total processed:  {i}')
    log.info(f'  Found:            {found}')
    log.info(f'  Needs review:     {reviewed}')
    log.info(f'  Not found:        {not_found}')
    if dry_run:
        log.info('  ⚠️  DRY RUN — nothing written to sheet')

    return {'found': found, 'needs_review': reviewed, 'not_found': not_found}
