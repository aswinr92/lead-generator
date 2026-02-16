"""
run.py — CLI entry point for the Instagram discovery pipeline.

Usage:
    # Full run (Google CSE + Claude AI)
    python social_identity/run.py \\
        --sheet-id SHEET_ID \\
        --google-api-key AIzaSy... \\
        --google-cse-id 123:abc... \\
        --anthropic-key sk-ant-...

    # Score-only, no AI (free, lower accuracy)
    python social_identity/run.py --sheet-id SHEET_ID

    # Test on first 20 vendors without writing to sheet
    python social_identity/run.py --sheet-id SHEET_ID --limit 20 --dry-run

    # Verbose logging
    python social_identity/run.py --sheet-id SHEET_ID --verbose

Environment variables (alternative to flags):
    GOOGLE_API_KEY, GOOGLE_CSE_ID, ANTHROPIC_API_KEY
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Allow running from project root: python social_identity/run.py
sys.path.insert(0, str(Path(__file__).parent.parent))

from social_identity.pipeline import run_pipeline


def _setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s  %(levelname)-8s  %(name)s  %(message)s',
        datefmt='%H:%M:%S',
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )
    # Quieten noisy third-party loggers
    for noisy in ('urllib3', 'requests', 'gspread', 'google', 'anthropic'):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def main():
    parser = argparse.ArgumentParser(
        description='Discover Instagram profiles for wedding vendors in Google Sheets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full run with Google CSE + Claude AI (most accurate)
  python social_identity/run.py \\
      --sheet-id YOUR_SHEET_ID \\
      --google-api-key AIzaSy... --google-cse-id 123:abc \\
      --anthropic-key sk-ant-...

  # No-cost run (HTML scraping + score-only, no AI)
  python social_identity/run.py --sheet-id YOUR_SHEET_ID

  # Test on 10 vendors, show browser output, skip sheet writes
  python social_identity/run.py --sheet-id ID --limit 10 --dry-run --verbose

  # Faster searches (reduce delay — risk of Google blocking)
  python social_identity/run.py --sheet-id ID --min-delay 2 --max-delay 4

Output columns written to sheet:
  instagram_url, instagram_confidence, instagram_status,
  instagram_followers, instagram_verified, checked_at

instagram_status values:
  found        — AI confirmed match (YES ≥70 or LIKELY ≥85 confidence)
  needs_review — Low-confidence or LIKELY match — verify manually before outreach
  not_found    — No credible Instagram profile found

Safe to re-run — rows with instagram_status already filled are skipped.
        """
    )

    parser.add_argument('--sheet-id', required=True,
                        help='Google Sheets ID from the URL')

    parser.add_argument('--google-api-key', default=None, metavar='KEY',
                        help='Google Custom Search API key (optional). '
                             'Falls back to google.com HTML scraping if omitted. '
                             'Env: GOOGLE_API_KEY')
    parser.add_argument('--google-cse-id', default=None, metavar='CSE_ID',
                        help='Google Custom Search Engine ID. Required with --google-api-key. '
                             'Create at https://cse.google.com  Env: GOOGLE_CSE_ID')

    parser.add_argument('--anthropic-key', default=None, metavar='KEY',
                        help='Anthropic API key for Claude AI verification (optional). '
                             'Without this, candidates are accepted by score only. '
                             'Env: ANTHROPIC_API_KEY')

    parser.add_argument('--limit', type=int, default=None, metavar='N',
                        help='Process at most N vendors (useful for testing)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Run pipeline but do NOT write results to the sheet')

    parser.add_argument('--min-delay', type=float, default=3.0, metavar='SEC',
                        help='Min seconds between Google searches (default: 3.0)')
    parser.add_argument('--max-delay', type=float, default=7.0, metavar='SEC',
                        help='Max seconds between Google searches (default: 7.0)')

    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable DEBUG logging')

    args = parser.parse_args()
    _setup_logging(args.verbose)

    # Resolve keys: CLI flag > env var
    google_api_key = args.google_api_key or os.environ.get('GOOGLE_API_KEY')
    google_cse_id  = args.google_cse_id  or os.environ.get('GOOGLE_CSE_ID')
    anthropic_key  = args.anthropic_key  or os.environ.get('ANTHROPIC_API_KEY')

    # Validate: CSE ID required when API key is given
    if google_api_key and not google_cse_id:
        parser.error('--google-cse-id is required when --google-api-key is provided')

    try:
        stats = run_pipeline(
            sheet_id=args.sheet_id,
            google_api_key=google_api_key,
            google_cse_id=google_cse_id,
            anthropic_key=anthropic_key,
            limit=args.limit,
            dry_run=args.dry_run,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
        )
        found    = stats.get('found', 0)
        reviewed = stats.get('needs_review', 0)
        total    = sum(stats.values())
        print(f'\n✅  Done — {found}/{total} found, {reviewed} need review')
        sys.exit(0)

    except KeyboardInterrupt:
        print('\n\n⚠️  Interrupted. Re-run to resume (already-processed rows are skipped).')
        sys.exit(1)
    except Exception as e:
        logging.getLogger(__name__).error(f'Fatal error: {e}', exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
