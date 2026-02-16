"""
social_identity — Instagram discovery pipeline for wedding vendors.

Pipeline steps:
  1. normalize   — clean business name for search
  2. google_search — find candidate Instagram profile URLs via Google
  3. extract_profile — fetch og:title, bio, followers from each candidate
  4. score        — rank candidates by name/city/category similarity
  5. ai_verify    — confirm match with Claude AI
  6. fallback     — check vendor website and Maps if search yields nothing
  7. sheets_update — write result columns back to Google Sheets immediately

Entry point: python social_identity/run.py --sheet-id SHEET_ID
"""
