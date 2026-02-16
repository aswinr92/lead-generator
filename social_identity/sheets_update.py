"""
sheets_update.py — Google Sheets read/write for the Instagram discovery pipeline.

Design principles:
  - Write each result immediately after processing (not in batch at the end)
    so that a Ctrl+C or crash loses at most one vendor's work.
  - Skip rows where instagram_status is already populated (resumability).
  - Add missing output columns to the sheet automatically on first run.
  - Use column-index writes (single cell range) to avoid overwriting
    neighbouring columns.

Output columns managed:
  instagram_url, instagram_confidence, instagram_status,
  instagram_followers, instagram_verified, checked_at
"""

import logging
from datetime import datetime, timezone
from typing import Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

log = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]
CREDENTIALS_FILE = 'config/google_credentials.json'

# Columns this pipeline writes (must match field names in pipeline output)
OUTPUT_COLUMNS = [
    'instagram_url',
    'instagram_confidence',
    'instagram_status',        # found | not_found | needs_review
    'instagram_followers',
    'instagram_verified',      # true | false | unknown
    'checked_at',
]

# Existing sheet column that maps to our vendor field names
_FIELD_ALIASES = {
    'name':            'business_name',
    'url':             'google_maps_url',
}


class SheetsWriter:
    """
    Manages reading vendors from and writing results to a Google Sheet.
    """

    def __init__(self, sheet_id: str, worksheet_index: int = 0):
        self.sheet_id = sheet_id
        self.worksheet_index = worksheet_index
        self._client: Optional[gspread.Client] = None
        self._worksheet: Optional[gspread.Worksheet] = None
        self._headers: list[str] = []
        self._col_index: dict[str, int] = {}  # col_name → 1-based column number

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #

    def _connect(self):
        if self._worksheet is not None:
            return
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        self._client = gspread.authorize(creds)
        spreadsheet = self._client.open_by_key(self.sheet_id)
        self._worksheet = spreadsheet.get_worksheet(self.worksheet_index)

    def _refresh_headers(self):
        """Re-read header row and rebuild column index."""
        self._headers = self._worksheet.row_values(1)
        self._col_index = {h: i + 1 for i, h in enumerate(self._headers)}

    # ------------------------------------------------------------------ #
    # Column management
    # ------------------------------------------------------------------ #

    def ensure_output_columns(self):
        """Add any missing OUTPUT_COLUMNS to the header row."""
        self._connect()
        self._refresh_headers()

        missing = [c for c in OUTPUT_COLUMNS if c not in self._col_index]
        if not missing:
            log.info(f'All output columns already exist in sheet')
            return

        log.info(f'Adding columns: {missing}')
        new_headers = self._headers + missing
        self._worksheet.update(values=[new_headers], range_name='A1')
        self._refresh_headers()

    # ------------------------------------------------------------------ #
    # Load vendors
    # ------------------------------------------------------------------ #

    def load_vendors(self) -> list[dict]:
        """
        Load all rows from the sheet as a list of dicts.

        Applies field aliases so pipeline code uses canonical field names
        (business_name, google_maps_url) regardless of actual column names.

        Adds 'row_index' (1-based sheet row including header, so data row 1 = row_index 2).
        Skips rows where instagram_status is already filled (resumability).
        """
        self._connect()
        self._refresh_headers()
        self.ensure_output_columns()

        all_values = self._worksheet.get_all_values()
        if len(all_values) <= 1:
            return []

        headers = all_values[0]
        vendors = []

        for sheet_row, row_values in enumerate(all_values[1:], start=2):
            # Pad short rows
            padded = row_values + [''] * (len(headers) - len(row_values))
            record = dict(zip(headers, padded))

            # Skip already-processed rows
            status = record.get('instagram_status', '').strip()
            if status in ('found', 'not_found', 'needs_review'):
                continue

            # Apply field aliases for pipeline compatibility
            for sheet_col, pipeline_field in _FIELD_ALIASES.items():
                if sheet_col in record and pipeline_field not in record:
                    record[pipeline_field] = record[sheet_col]

            record['row_index'] = sheet_row
            vendors.append(record)

        log.info(f'Loaded {len(vendors)} vendors needing Instagram check')
        return vendors

    # ------------------------------------------------------------------ #
    # Write result
    # ------------------------------------------------------------------ #

    def write_result(self, row_index: int, result: dict):
        """
        Write the pipeline result for one vendor back to the sheet immediately.

        Only writes OUTPUT_COLUMNS cells — does not touch any other column.

        Args:
            row_index:  1-based sheet row number (as stored in vendor['row_index'])
            result:     Dict with keys matching OUTPUT_COLUMNS
        """
        self._connect()

        if not self._col_index:
            self._refresh_headers()

        updates = []
        for col_name in OUTPUT_COLUMNS:
            col_num = self._col_index.get(col_name)
            if col_num is None:
                continue
            val = result.get(col_name, '')
            if val is None:
                val = ''
            updates.append({
                'range': gspread.utils.rowcol_to_a1(row_index, col_num),
                'values': [[str(val)]],
            })

        if updates:
            self._worksheet.batch_update(updates)
            log.debug(f'Row {row_index}: written {len(updates)} cells')

    # ------------------------------------------------------------------ #
    # Convenience: build result dict
    # ------------------------------------------------------------------ #

    @staticmethod
    def build_result(instagram_url: str = '',
                     confidence: int = 0,
                     status: str = 'not_found',
                     followers: Optional[int] = None,
                     verified: str = 'unknown') -> dict:
        """
        Build a result dict with all OUTPUT_COLUMNS populated.
        Pass into write_result().
        """
        return {
            'instagram_url':        instagram_url or '',
            'instagram_confidence': confidence,
            'instagram_status':     status,
            'instagram_followers':  followers if followers is not None else '',
            'instagram_verified':   verified,
            'checked_at':           datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        }
