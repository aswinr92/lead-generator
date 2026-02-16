"""
Google Sheets Deduplicator
Downloads existing data from Google Sheets and deduplicates against new data.
"""

import pandas as pd
from typing import Optional, Tuple
import gspread
from google.oauth2.service_account import Credentials


class SheetsDeduplicator:
    """Handles deduplication between new data and existing Google Sheets data."""

    def __init__(self, credentials_path: str = "config/google_credentials.json"):
        """
        Initialize sheets deduplicator.

        Args:
            credentials_path: Path to Google service account credentials
        """
        self.credentials_path = credentials_path
        self.client = None

    def connect(self):
        """Connect to Google Sheets API."""
        if self.client:
            return

        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=scopes
            )
            self.client = gspread.authorize(creds)
            print("✅ Connected to Google Sheets")
        except Exception as e:
            print(f"⚠️  Could not connect to Google Sheets: {e}")
            print("   Will proceed without existing data comparison")
            self.client = None

    def download_existing_data(
        self,
        sheet_id: str,
        worksheet_name: str = "Vendor Data"
    ) -> Optional[pd.DataFrame]:
        """
        Download existing vendor data from Google Sheet.

        Args:
            sheet_id: Google Sheet ID
            worksheet_name: Name of worksheet to download

        Returns:
            DataFrame with existing data or None
        """
        if not self.client:
            self.connect()

        if not self.client:
            return None

        try:
            print(f"\n📥 Downloading existing data from Google Sheets...")

            # Open sheet
            spreadsheet = self.client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)

            # Get all records
            records = worksheet.get_all_records()

            if not records:
                print("   No existing data found")
                return None

            df = pd.DataFrame(records)
            print(f"   ✓ Downloaded {len(df)} existing records")

            return df

        except gspread.exceptions.WorksheetNotFound:
            print(f"   ⚠️  Worksheet '{worksheet_name}' not found")
            return None
        except Exception as e:
            print(f"   ⚠️  Error downloading data: {e}")
            return None

    def merge_with_existing(
        self,
        new_data: pd.DataFrame,
        existing_data: pd.DataFrame
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Merge new data with existing data.

        Args:
            new_data: Newly scraped and cleaned data
            existing_data: Existing data from Google Sheets

        Returns:
            Tuple of (merged DataFrame, merge stats)
        """
        print(f"\n🔄 Merging new data with existing data...")

        # Stats
        stats = {
            'existing_count': len(existing_data),
            'new_count': len(new_data),
            'total_before_dedup': len(existing_data) + len(new_data),
            'duplicates_found': 0,
            'new_vendors': 0,
            'updated_vendors': 0,
            'final_count': 0
        }

        # Add source markers
        existing_data = existing_data.copy()
        new_data = new_data.copy()

        existing_data['_source'] = 'existing'
        new_data['_source'] = 'new'

        # Combine
        combined = pd.concat([existing_data, new_data], ignore_index=True)

        print(f"   Existing: {stats['existing_count']} records")
        print(f"   New: {stats['new_count']} records")
        print(f"   Combined: {len(combined)} records")

        return combined, stats

    def get_merge_report(
        self,
        stats: dict,
        final_count: int
    ) -> str:
        """
        Generate merge report.

        Args:
            stats: Merge statistics
            final_count: Final record count after deduplication

        Returns:
            Formatted report string
        """
        stats['final_count'] = final_count
        stats['duplicates_found'] = stats['total_before_dedup'] - final_count
        stats['new_vendors'] = stats['new_count'] - stats['duplicates_found']

        report = "\n" + "=" * 80 + "\n"
        report += "📊 GOOGLE SHEETS MERGE REPORT\n"
        report += "=" * 80 + "\n"
        report += f"Existing vendors in Sheets: {stats['existing_count']}\n"
        report += f"Newly scraped vendors:      {stats['new_count']}\n"
        report += f"Total before dedup:         {stats['total_before_dedup']}\n"
        report += f"Duplicates removed:         {stats['duplicates_found']}\n"
        report += f"New unique vendors:         {stats['new_vendors']}\n"
        report += f"Final vendor count:         {stats['final_count']}\n"
        report += "=" * 80 + "\n"

        return report


def deduplicate_with_sheets(
    new_data_csv: str,
    sheet_id: Optional[str] = None,
    credentials_path: str = "config/google_credentials.json"
) -> Tuple[pd.DataFrame, dict]:
    """
    Deduplicate new data against existing Google Sheets data.

    Args:
        new_data_csv: Path to newly cleaned CSV
        sheet_id: Google Sheet ID (optional)
        credentials_path: Path to credentials

    Returns:
        Tuple of (final deduplicated DataFrame, merge stats)
    """
    from .deduplicator import deduplicate_vendors

    # Load new data
    new_data = pd.read_csv(new_data_csv)
    print(f"\n📂 Loaded {len(new_data)} newly cleaned records")

    # Try to download existing data
    deduplicator = SheetsDeduplicator(credentials_path)

    existing_data = None
    if sheet_id:
        existing_data = deduplicator.download_existing_data(sheet_id)

    # If no existing data, just return new data
    if existing_data is None or len(existing_data) == 0:
        print("\n✅ No existing data - using new data only")

        # Still deduplicate within new data
        final_data, dup_log = deduplicate_vendors(new_data)

        stats = {
            'existing_count': 0,
            'new_count': len(new_data),
            'total_before_dedup': len(new_data),
            'duplicates_found': len(new_data) - len(final_data),
            'new_vendors': len(final_data),
            'updated_vendors': 0,
            'final_count': len(final_data)
        }

        return final_data, stats

    # Merge with existing
    combined, stats = deduplicator.merge_with_existing(new_data, existing_data)

    # Deduplicate combined data
    print("\n🔍 Deduplicating combined data...")
    final_data, dup_log = deduplicate_vendors(combined)

    # Remove source marker
    if '_source' in final_data.columns:
        final_data = final_data.drop(columns=['_source'])

    # Print report
    report = deduplicator.get_merge_report(stats, len(final_data))
    print(report)

    return final_data, stats


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python sheets_deduplicator.py <new_data_csv> [sheet_id]")
        sys.exit(1)

    new_csv = sys.argv[1]
    sheet_id = sys.argv[2] if len(sys.argv) > 2 else None

    final_df, stats = deduplicate_with_sheets(new_csv, sheet_id)

    print(f"\n✅ Final result: {len(final_df)} unique vendors")
