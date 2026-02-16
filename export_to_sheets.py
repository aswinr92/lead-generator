"""
Export Wedding Vendor Data to Google Sheets

This script exports vendor data from CSV to Google Sheets with:
- Professional formatting (bold headers, frozen rows, filters)
- Color-coded ratings (green/yellow/red)
- Onboarding status tracking with dropdown validation
- Automated statistics dashboard
- Shareable collaboration links
"""

import yaml
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from exporters.google_sheets_exporter import GoogleSheetsExporter


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def find_latest_csv(output_dir: str = "output") -> str:
    """
    Find the most recent vendors_*.csv file.

    Args:
        output_dir: Directory containing CSV files

    Returns:
        Path to the latest CSV file

    Raises:
        FileNotFoundError: If no CSV files found
    """
    csv_files = list(Path(output_dir).glob("vendors_*.csv"))

    if not csv_files:
        raise FileNotFoundError(
            f"❌ No vendor CSV files found in {output_dir}/\n"
            f"   Run main.py first to scrape vendor data"
        )

    # Sort by modification time, return most recent
    latest = max(csv_files, key=lambda p: p.stat().st_mtime)
    return str(latest)


def validate_credentials(credentials_path: str = "config/google_credentials.json") -> bool:
    """
    Check if Google Sheets credentials file exists.

    Args:
        credentials_path: Path to credentials JSON

    Returns:
        True if file exists, False otherwise
    """
    if not Path(credentials_path).exists():
        print(f"❌ Error: Credentials file not found")
        print(f"   Expected: {credentials_path}")
        print(f"   See docs/GOOGLE_SHEETS_SETUP.md for setup instructions")
        return False
    return True


def load_csv_data(csv_path: str) -> pd.DataFrame:
    """
    Load vendor data from CSV file.

    Args:
        csv_path: Path to CSV file

    Returns:
        DataFrame with vendor data

    Raises:
        ValueError: If CSV is empty
    """
    df = pd.read_csv(csv_path)

    if df.empty:
        raise ValueError("❌ CSV file is empty. No vendor data to export.")

    return df


def show_data_preview(df: pd.DataFrame, csv_path: str):
    """
    Display preview of data to be exported.

    Args:
        df: Vendor DataFrame
        csv_path: Path to source CSV
    """
    print("\n" + "=" * 70)
    print("📊 DATA PREVIEW")
    print("=" * 70)
    print(f"Source: {csv_path}")
    print(f"File size: {Path(csv_path).stat().st_size / 1024:.1f} KB")
    print(f"Last modified: {datetime.fromtimestamp(Path(csv_path).stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Basic stats
    print(f"📈 Total vendors: {len(df)}")

    # Categories
    if 'category' in df.columns:
        categories = df['category'].value_counts()
        print(f"\n🏷️  Categories ({len(categories)}):")
        for category, count in categories.items():
            print(f"   - {category}: {count}")

    # Cities (extract from search_query)
    if 'search_query' in df.columns:
        cities = df['search_query'].str.extract(r'in (.+)$')[0].value_counts()
        print(f"\n📍 Cities ({len(cities)}):")
        for city, count in cities.items():
            print(f"   - {city}: {count}")

    # Contact info
    if 'phone' in df.columns:
        with_phone = df['phone'].notna().sum()
        without_phone = df['phone'].isna().sum()
        print(f"\n📞 Phone numbers:")
        print(f"   - With phone: {with_phone} ({with_phone/len(df)*100:.1f}%)")
        print(f"   - Without phone: {without_phone} ({without_phone/len(df)*100:.1f}%)")

    if 'website' in df.columns:
        with_website = df['website'].notna().sum()
        without_website = df['website'].isna().sum()
        print(f"\n🌐 Websites:")
        print(f"   - With website: {with_website} ({with_website/len(df)*100:.1f}%)")
        print(f"   - Without website: {without_website} ({without_website/len(df)*100:.1f}%)")

    # Ratings
    if 'rating' in df.columns:
        ratings = pd.to_numeric(df['rating'], errors='coerce')
        avg_rating = ratings.mean()
        if not pd.isna(avg_rating):
            print(f"\n⭐ Average rating: {avg_rating:.2f}")

    print("=" * 70)


def prompt_sheet_id() -> tuple:
    """
    Prompt user to create new sheet or update existing.

    Returns:
        Tuple of (sheet_id, sheet_name) or (None, None) to create new
    """
    print("\n" + "=" * 70)
    print("📤 EXPORT OPTIONS")
    print("=" * 70)
    print("1. Create new Google Sheet")
    print("2. Update existing Google Sheet")
    print()

    while True:
        choice = input("Choose option (1 or 2): ").strip()

        if choice == '1':
            # Create new sheet
            sheet_name = input("\nEnter sheet name (or press Enter for default): ").strip()
            return None, sheet_name if sheet_name else None

        elif choice == '2':
            # Update existing
            print("\nTo update an existing sheet, you need the Sheet ID.")
            print("You can find it in the URL:")
            print("https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")
            print()
            sheet_id = input("Enter Sheet ID: ").strip()

            if sheet_id:
                return sheet_id, None
            else:
                print("❌ Sheet ID cannot be empty")
                continue

        else:
            print("❌ Invalid choice. Please enter 1 or 2")


def export_data(
    df: pd.DataFrame,
    config: dict,
    sheet_id: str = None,
    sheet_name: str = None
):
    """
    Export data to Google Sheets.

    Args:
        df: Vendor DataFrame
        config: Configuration dictionary
        sheet_id: Existing sheet ID or None
        sheet_name: Name for new sheet or None
    """
    print("\n" + "=" * 70)
    print("🔐 AUTHENTICATING")
    print("=" * 70)

    try:
        # Initialize exporter
        credentials_path = config.get('google_sheets', {}).get('credentials_file', 'config/google_credentials.json')
        exporter = GoogleSheetsExporter(credentials_path=credentials_path, config=config.get('google_sheets', {}))
        print(f"✅ Authentication successful")
        print(f"   Service account: {exporter.service_account_email}")

    except FileNotFoundError as e:
        print(str(e))
        sys.exit(1)
    except ValueError as e:
        print(str(e))
        sys.exit(1)

    print("\n" + "=" * 70)
    print("📤 EXPORTING TO GOOGLE SHEETS")
    print("=" * 70)

    if sheet_id:
        print(f"Mode: Updating existing sheet")
        print(f"Sheet ID: {sheet_id}")
    else:
        print(f"Mode: Creating new sheet")
        if sheet_name:
            print(f"Sheet name: {sheet_name}")

    print(f"\nExporting {len(df)} vendors...")

    try:
        # Export to Google Sheets
        result = exporter.export_to_sheet(
            df=df,
            sheet_id=sheet_id,
            sheet_name=sheet_name
        )

        # Success!
        print("\n" + "=" * 70)
        print("✅ EXPORT SUCCESSFUL!")
        print("=" * 70)
        print(f"\n🔗 Spreadsheet URL:")
        print(f"   {result['sheet_url']}")
        print(f"\n📑 Tabs created:")
        print(f"   - {result['data_tab']} (vendor data with {len(df)} rows)")
        print(f"   - {result['summary_tab']} (statistics dashboard)")
        print(f"\n👤 Service account email:")
        print(f"   {result['service_account_email']}")
        print(f"\n💡 To share with others:")
        print(f"   1. Open the spreadsheet URL above")
        print(f"   2. Click 'Share' button (top-right)")
        print(f"   3. Add email addresses")
        print(f"   4. Set permission to 'Editor' or 'Viewer'")
        print("\n" + "=" * 70)

    except Exception as e:
        print(f"\n❌ Export failed: {e}")
        print("\nTroubleshooting:")
        print("   - Verify credentials file exists and is valid")
        print("   - For existing sheets, ensure Sheet ID is correct")
        print("   - Check that service account has permission to access the sheet")
        print("   - See docs/GOOGLE_SHEETS_SETUP.md for help")
        sys.exit(1)


def main():
    """Main execution function."""
    print("\n" + "=" * 70)
    print("🎉 WEDDING VENDOR EXPORTER - GOOGLE SHEETS")
    print("=" * 70)
    print("Export vendor data to formatted Google Sheets with statistics")
    print()

    try:
        # 1. Load configuration
        print("🔍 Loading configuration...")
        config = load_config()
        print("✅ Configuration loaded")

        # 2. Validate credentials
        print("\n🔍 Checking credentials...")
        credentials_path = config.get('google_sheets', {}).get('credentials_file', 'config/google_credentials.json')
        if not validate_credentials(credentials_path):
            sys.exit(1)
        print("✅ Credentials file found")

        # 3. Find latest CSV
        print("\n🔍 Finding latest vendor data...")
        csv_path = find_latest_csv()
        print(f"✅ Found: {csv_path}")

        # 4. Load CSV data
        print("\n🔍 Loading CSV data...")
        df = load_csv_data(csv_path)
        print(f"✅ Loaded {len(df)} vendors")

        # 5. Show data preview
        show_data_preview(df, csv_path)

        # 6. Prompt for export options
        sheet_id, sheet_name = prompt_sheet_id()

        # 7. Export to Google Sheets
        export_data(df, config, sheet_id, sheet_name)

    except FileNotFoundError as e:
        print(f"\n{e}")
        sys.exit(1)
    except ValueError as e:
        print(f"\n{e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Export cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
