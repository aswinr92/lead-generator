"""
Wedding Vendor Scraper - Integrated Workflow
Complete pipeline: Scrape → Clean → Deduplicate → Export to Google Sheets

Single command to run everything!
"""

import argparse
import sys
from pathlib import Path


def print_banner():
    """Print application banner."""
    print("=" * 80)
    print("🎉 WEDDING VENDOR SCRAPER - itsmy.wedding")
    print("=" * 80)
    print("\n🚀 Integrated Workflow: Scrape → Clean → Deduplicate → Export")
    print()


def load_sheet_id_from_config() -> str:
    """Load saved sheet ID from config if available."""
    config_file = Path("config/sheet_id.txt")

    if config_file.exists():
        with open(config_file, 'r') as f:
            sheet_id = f.read().strip()
            if sheet_id:
                return sheet_id

    return None


def save_sheet_id_to_config(sheet_id: str):
    """Save sheet ID to config for future runs."""
    config_file = Path("config/sheet_id.txt")
    config_file.parent.mkdir(parents=True, exist_ok=True)

    with open(config_file, 'w') as f:
        f.write(sheet_id)

    print(f"✅ Saved sheet ID to {config_file} for future runs")


def main():
    """Main execution function."""
    # Parse arguments
    parser = argparse.ArgumentParser(
        description='Wedding Vendor Scraper - Integrated Workflow',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full workflow (scrape + clean + export)
  python main.py --sheet-id YOUR_SHEET_ID

  # Skip scraping (clean + export existing data)
  python main.py --skip-scraping --sheet-id YOUR_SHEET_ID

  # Scrape and clean only (no export)
  python main.py --skip-export

  # Full workflow with auto-cleanup
  python main.py --sheet-id YOUR_SHEET_ID --auto-cleanup

  # Just scraping (legacy mode)
  python main_scraper_only.py
        """
    )

    parser.add_argument(
        '--skip-scraping',
        action='store_true',
        help='Skip scraping step (clean and export existing CSV files)'
    )
    parser.add_argument(
        '--skip-export',
        action='store_true',
        help='Skip Google Sheets export (scrape and clean only)'
    )
    parser.add_argument(
        '--auto-cleanup',
        action='store_true',
        help='Automatically delete raw CSV files after successful merge'
    )
    parser.add_argument(
        '--sheet-id',
        help='Google Sheet ID for deduplication and export (saved for future runs)'
    )
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to configuration file (default: config/config.yaml)'
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Interactive mode - prompt for sheet ID'
    )

    args = parser.parse_args()

    # Print banner
    print_banner()

    # Handle sheet ID
    sheet_id = args.sheet_id

    # Try to load from config if not provided
    if not sheet_id and not args.skip_export:
        saved_sheet_id = load_sheet_id_from_config()
        if saved_sheet_id:
            print(f"📋 Using saved Sheet ID: {saved_sheet_id}")
            sheet_id = saved_sheet_id
        elif args.interactive:
            print("\n📝 Google Sheet ID not found")
            sheet_id = input("Enter your Google Sheet ID (or press Enter to skip export): ").strip()
            if sheet_id:
                save_sheet_id_to_config(sheet_id)

    # Show workflow configuration
    print("\n⚙️  Workflow Configuration:")
    print(f"   Scraping:       {'⏭️  SKIP' if args.skip_scraping else '✓ RUN'}")
    print(f"   Cleaning:       ✓ RUN")
    print(f"   Deduplication:  ✓ RUN")
    print(f"   Export:         {'⏭️  SKIP' if args.skip_export else '✓ RUN'}")
    print(f"   Auto-cleanup:   {'✓ YES' if args.auto_cleanup else '✗ NO'}")
    print(f"   Sheet ID:       {sheet_id if sheet_id else 'None (will create new sheet)'}")

    # Confirm if interactive
    if args.interactive:
        print("\n❓ Proceed with this configuration?")
        confirm = input("   Enter 'yes' to continue (or anything else to abort): ").strip().lower()
        if confirm != 'yes':
            print("\n⚠️  Workflow aborted by user")
            return

    # Run integrated workflow
    print("\n")
    try:
        from integrated_workflow import run_integrated_workflow

        stats = run_integrated_workflow(
            skip_scraping=args.skip_scraping,
            skip_export=args.skip_export,
            auto_cleanup=args.auto_cleanup,
            sheet_id=sheet_id,
            config_path=args.config
        )

        # Save sheet ID if new one was created
        if not args.skip_export and sheet_id and stats.get('export', {}).get('sheet_url'):
            save_sheet_id_to_config(sheet_id)

        print("\n🎉 SUCCESS! Workflow completed successfully!")

        # Print next steps
        print("\n💡 Next Steps:")
        if not args.skip_export and stats.get('export', {}).get('sheet_url'):
            print(f"   1. Open your Google Sheet: {stats['export']['sheet_url']}")
            print("   2. Share with your team")
            print("   3. Start onboarding vendors!")
        else:
            print("   1. Check cleaned data in output/ directory")
            print("   2. Run with --sheet-id to export to Google Sheets")

    except KeyboardInterrupt:
        print("\n\n⚠️  Workflow interrupted by user")
        print("   Partial results may have been saved")
        sys.exit(1)

    except Exception as e:
        print(f"\n❌ Workflow error: {e}")
        print("\n🔧 Troubleshooting:")
        print("   - Check that all dependencies are installed: pip install -r requirements.txt")
        print("   - Verify Google credentials: config/google_credentials.json")
        print("   - Check config file: config/config.yaml")
        print("   - Review error details above")

        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
