"""
Integrated Workflow
Single command to scrape, clean, deduplicate, and export to Google Sheets.
"""

import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
import os
import glob


class IntegratedWorkflow:
    """Manages the complete workflow from scraping to Google Sheets export."""

    def __init__(self, config_path: str = "config/config.yaml"):
        """
        Initialize integrated workflow.

        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        self.workflow_stats = {
            'scraping': {},
            'cleaning': {},
            'merging': {},
            'export': {}
        }

    def _load_config(self) -> dict:
        """Load configuration from YAML file."""
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def run_full_workflow(
        self,
        skip_scraping: bool = False,
        skip_export: bool = False,
        auto_cleanup: bool = False,
        sheet_id: Optional[str] = None
    ) -> dict:
        """
        Run complete workflow: scrape → clean → deduplicate → export.

        Args:
            skip_scraping: Skip scraping step (use existing CSVs)
            skip_export: Skip Google Sheets export
            auto_cleanup: Automatically delete raw CSVs after successful merge
            sheet_id: Google Sheet ID for deduplication and export

        Returns:
            Workflow statistics dictionary
        """
        print("=" * 80)
        print("🚀 INTEGRATED WEDDING VENDOR WORKFLOW")
        print("=" * 80)
        print(f"\nStarted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Step 1: Scraping
        if skip_scraping:
            print("\n" + "=" * 80)
            print("⏭️  STEP 1: SCRAPING (SKIPPED)")
            print("=" * 80)
            print("Using existing CSV files")
        else:
            raw_csv = self._run_scraping()
            if not raw_csv:
                print("\n❌ Scraping failed - workflow aborted")
                return self.workflow_stats

        # Step 2: Cleaning & Deduplication
        cleaned_csv = self._run_cleaning()
        if not cleaned_csv:
            print("\n❌ Cleaning failed - workflow aborted")
            return self.workflow_stats

        # Step 3: Merge with existing Google Sheets data
        if sheet_id:
            final_csv = self._run_sheets_merge(cleaned_csv, sheet_id)
        else:
            final_csv = cleaned_csv
            print("\n⚠️  No sheet_id provided - skipping merge with existing data")

        # Step 4: Export to Google Sheets
        if skip_export:
            print("\n" + "=" * 80)
            print("⏭️  STEP 4: EXPORT TO GOOGLE SHEETS (SKIPPED)")
            print("=" * 80)
        else:
            self._run_export(final_csv, sheet_id)

        # Step 5: Cleanup
        if auto_cleanup:
            self._cleanup_raw_files()

        # Print final summary
        self._print_summary()

        return self.workflow_stats

    def _run_scraping(self) -> Optional[str]:
        """
        Run scraping step.

        Returns:
            Path to raw CSV file or None
        """
        print("\n" + "=" * 80)
        print("📍 STEP 1: SCRAPING VENDORS FROM GOOGLE MAPS")
        print("=" * 80)

        try:
            from scrapers.google_maps_scraper_optimized import OptimizedGoogleMapsVendorScraper

            # Get config
            cities = self.config['cities']
            categories = self.config['categories']
            scraping_config = self.config['scraping']

            # Generate queries
            queries = []
            for city in cities:
                for category in categories:
                    queries.append(f"{category} in {city}")

            print(f"\n🔎 Searching {len(queries)} queries...")
            print(f"   Cities: {', '.join(cities)}")
            print(f"   Categories: {', '.join(categories)}")

            # Initialize scraper
            scraper = OptimizedGoogleMapsVendorScraper(
                headless=scraping_config['headless']
            )

            all_vendors = []

            # Scrape
            for idx, query in enumerate(queries, 1):
                print(f"\n[{idx}/{len(queries)}] {query}")

                vendors = scraper.search_vendors(
                    query=query,
                    max_results=scraping_config['max_results_per_search']
                )

                all_vendors.extend(vendors)
                print(f"   ✓ Collected {len(vendors)} vendors")

                # Rate limiting
                if idx < len(queries):
                    import time
                    import random
                    delay = scraping_config['rate_limit_delay']
                    time.sleep(delay + random.uniform(0, 2))

            # Save
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"output/vendors_{timestamp}.csv"

            df = pd.DataFrame(all_vendors)
            df.to_csv(output_path, index=False, encoding='utf-8')

            # Stats
            self.workflow_stats['scraping'] = {
                'vendors_scraped': len(all_vendors),
                'queries_run': len(queries),
                'output_file': output_path
            }

            print(f"\n✅ Scraped {len(all_vendors)} vendors → {output_path}")

            scraper.close()

            return output_path

        except Exception as e:
            print(f"\n❌ Scraping error: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _run_cleaning(self) -> Optional[str]:
        """
        Run cleaning and deduplication step.

        Returns:
            Path to cleaned CSV file or None
        """
        print("\n" + "=" * 80)
        print("🧹 STEP 2: CLEANING & DEDUPLICATION")
        print("=" * 80)

        try:
            from processors.csv_merger import merge_all_vendor_csvs

            # Find all raw vendor CSVs (exclude cleaned files)
            csv_pattern = "output/vendors_*.csv"
            all_csvs = glob.glob(csv_pattern)

            # Exclude cleaned files
            raw_csvs = [
                f for f in all_csvs
                if 'cleaned' not in f and 'merged' not in f
            ]

            if not raw_csvs:
                print("⚠️  No raw CSV files found")
                return None

            print(f"\n📂 Found {len(raw_csvs)} raw CSV files:")
            for f in raw_csvs:
                print(f"   - {Path(f).name}")

            # Merge and clean
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            cleaned_path = f"output/vendors_cleaned_{timestamp}.csv"

            df_cleaned = merge_all_vendor_csvs(
                input_dir="output",
                output_file=cleaned_path,
                pattern="vendors_*.csv",
                clean_and_dedupe=True
            )

            # Stats
            self.workflow_stats['cleaning'] = {
                'raw_files_merged': len(raw_csvs),
                'records_cleaned': len(df_cleaned),
                'output_file': cleaned_path
            }

            print(f"\n✅ Cleaned data saved → {cleaned_path}")

            return cleaned_path

        except Exception as e:
            print(f"\n❌ Cleaning error: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _run_sheets_merge(
        self,
        cleaned_csv: str,
        sheet_id: str
    ) -> Optional[str]:
        """
        Merge cleaned data with existing Google Sheets data.

        Args:
            cleaned_csv: Path to cleaned CSV
            sheet_id: Google Sheet ID

        Returns:
            Path to final merged CSV or None
        """
        print("\n" + "=" * 80)
        print("🔄 STEP 3: MERGE WITH EXISTING GOOGLE SHEETS DATA")
        print("=" * 80)

        try:
            from processors.sheets_deduplicator import deduplicate_with_sheets

            # Deduplicate against existing sheets data
            final_df, merge_stats = deduplicate_with_sheets(
                cleaned_csv,
                sheet_id=sheet_id
            )

            # Save final result
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_path = f"output/vendors_final_{timestamp}.csv"

            final_df.to_csv(final_path, index=False, encoding='utf-8')

            # Stats
            self.workflow_stats['merging'] = merge_stats
            self.workflow_stats['merging']['output_file'] = final_path

            print(f"\n✅ Final merged data saved → {final_path}")

            return final_path

        except Exception as e:
            print(f"\n❌ Merge error: {e}")
            import traceback
            traceback.print_exc()
            # Fall back to cleaned CSV
            return cleaned_csv

    def _run_export(
        self,
        csv_file: str,
        sheet_id: Optional[str] = None
    ):
        """
        Export to Google Sheets.

        Args:
            csv_file: Path to CSV file to export
            sheet_id: Google Sheet ID (optional)
        """
        print("\n" + "=" * 80)
        print("📤 STEP 4: EXPORT TO GOOGLE SHEETS")
        print("=" * 80)

        try:
            from exporters.google_sheets_exporter import GoogleSheetsExporter

            # Initialize exporter
            exporter = GoogleSheetsExporter()

            # Load data
            df = pd.read_csv(csv_file)

            print(f"\n📊 Exporting {len(df)} vendors to Google Sheets...")

            # Export
            if sheet_id:
                # Update existing sheet
                sheet_url = exporter.update_sheet(df, sheet_id)
                print(f"\n✅ Updated existing sheet: {sheet_url}")
            else:
                # Create new sheet
                sheet_name = f"Wedding Vendors - {datetime.now().strftime('%Y-%m-%d')}"
                sheet_url = exporter.create_and_export(df, sheet_name)
                print(f"\n✅ Created new sheet: {sheet_url}")

            # Stats
            self.workflow_stats['export'] = {
                'vendors_exported': len(df),
                'sheet_url': sheet_url
            }

        except Exception as e:
            print(f"\n❌ Export error: {e}")
            print("   Data is still saved in CSV files")
            import traceback
            traceback.print_exc()

    def _cleanup_raw_files(self):
        """Delete raw CSV files after successful workflow."""
        print("\n" + "=" * 80)
        print("🗑️  STEP 5: CLEANUP RAW FILES")
        print("=" * 80)

        # Find raw CSV files (exclude cleaned, merged, final)
        csv_pattern = "output/vendors_*.csv"
        all_csvs = glob.glob(csv_pattern)

        raw_csvs = [
            f for f in all_csvs
            if all(x not in f for x in ['cleaned', 'merged', 'final'])
        ]

        if not raw_csvs:
            print("\n✓ No raw files to clean up")
            return

        print(f"\n🗑️  Deleting {len(raw_csvs)} raw CSV files:")
        for csv_file in raw_csvs:
            try:
                print(f"   - {Path(csv_file).name}")
                os.remove(csv_file)
            except Exception as e:
                print(f"     ⚠️  Could not delete: {e}")

        print("\n✅ Cleanup complete")

    def _print_summary(self):
        """Print workflow summary."""
        print("\n" + "=" * 80)
        print("📊 WORKFLOW SUMMARY")
        print("=" * 80)

        if self.workflow_stats.get('scraping'):
            print(f"\n📍 Scraping:")
            print(f"   Vendors scraped: {self.workflow_stats['scraping'].get('vendors_scraped', 0)}")

        if self.workflow_stats.get('cleaning'):
            print(f"\n🧹 Cleaning:")
            print(f"   Records cleaned: {self.workflow_stats['cleaning'].get('records_cleaned', 0)}")

        if self.workflow_stats.get('merging'):
            stats = self.workflow_stats['merging']
            print(f"\n🔄 Merging:")
            print(f"   Existing vendors: {stats.get('existing_count', 0)}")
            print(f"   New vendors: {stats.get('new_vendors', 0)}")
            print(f"   Final count: {stats.get('final_count', 0)}")

        if self.workflow_stats.get('export'):
            print(f"\n📤 Export:")
            print(f"   Vendors exported: {self.workflow_stats['export'].get('vendors_exported', 0)}")
            if 'sheet_url' in self.workflow_stats['export']:
                print(f"   Sheet URL: {self.workflow_stats['export']['sheet_url']}")

        print("\n" + "=" * 80)
        print(f"✅ WORKFLOW COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)


def run_integrated_workflow(
    skip_scraping: bool = False,
    skip_export: bool = False,
    auto_cleanup: bool = False,
    sheet_id: Optional[str] = None,
    config_path: str = "config/config.yaml"
) -> dict:
    """
    Run the integrated workflow.

    Args:
        skip_scraping: Skip scraping (use existing CSVs)
        skip_export: Skip Google Sheets export
        auto_cleanup: Delete raw CSVs after success
        sheet_id: Google Sheet ID
        config_path: Path to config file

    Returns:
        Workflow statistics
    """
    workflow = IntegratedWorkflow(config_path)

    return workflow.run_full_workflow(
        skip_scraping=skip_scraping,
        skip_export=skip_export,
        auto_cleanup=auto_cleanup,
        sheet_id=sheet_id
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description='Run integrated scraping, cleaning, and export workflow'
    )

    parser.add_argument(
        '--skip-scraping',
        action='store_true',
        help='Skip scraping step (use existing CSV files)'
    )
    parser.add_argument(
        '--skip-export',
        action='store_true',
        help='Skip Google Sheets export'
    )
    parser.add_argument(
        '--auto-cleanup',
        action='store_true',
        help='Automatically delete raw CSV files after successful merge'
    )
    parser.add_argument(
        '--sheet-id',
        help='Google Sheet ID for deduplication and export'
    )
    parser.add_argument(
        '--config',
        default='config/config.yaml',
        help='Path to configuration file'
    )

    args = parser.parse_args()

    run_integrated_workflow(
        skip_scraping=args.skip_scraping,
        skip_export=args.skip_export,
        auto_cleanup=args.auto_cleanup,
        sheet_id=args.sheet_id,
        config_path=args.config
    )
