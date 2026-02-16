"""
CSV Merger Module
Intelligently merges multiple CSV files from different scraper runs.
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import glob


class CSVMerger:
    """Merges multiple vendor CSV files with intelligent field mapping."""

    def __init__(self):
        """Initialize CSV merger."""
        self.merge_stats = {
            'files_processed': 0,
            'total_records_before': 0,
            'total_records_after': 0,
            'duplicates_removed': 0
        }

    def find_csv_files(self, directory: str, pattern: str = "vendors_*.csv") -> List[str]:
        """
        Find all vendor CSV files in directory.

        Args:
            directory: Directory to search
            pattern: Glob pattern for CSV files

        Returns:
            List of CSV file paths
        """
        search_pattern = str(Path(directory) / pattern)
        csv_files = glob.glob(search_pattern)

        # Sort by filename (which includes timestamp)
        csv_files.sort()

        return csv_files

    def merge_files(
        self,
        csv_files: List[str],
        output_file: str = None
    ) -> pd.DataFrame:
        """
        Merge multiple CSV files.

        Args:
            csv_files: List of CSV file paths
            output_file: Optional output file path

        Returns:
            Merged DataFrame
        """
        print(f"\n🔄 Merging {len(csv_files)} CSV files...")

        if not csv_files:
            print("⚠️  No CSV files to merge")
            return pd.DataFrame()

        all_dfs = []
        self.merge_stats['files_processed'] = len(csv_files)

        for csv_file in csv_files:
            try:
                df = pd.read_csv(csv_file)
                print(f"   ✓ {Path(csv_file).name}: {len(df)} records")

                # Add source file info
                df['source_file'] = Path(csv_file).name

                all_dfs.append(df)
                self.merge_stats['total_records_before'] += len(df)

            except Exception as e:
                print(f"   ✗ Error reading {csv_file}: {e}")

        # Combine all dataframes
        if not all_dfs:
            print("⚠️  No data to merge")
            return pd.DataFrame()

        merged_df = pd.concat(all_dfs, ignore_index=True)

        print(f"\n✅ Merged {len(merged_df)} total records from {len(csv_files)} files")

        # Save if output path provided
        if output_file:
            merged_df.to_csv(output_file, index=False)
            print(f"💾 Saved merged data to {output_file}")

        return merged_df

    def merge_with_deduplication(
        self,
        csv_files: List[str],
        output_file: str = None,
        clean: bool = True
    ) -> pd.DataFrame:
        """
        Merge CSV files with cleaning and deduplication.

        Args:
            csv_files: List of CSV file paths
            output_file: Optional output file path
            clean: Whether to clean data before deduplication

        Returns:
            Merged and deduplicated DataFrame
        """
        from .data_cleaner import VendorDataCleaner
        from .deduplicator import deduplicate_vendors

        # Merge files
        merged_df = self.merge_files(csv_files)

        if merged_df.empty:
            return merged_df

        # Clean data if requested
        if clean:
            print("\n🧹 Cleaning merged data...")
            cleaner = VendorDataCleaner()
            merged_df = cleaner.clean_dataframe(merged_df)
            merged_df = cleaner.add_derived_fields(merged_df)

        # Deduplicate
        print("\n🔍 Deduplicating merged data...")
        deduped_df, duplicate_log = deduplicate_vendors(merged_df)

        self.merge_stats['total_records_after'] = len(deduped_df)
        self.merge_stats['duplicates_removed'] = len(merged_df) - len(deduped_df)

        # Save if output path provided
        if output_file:
            deduped_df.to_csv(output_file, index=False)
            print(f"\n💾 Saved cleaned & deduplicated data to {output_file}")

        return deduped_df

    def get_merge_report(self) -> Dict:
        """
        Get merge statistics report.

        Returns:
            Dictionary with merge statistics
        """
        return {
            **self.merge_stats,
            'deduplication_rate': (
                self.merge_stats['duplicates_removed'] /
                self.merge_stats['total_records_before'] * 100
                if self.merge_stats['total_records_before'] > 0 else 0
            )
        }

    def print_merge_report(self):
        """Print formatted merge report."""
        report = self.get_merge_report()

        print("\n" + "=" * 60)
        print("📊 MERGE REPORT")
        print("=" * 60)
        print(f"Files processed:      {report['files_processed']}")
        print(f"Total records before: {report['total_records_before']}")
        print(f"Total records after:  {report['total_records_after']}")
        print(f"Duplicates removed:   {report['duplicates_removed']}")
        print(f"Deduplication rate:   {report['deduplication_rate']:.1f}%")
        print("=" * 60)


def merge_all_vendor_csvs(
    input_dir: str = "output",
    output_file: str = None,
    pattern: str = "vendors_*.csv",
    clean_and_dedupe: bool = True
) -> pd.DataFrame:
    """
    Convenience function to merge all vendor CSVs in a directory.

    Args:
        input_dir: Directory containing CSV files
        output_file: Optional output file path
        pattern: Glob pattern for CSV files
        clean_and_dedupe: Whether to clean and deduplicate

    Returns:
        Merged DataFrame
    """
    merger = CSVMerger()

    # Find CSV files
    csv_files = merger.find_csv_files(input_dir, pattern)

    if not csv_files:
        print(f"⚠️  No CSV files found matching pattern '{pattern}' in {input_dir}")
        return pd.DataFrame()

    print(f"\n📂 Found {len(csv_files)} CSV files:")
    for f in csv_files:
        print(f"   - {Path(f).name}")

    # Merge with or without cleaning/deduplication
    if clean_and_dedupe:
        result_df = merger.merge_with_deduplication(csv_files, output_file)
    else:
        result_df = merger.merge_files(csv_files, output_file)

    # Print report
    merger.print_merge_report()

    return result_df


if __name__ == "__main__":
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Merge vendor CSV files')
    parser.add_argument(
        '--input-dir',
        default='output',
        help='Input directory containing CSV files'
    )
    parser.add_argument(
        '--output',
        help='Output CSV file path'
    )
    parser.add_argument(
        '--pattern',
        default='vendors_*.csv',
        help='Glob pattern for CSV files'
    )
    parser.add_argument(
        '--no-clean',
        action='store_true',
        help='Skip cleaning and deduplication'
    )

    args = parser.parse_args()

    result = merge_all_vendor_csvs(
        input_dir=args.input_dir,
        output_file=args.output,
        pattern=args.pattern,
        clean_and_dedupe=not args.no_clean
    )

    print(f"\n✅ Final result: {len(result)} unique vendors")
