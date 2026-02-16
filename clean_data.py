"""
Data Cleaning and Deduplication Script
Processes vendor CSV files: cleans data, removes duplicates, and generates reports.
"""

import sys
import os

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

import pandas as pd
import argparse
from pathlib import Path
from datetime import datetime
from processors.data_cleaner import VendorDataCleaner
from processors.deduplicator import deduplicate_vendors
from processors.csv_merger import CSVMerger
import json


def generate_cleaning_report(
    df_original: pd.DataFrame,
    df_cleaned: pd.DataFrame,
    df_deduped: pd.DataFrame,
    duplicate_log: list,
    output_dir: str
):
    """
    Generate comprehensive cleaning and deduplication report.

    Args:
        df_original: Original DataFrame
        df_cleaned: Cleaned DataFrame
        df_deduped: Deduplicated DataFrame
        duplicate_log: Log of duplicates found
        output_dir: Directory to save report
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = Path(output_dir) / f"cleaning_report_{timestamp}.txt"

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("WEDDING VENDOR DATA CLEANING & DEDUPLICATION REPORT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Overall Statistics
        f.write("📊 OVERALL STATISTICS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Original records:        {len(df_original)}\n")
        f.write(f"After cleaning:          {len(df_cleaned)}\n")
        f.write(f"After deduplication:     {len(df_deduped)}\n")
        f.write(f"Duplicates removed:      {len(df_cleaned) - len(df_deduped)}\n")
        f.write(f"Deduplication rate:      {(len(df_cleaned) - len(df_deduped)) / len(df_cleaned) * 100:.1f}%\n\n")

        # Data Completeness - Before
        f.write("📋 DATA COMPLETENESS - BEFORE CLEANING\n")
        f.write("-" * 80 + "\n")
        for col in ['name', 'phone', 'address', 'website', 'rating', 'category']:
            if col in df_original.columns:
                filled = df_original[col].notna().sum()
                filled_pct = filled / len(df_original) * 100
                f.write(f"{col:20s}: {filled:5d} / {len(df_original):5d} ({filled_pct:5.1f}%)\n")
        f.write("\n")

        # Data Completeness - After
        f.write("📋 DATA COMPLETENESS - AFTER CLEANING\n")
        f.write("-" * 80 + "\n")
        for col in ['name', 'phone', 'address', 'website', 'rating', 'category']:
            if col in df_deduped.columns:
                filled = df_deduped[col].notna().sum()
                filled_pct = filled / len(df_deduped) * 100
                f.write(f"{col:20s}: {filled:5d} / {len(df_deduped):5d} ({filled_pct:5.1f}%)\n")
        f.write("\n")

        # Data Quality Scores
        if 'quality_score' in df_deduped.columns:
            f.write("⭐ DATA QUALITY SCORES\n")
            f.write("-" * 80 + "\n")
            scores = df_deduped['quality_score']
            f.write(f"Average quality score:   {scores.mean():.1f} / 100\n")
            f.write(f"Median quality score:    {scores.median():.1f} / 100\n")
            f.write(f"High quality (>80):      {(scores > 80).sum()} ({(scores > 80).sum() / len(scores) * 100:.1f}%)\n")
            f.write(f"Medium quality (50-80):  {((scores >= 50) & (scores <= 80)).sum()} ({((scores >= 50) & (scores <= 80)).sum() / len(scores) * 100:.1f}%)\n")
            f.write(f"Low quality (<50):       {(scores < 50).sum()} ({(scores < 50).sum() / len(scores) * 100:.1f}%)\n\n")

        # Phone Number Statistics
        f.write("📞 PHONE NUMBER CLEANING\n")
        f.write("-" * 80 + "\n")
        original_phones = df_original[df_original['phone'].notna() & (df_original['phone'] != '')]['phone']
        cleaned_phones = df_deduped[df_deduped['phone'].notna() & (df_deduped['phone'] != '')]['phone']
        f.write(f"Records with phone (before): {len(original_phones)}\n")
        f.write(f"Records with phone (after):  {len(cleaned_phones)}\n")
        f.write(f"Valid E164 format:           {cleaned_phones.str.startswith('+91').sum()}\n\n")

        # Category Distribution
        if 'category' in df_deduped.columns:
            f.write("📂 CATEGORY DISTRIBUTION\n")
            f.write("-" * 80 + "\n")
            category_counts = df_deduped['category'].value_counts()
            for category, count in category_counts.head(10).items():
                f.write(f"{category:40s}: {count:4d}\n")
            f.write("\n")

        # City Distribution
        if 'city' in df_deduped.columns:
            f.write("🏙️  CITY DISTRIBUTION\n")
            f.write("-" * 80 + "\n")
            city_counts = df_deduped['city'].value_counts()
            for city, count in city_counts.head(10).items():
                f.write(f"{city:40s}: {count:4d}\n")
            f.write("\n")

        # Rating Statistics
        if 'rating' in df_deduped.columns:
            f.write("⭐ RATING STATISTICS\n")
            f.write("-" * 80 + "\n")
            ratings = df_deduped[df_deduped['rating'] > 0]['rating']
            if len(ratings) > 0:
                f.write(f"Records with ratings:    {len(ratings)}\n")
                f.write(f"Average rating:          {ratings.mean():.2f}\n")
                f.write(f"Median rating:           {ratings.median():.2f}\n")
                f.write(f"Excellent (4.5-5.0):     {((ratings >= 4.5) & (ratings <= 5.0)).sum()}\n")
                f.write(f"Good (4.0-4.5):          {((ratings >= 4.0) & (ratings < 4.5)).sum()}\n")
                f.write(f"Average (3.0-4.0):       {((ratings >= 3.0) & (ratings < 4.0)).sum()}\n")
                f.write(f"Below average (<3.0):    {(ratings < 3.0).sum()}\n")
            f.write("\n")

        # Duplicate Details
        f.write("🔍 DUPLICATE DETAILS\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total duplicate groups found: {len(duplicate_log)}\n\n")

        if duplicate_log:
            f.write("Sample duplicates (first 5 groups):\n\n")
            for i, dup in enumerate(duplicate_log[:5], 1):
                f.write(f"Group {i}:\n")
                f.write(f"  Merged into: {dup['merged_name']}\n")
                f.write(f"  Phone: {dup['merged_phone']}\n")
                f.write(f"  Original records:\n")
                for j, orig in enumerate(dup['original_records'], 1):
                    f.write(f"    {j}. {orig['name']}\n")
                    f.write(f"       Phone: {orig['phone']}\n")
                    f.write(f"       Quality: {orig['quality_score']}\n")
                f.write("\n")

        # Data Quality Issues
        f.write("⚠️  DATA QUALITY ISSUES\n")
        f.write("-" * 80 + "\n")

        # Missing critical fields
        missing_name = df_deduped[df_deduped['name'].isna() | (df_deduped['name'] == '')]
        missing_phone = df_deduped[df_deduped['phone'].isna() | (df_deduped['phone'] == '')]

        f.write(f"Records missing name:        {len(missing_name)}\n")
        f.write(f"Records missing phone:       {len(missing_phone)}\n")
        f.write(f"Records missing both:        {len(df_deduped[(df_deduped['name'].isna() | (df_deduped['name'] == '')) & (df_deduped['phone'].isna() | (df_deduped['phone'] == ''))])}\n\n")

        # Recommendations
        f.write("💡 RECOMMENDATIONS\n")
        f.write("-" * 80 + "\n")

        if len(missing_name) > 0:
            f.write(f"• {len(missing_name)} records are missing business names\n")
            f.write("  → Consider re-scraping these URLs or manual verification\n")

        if len(missing_phone) > 0:
            f.write(f"• {len(missing_phone)} records are missing phone numbers\n")
            f.write("  → These vendors may need manual contact info lookup\n")

        low_quality = df_deduped[df_deduped['quality_score'] < 50] if 'quality_score' in df_deduped.columns else []
        if len(low_quality) > 0:
            f.write(f"• {len(low_quality)} records have low quality scores (<50)\n")
            f.write("  → Review and enhance these records\n")

        f.write("\n" + "=" * 80 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 80 + "\n")

    print(f"\n📄 Cleaning report saved to: {report_path}")

    # Also save JSON version for programmatic access
    json_report_path = Path(output_dir) / f"cleaning_report_{timestamp}.json"
    with open(json_report_path, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'statistics': {
                'original_records': len(df_original),
                'cleaned_records': len(df_cleaned),
                'final_records': len(df_deduped),
                'duplicates_removed': len(df_cleaned) - len(df_deduped),
                'deduplication_rate': (len(df_cleaned) - len(df_deduped)) / len(df_cleaned) * 100 if len(df_cleaned) > 0 else 0
            },
            'duplicate_log': duplicate_log[:50]  # First 50 for JSON
        }, f, indent=2, ensure_ascii=False)

    print(f"📄 JSON report saved to: {json_report_path}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description='Clean and deduplicate wedding vendor data'
    )

    parser.add_argument(
        '--input',
        help='Input CSV file (if not provided, will merge all CSVs in output directory)'
    )
    parser.add_argument(
        '--input-dir',
        default='output',
        help='Input directory for CSV files (default: output)'
    )
    parser.add_argument(
        '--output',
        help='Output CSV file (default: auto-generated with _cleaned suffix)'
    )
    parser.add_argument(
        '--output-dir',
        default='output',
        help='Output directory (default: output)'
    )
    parser.add_argument(
        '--merge-all',
        action='store_true',
        help='Merge all vendor CSV files in input directory'
    )
    parser.add_argument(
        '--name-threshold',
        type=float,
        default=85.0,
        help='Name similarity threshold for deduplication (default: 85.0)'
    )
    parser.add_argument(
        '--address-threshold',
        type=float,
        default=80.0,
        help='Address similarity threshold for deduplication (default: 80.0)'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("🧹 WEDDING VENDOR DATA CLEANING & DEDUPLICATION")
    print("=" * 80)

    # Determine input
    if args.merge_all or not args.input:
        print(f"\n📂 Mode: Merge and clean all CSV files in '{args.input_dir}'")

        # Use CSV merger
        merger = CSVMerger()
        csv_files = merger.find_csv_files(args.input_dir, "vendors_*.csv")

        if not csv_files:
            print(f"⚠️  No CSV files found in {args.input_dir}")
            return

        print(f"\n📋 Found {len(csv_files)} CSV files:")
        for f in csv_files:
            print(f"   - {Path(f).name}")

        # Merge files
        df_original = merger.merge_files(csv_files)

    else:
        print(f"\n📂 Mode: Clean single file '{args.input}'")
        df_original = pd.read_csv(args.input)
        print(f"   Loaded {len(df_original)} records")

    if df_original.empty:
        print("⚠️  No data to process")
        return

    # Initialize cleaner
    cleaner = VendorDataCleaner()

    # Clean data
    print("\n🧹 Cleaning data...")
    df_cleaned = cleaner.clean_dataframe(df_original)
    print("   ✓ Standardized phone numbers, addresses, websites")
    print("   ✓ Cleaned business names and categories")
    print("   ✓ Validated ratings and review counts")

    # Add derived fields
    print("\n➕ Adding derived fields...")
    df_cleaned = cleaner.add_derived_fields(df_cleaned)
    print("   ✓ Extracted cities and pincodes")
    print("   ✓ Calculated quality scores")

    # Deduplicate
    print("\n🔍 Deduplicating records...")
    df_deduped, duplicate_log = deduplicate_vendors(
        df_cleaned,
        name_threshold=args.name_threshold,
        address_threshold=args.address_threshold
    )

    # Generate output filename
    if args.output:
        output_path = args.output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = str(Path(args.output_dir) / f"vendors_cleaned_{timestamp}.csv")

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Save cleaned data
    df_deduped.to_csv(output_path, index=False, encoding='utf-8')
    print(f"\n💾 Saved cleaned data to: {output_path}")

    # Generate report
    print("\n📊 Generating cleaning report...")
    generate_cleaning_report(
        df_original,
        df_cleaned,
        df_deduped,
        duplicate_log,
        args.output_dir
    )

    # Print summary
    print("\n" + "=" * 80)
    print("✅ CLEANING COMPLETE!")
    print("=" * 80)
    print(f"Original records:     {len(df_original)}")
    print(f"Cleaned records:      {len(df_cleaned)}")
    print(f"Final records:        {len(df_deduped)}")
    print(f"Duplicates removed:   {len(df_cleaned) - len(df_deduped)}")
    print(f"Deduplication rate:   {(len(df_cleaned) - len(df_deduped)) / len(df_cleaned) * 100:.1f}%")
    print("=" * 80)


if __name__ == "__main__":
    main()
