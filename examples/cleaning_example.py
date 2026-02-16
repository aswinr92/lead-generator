"""
Example Usage: Data Cleaning and Deduplication

This script demonstrates various ways to use the data cleaning modules.
"""

import pandas as pd
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from processors.data_cleaner import VendorDataCleaner, clean_vendor_data
from processors.deduplicator import deduplicate_vendors
from processors.csv_merger import merge_all_vendor_csvs


def example_1_clean_single_file():
    """Example 1: Clean a single CSV file."""
    print("\n" + "=" * 80)
    print("EXAMPLE 1: Clean Single CSV File")
    print("=" * 80)

    input_file = "output/vendors_20260208_235209.csv"
    output_file = "output/vendors_cleaned_example.csv"

    # Simple one-liner
    df_cleaned = clean_vendor_data(input_file, output_file)

    print(f"\n✅ Cleaned {len(df_cleaned)} records")
    print(f"   Output: {output_file}")


def example_2_step_by_step_cleaning():
    """Example 2: Step-by-step cleaning with custom logic."""
    print("\n" + "=" * 80)
    print("EXAMPLE 2: Step-by-Step Cleaning")
    print("=" * 80)

    # Load data
    df = pd.read_csv("output/vendors_20260208_235209.csv")
    print(f"\n📂 Loaded {len(df)} records")

    # Initialize cleaner
    cleaner = VendorDataCleaner()

    # Clean specific fields
    print("\n🧹 Cleaning phone numbers...")
    df['phone'] = df['phone'].apply(cleaner.clean_phone_number)
    print(f"   Valid phones: {(df['phone'] != '').sum()}")

    print("\n🧹 Cleaning business names...")
    df['name'] = df['name'].apply(cleaner.clean_business_name)
    print(f"   Valid names: {(df['name'] != '').sum()}")

    print("\n🧹 Cleaning addresses...")
    df['address'] = df['address'].apply(cleaner.clean_address)

    # Add derived fields
    print("\n➕ Extracting cities and pincodes...")
    df['city'] = df['address'].apply(cleaner.extract_city)
    df['pincode'] = df['address'].apply(cleaner.extract_pincode)

    print(f"   Cities found: {df['city'].nunique()}")
    print(f"   Pincodes found: {df['pincode'].notna().sum()}")

    # Calculate quality scores
    print("\n⭐ Calculating quality scores...")
    df['quality_score'] = df.apply(cleaner._calculate_quality_score, axis=1)
    print(f"   Average quality: {df['quality_score'].mean():.1f}/100")

    print("\n✅ Step-by-step cleaning complete!")


def example_3_deduplication():
    """Example 3: Deduplication only."""
    print("\n" + "=" * 80)
    print("EXAMPLE 3: Deduplication")
    print("=" * 80)

    # Load data
    df = pd.read_csv("output/vendors_20260208_235209.csv")
    print(f"\n📂 Loaded {len(df)} records")

    # Deduplicate with custom thresholds
    print("\n🔍 Finding duplicates...")
    df_deduped, duplicate_log = deduplicate_vendors(
        df,
        name_threshold=90.0,  # Stricter
        address_threshold=85.0
    )

    print(f"\n✅ Deduplication complete!")
    print(f"   Original: {len(df)} records")
    print(f"   Deduplicated: {len(df_deduped)} records")
    print(f"   Removed: {len(df) - len(df_deduped)} duplicates")

    # Show duplicate examples
    if duplicate_log:
        print("\n📋 First duplicate group:")
        dup = duplicate_log[0]
        print(f"   Merged into: {dup['merged_name']}")
        print(f"   Phone: {dup['merged_phone']}")
        print(f"   Original records:")
        for i, orig in enumerate(dup['original_records'], 1):
            print(f"      {i}. {orig['name']} (Quality: {orig['quality_score']})")


def example_4_merge_multiple_files():
    """Example 4: Merge multiple CSV files."""
    print("\n" + "=" * 80)
    print("EXAMPLE 4: Merge Multiple CSV Files")
    print("=" * 80)

    # Merge all vendor CSVs
    df_merged = merge_all_vendor_csvs(
        input_dir="output",
        output_file="output/all_vendors_merged.csv",
        pattern="vendors_*.csv",
        clean_and_dedupe=True
    )

    print(f"\n✅ Merged result: {len(df_merged)} unique vendors")


def example_5_custom_processing():
    """Example 5: Custom processing with filters."""
    print("\n" + "=" * 80)
    print("EXAMPLE 5: Custom Processing")
    print("=" * 80)

    # Load data
    df = pd.read_csv("output/vendors_20260208_235209.csv")
    print(f"\n📂 Loaded {len(df)} records")

    # Clean
    cleaner = VendorDataCleaner()
    df = cleaner.clean_dataframe(df)
    df = cleaner.add_derived_fields(df)

    # Filter high-quality records only
    print("\n🔍 Filtering high-quality records...")
    high_quality = df[df['quality_score'] >= 70]
    print(f"   Found {len(high_quality)} high-quality records")

    # Filter by city
    print("\n🔍 Filtering by city...")
    tvm_vendors = df[df['city'] == 'Thiruvananthapuram']
    print(f"   Found {len(tvm_vendors)} vendors in Thiruvananthapuram")

    # Filter by category
    print("\n🔍 Filtering by category...")
    caterers = df[df['category'].str.contains('Caterer', case=False, na=False)]
    print(f"   Found {len(caterers)} caterers")

    # Filter highly-rated with many reviews
    print("\n🔍 Filtering top-rated vendors...")
    top_rated = df[(df['rating'] >= 4.5) & (df['reviews_count'] >= 10)]
    print(f"   Found {len(top_rated)} top-rated vendors")

    # Save filtered results
    output_file = "output/tvm_high_quality_vendors.csv"
    result = df[
        (df['city'] == 'Thiruvananthapuram') &
        (df['quality_score'] >= 70)
    ]
    result.to_csv(output_file, index=False)
    print(f"\n💾 Saved {len(result)} filtered vendors to {output_file}")


def example_6_analyze_data_quality():
    """Example 6: Analyze data quality."""
    print("\n" + "=" * 80)
    print("EXAMPLE 6: Data Quality Analysis")
    print("=" * 80)

    # Load cleaned data
    df = pd.read_csv("output/vendors_20260208_235209.csv")

    # Clean and add quality scores
    cleaner = VendorDataCleaner()
    df = cleaner.clean_dataframe(df)
    df = cleaner.add_derived_fields(df)

    print("\n📊 Data Quality Report:")
    print("-" * 80)

    # Field completeness
    print("\n📋 Field Completeness:")
    for col in ['name', 'phone', 'address', 'website', 'rating', 'category']:
        if col in df.columns:
            filled = df[col].notna().sum()
            pct = filled / len(df) * 100
            print(f"   {col:15s}: {filled:4d}/{len(df):4d} ({pct:5.1f}%)")

    # Quality distribution
    print("\n⭐ Quality Score Distribution:")
    high = (df['quality_score'] >= 80).sum()
    medium = ((df['quality_score'] >= 50) & (df['quality_score'] < 80)).sum()
    low = (df['quality_score'] < 50).sum()

    print(f"   High (≥80):      {high:4d} ({high/len(df)*100:5.1f}%)")
    print(f"   Medium (50-79):  {medium:4d} ({medium/len(df)*100:5.1f}%)")
    print(f"   Low (<50):       {low:4d} ({low/len(df)*100:5.1f}%)")

    # Top 5 categories
    print("\n📂 Top 5 Categories:")
    top_categories = df['category'].value_counts().head()
    for category, count in top_categories.items():
        print(f"   {category:30s}: {count:3d}")

    # Top 5 cities
    print("\n🏙️  Top 5 Cities:")
    top_cities = df['city'].value_counts().head()
    for city, count in top_cities.items():
        print(f"   {city:30s}: {count:3d}")

    # Records needing attention
    print("\n⚠️  Records Needing Attention:")
    missing_name = df[df['name'].isna() | (df['name'] == '')]
    missing_phone = df[df['phone'].isna() | (df['phone'] == '')]

    print(f"   Missing name:        {len(missing_name):4d}")
    print(f"   Missing phone:       {len(missing_phone):4d}")
    print(f"   Missing both:        {len(df[(df['name'].isna() | (df['name'] == '')) & (df['phone'].isna() | (df['phone'] == ''))])}")


def main():
    """Run all examples."""
    print("\n" + "=" * 80)
    print("🎉 DATA CLEANING EXAMPLES")
    print("=" * 80)
    print("\nThese examples demonstrate various data cleaning operations.")
    print("Uncomment the examples you want to run in the main() function.")

    # Run examples (comment out ones you don't need)
    # example_1_clean_single_file()
    # example_2_step_by_step_cleaning()
    # example_3_deduplication()
    # example_4_merge_multiple_files()
    # example_5_custom_processing()
    example_6_analyze_data_quality()

    print("\n" + "=" * 80)
    print("✅ EXAMPLES COMPLETE!")
    print("=" * 80)


if __name__ == "__main__":
    main()
