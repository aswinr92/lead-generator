# Data Cleaning & Deduplication Guide

Comprehensive guide for cleaning and deduplicating wedding vendor data from multiple scraper runs.

## Overview

The data cleaning system provides:
- **Multi-file CSV merging** - Combine data from multiple scraper runs
- **Intelligent deduplication** - Remove duplicates using phone, name, and address matching
- **Data standardization** - Clean and format all fields consistently
- **Quality scoring** - Assess data completeness for each record
- **Detailed reporting** - Track what was cleaned and merged

## Quick Start

### Clean a Single CSV File

```bash
python clean_data.py --input output/vendors_20240208_221125.csv
```

### Merge and Clean All CSV Files

```bash
python clean_data.py --merge-all
```

### Custom Output Location

```bash
python clean_data.py --merge-all --output cleaned_vendors.csv
```

## Architecture

### Modules

1. **`processors/data_cleaner.py`**
   - Standardizes phone numbers (E164 format with +91)
   - Cleans business names (Title Case, whitespace normalization)
   - Standardizes addresses (city name mapping, pincode extraction)
   - Validates websites (URL format, removes tracking params)
   - Converts ratings and review counts to proper types
   - Calculates data quality scores (0-100)

2. **`processors/deduplicator.py`**
   - Strategy 1: Exact phone number match (highest priority)
   - Strategy 2: Name + Address fuzzy matching (85% similarity)
   - Strategy 3: Name similarity in same city (for records without phone)
   - Merges duplicates keeping most complete data
   - Logs all merge operations for audit trail

3. **`processors/csv_merger.py`**
   - Discovers all vendor CSV files in a directory
   - Merges files while preserving source information
   - Handles schema differences gracefully
   - Generates merge statistics

4. **`clean_data.py`**
   - Main CLI script that orchestrates the entire pipeline
   - Generates comprehensive reports (TXT and JSON)
   - Provides detailed statistics and recommendations

## Data Cleaning Operations

### Phone Numbers

**Before:**
- `099950 62979`
- `0471 272 4030`
- `9746505067`
- `1111111111` (invalid)

**After:**
- `+919995062979`
- `+914712724030`
- `+919746505067`
- `` (removed)

**Operations:**
- Remove spaces, dashes, parentheses
- Add +91 country code if missing
- Validate using phonenumbers library
- Filter out fake numbers (all 1s, 0s, 9s, etc.)
- Convert to E164 format

### Business Names

**Before:**
- `  royal events    AND WEDDING `
- `SHAMY CATERERS`
- `mangalya catering and bakery`

**After:**
- `Royal Events And Wedding`
- `Shamy Caterers`
- `Mangalya Catering And Bakery`

**Operations:**
- Remove extra whitespace
- Title Case (preserving acronyms)
- Trim leading/trailing spaces

### Addresses

**Before:**
- `Kalippankulam Rd, Manacaud, Trivandrum, Kerala 695009`
- `
N15, N St, Jawahar Nagar, Kowdiar, TVM, Kerala 695003`

**After:**
- `Kalippankulam Rd, Manacaud, Thiruvananthapuram, Kerala 695009`
- `N15, N St, Jawahar Nagar, Kowdiar, Thiruvananthapuram, Kerala 695003`

**Operations:**
- Standardize city names (Trivandrum → Thiruvananthapuram)
- Remove excessive whitespace and newlines
- Extract pincode (6 digits)
- Extract city name

**City Mappings:**
- `trivandrum`, `tvm` → `Thiruvananthapuram`
- `cochin` → `Kochi`
- `calicut` → `Kozhikode`
- `trichur` → `Thrissur`
- `alleppey` → `Alappuzha`
- `palghat` → `Palakkad`

### Websites

**Before:**
- `www.example.com`
- `http://example.com?utm_source=google&fbclid=123`
- `example.com/page`

**After:**
- `https://www.example.com`
- `http://example.com`
- `https://example.com/page`

**Operations:**
- Add `https://` if protocol missing
- Remove tracking parameters (utm_*, fbclid, gclid, etc.)
- Remove URL fragments

### Ratings & Reviews

**Operations:**
- Convert ratings to float (0.0-5.0)
- Handle missing values (default: 0.0)
- Convert review counts to integers
- Remove commas from review counts

## Deduplication Strategies

### Strategy 1: Exact Phone Match (Highest Priority)

**How it works:**
- Groups records with identical phone numbers
- Keeps record with highest quality score
- Merges data from all duplicates

**Example:**
```
Record 1: Name="Royal Events", Phone="+919995062979", Address="Kowdiar"
Record 2: Name="", Phone="+919995062979", Address="Kowdiar, Thiruvananthapuram"

Result: Name="Royal Events", Phone="+919995062979", Address="Kowdiar, Thiruvananthapuram"
```

### Strategy 2: Name + Address Similarity

**How it works:**
- Compares business names using fuzzy matching (Token Sort Ratio)
- Compares addresses using fuzzy matching (Partial Ratio)
- Matches if both exceed thresholds (default: 85% name, 80% address)

**Example:**
```
Record 1: Name="Shamy Caterers", Address="Kalippankulam Rd, Manacaud"
Record 2: Name="Shamy Catering", Address="Kalippankulam Road, Manacaud, TVM"

Similarity: Name=91%, Address=88% → Match!
```

**Thresholds:**
- Name similarity: 85% (configurable via `--name-threshold`)
- Address similarity: 80% (configurable via `--address-threshold`)

### Strategy 3: Name Similarity in Same City

**How it works:**
- For records without phone numbers
- Compares names within the same city
- Higher threshold (90%) to avoid false positives

**Example:**
```
Record 1: Name="Mangalya Catering", City="Thiruvananthapuram", Phone=""
Record 2: Name="Mangalya Caterers", City="Thiruvananthapuram", Phone=""

Similarity: Name=94%, Same City → Match!
```

### Merge Logic

When duplicates are found:
1. Select record with highest quality score as base
2. Fill missing fields from other duplicates
3. For conflicting data:
   - **Reviews count:** Take maximum
   - **Rating:** Take highest (if review counts similar)
   - **Other fields:** Prefer non-empty values

## Data Quality Score

Each record receives a quality score (0-100) based on field completeness:

| Field | Points | Criteria |
|-------|--------|----------|
| Name | 20 | Non-empty string |
| Phone | 25 | Valid E164 format (+91...) |
| Address | 15 | Length > 10 characters |
| Website | 10 | Valid URL (starts with http) |
| Rating | 15 | Rating > 0 |
| Reviews | 10 | Count > 0 |
| Category | 5 | Non-empty string |

**Quality Tiers:**
- **High Quality (>80):** Complete records ready for use
- **Medium Quality (50-80):** Usable but could be enhanced
- **Low Quality (<50):** Missing critical information

## Command-Line Options

### `clean_data.py`

```bash
# Basic usage
python clean_data.py --input <csv_file>

# Merge all CSVs
python clean_data.py --merge-all

# Custom directories
python clean_data.py --merge-all --input-dir raw_data --output-dir cleaned_data

# Custom output file
python clean_data.py --input vendors.csv --output cleaned_vendors.csv

# Adjust deduplication thresholds
python clean_data.py --merge-all --name-threshold 90 --address-threshold 85
```

**Arguments:**
- `--input`: Input CSV file path
- `--input-dir`: Directory containing CSV files (default: `output`)
- `--output`: Output CSV file path (auto-generated if not provided)
- `--output-dir`: Output directory (default: `output`)
- `--merge-all`: Merge all CSV files in input directory
- `--name-threshold`: Name similarity threshold 0-100 (default: 85)
- `--address-threshold`: Address similarity threshold 0-100 (default: 80)

## Individual Module Usage

### Data Cleaner Only

```python
from processors.data_cleaner import clean_vendor_data

df_cleaned = clean_vendor_data('input.csv', 'output_cleaned.csv')
```

### Deduplicator Only

```python
from processors.deduplicator import deduplicate_vendors
import pandas as pd

df = pd.read_csv('input.csv')
df_deduped, duplicate_log = deduplicate_vendors(df)
df_deduped.to_csv('output_deduped.csv', index=False)
```

### CSV Merger Only

```python
from processors.csv_merger import merge_all_vendor_csvs

df_merged = merge_all_vendor_csvs(
    input_dir='output',
    output_file='merged.csv',
    clean_and_dedupe=True
)
```

## Report Files

After cleaning, two report files are generated:

### 1. Text Report (`cleaning_report_YYYYMMDD_HHMMSS.txt`)

Comprehensive human-readable report including:
- Overall statistics
- Data completeness before/after
- Quality score distribution
- Phone number cleaning results
- Category and city distribution
- Rating statistics
- Duplicate details (first 5 groups)
- Data quality issues
- Recommendations

### 2. JSON Report (`cleaning_report_YYYYMMDD_HHMMSS.json`)

Machine-readable report for programmatic access:
```json
{
  "timestamp": "2024-02-08T23:52:09.123456",
  "statistics": {
    "original_records": 150,
    "cleaned_records": 150,
    "final_records": 120,
    "duplicates_removed": 30,
    "deduplication_rate": 20.0
  },
  "duplicate_log": [...]
}
```

## Handling Multiple Scraper Runs

### Problem

When running the scraper multiple times:
- Accumulates duplicate data
- Some runs may have incomplete fields
- Need to merge intelligently

### Solution

1. **Run scraper multiple times** (different queries, times, or cities)
2. **Each run generates** `vendors_YYYYMMDD_HHMMSS.csv`
3. **Use merge-all mode** to combine and deduplicate:

```bash
python clean_data.py --merge-all
```

4. **System will:**
   - Find all vendor CSV files
   - Merge them together
   - Clean all data
   - Remove duplicates intelligently
   - Keep most complete data from all runs

### Example Workflow

```bash
# Day 1: Scrape Thiruvananthapuram
python main.py
# → output/vendors_20240208_100000.csv (50 vendors)

# Day 2: Scrape Kochi
python main.py
# → output/vendors_20240209_100000.csv (60 vendors)

# Day 3: Re-scrape to get missing data
python main.py
# → output/vendors_20240210_100000.csv (55 vendors, some overlaps)

# Clean and merge all runs
python clean_data.py --merge-all
# → output/vendors_cleaned_20240210_150000.csv (120 unique vendors)
```

**Result:**
- 165 total records → 120 unique vendors
- 45 duplicates merged with most complete data
- All fields standardized and validated

## Best Practices

### 1. Regular Cleaning Schedule

```bash
# After each scraper run
python main.py  # Scrape
python clean_data.py --merge-all  # Clean immediately
```

### 2. Keep Raw Data

- Never delete original CSV files
- Cleaned files have `_cleaned` suffix
- Can always re-run cleaning with different parameters

### 3. Review Reports

- Check `cleaning_report_*.txt` for issues
- Look for patterns in low-quality records
- Identify categories/cities with missing data

### 4. Adjust Thresholds

If too many duplicates retained:
```bash
python clean_data.py --merge-all --name-threshold 80 --address-threshold 75
```

If too many false positives:
```bash
python clean_data.py --merge-all --name-threshold 90 --address-threshold 85
```

### 5. Handle Missing Names

Records missing names are kept if they have phone numbers. To re-scrape:

```python
import pandas as pd

df = pd.read_csv('vendors_cleaned.csv')
missing_names = df[df['name'].isna() | (df['name'] == '')]

# Extract URLs
urls = missing_names['url'].tolist()

# Manual re-scrape or verification
```

## Troubleshooting

### Issue: Too Many Duplicates Kept

**Solution:** Lower similarity thresholds
```bash
python clean_data.py --merge-all --name-threshold 80 --address-threshold 75
```

### Issue: False Positives (Different Vendors Merged)

**Solution:** Raise similarity thresholds
```bash
python clean_data.py --merge-all --name-threshold 90 --address-threshold 85
```

### Issue: Many Records Missing Names

**Solution:** Improved scraper with better name extraction
- Uses multiple CSS selectors
- Falls back to page title
- Skips records missing both name AND phone

### Issue: Invalid Phone Numbers

**Solution:** Cleaner automatically filters:
- All same digits (1111111111)
- Invalid formats
- Non-Indian numbers (if default_country='IN')

### Issue: Duplicate Website Tracking

**Solution:** Cleaner removes tracking params:
- `utm_*` parameters
- `fbclid`, `gclid`, `msclkid`
- URL fragments (#)

## Integration with Google Sheets

After cleaning, export to Google Sheets:

```bash
python clean_data.py --merge-all
python export_to_sheets.py --input output/vendors_cleaned_YYYYMMDD_HHMMSS.csv
```

See `GOOGLE_SHEETS_QUICKSTART.md` for setup instructions.

## Performance

**Typical Performance:**
- Cleaning: ~1000 records/second
- Deduplication: ~100 records/second (depends on similarity calculations)
- Merging: ~5000 records/second

**Example:**
- 5 CSV files, 1000 records each
- Total time: ~30-60 seconds
- Output: ~800 unique cleaned vendors

## Future Enhancements

Planned features:
- [ ] Address geocoding (lat/lng)
- [ ] Advanced duplicate detection (similar URLs)
- [ ] Machine learning quality prediction
- [ ] Automated data enrichment (fetch missing info)
- [ ] Delta updates (only process new records)
- [ ] Multi-threaded deduplication

## Support

For issues or questions:
1. Check this guide
2. Review generated reports
3. Check logs in output directory
4. Open issue on GitHub

---

**Last Updated:** 2024-02-08
**Version:** 1.0.0
