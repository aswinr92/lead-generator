# Integrated Workflow Guide

**Single Command. Complete Solution.**

Run `python main.py` and get everything done: scraping, cleaning, deduplication (including existing Google Sheets data), and export.

## 🎯 Overview

The integrated workflow solves three critical problems:

1. **Only cleaned data goes to Google Sheets** - No raw, messy data
2. **No duplicate vendors across runs** - Deduplicates against existing Sheets data
3. **Single command execution** - No manual steps between scraping and export

## 🚀 Quick Start

### First Time Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Setup Google Sheets credentials (one-time)
# Follow: docs/GOOGLE_SHEETS_QUICKSTART.md

# 3. Create your first sheet and run full workflow
python main.py --interactive
```

### Subsequent Runs

```bash
# Just run this - everything is automated!
python main.py
```

The system will:
1. ✅ Scrape new vendors from Google Maps
2. ✅ Download existing vendors from your Google Sheet
3. ✅ Merge and deduplicate everything together
4. ✅ Upload only unique, cleaned vendors back to Sheets
5. ✅ Keep your data quality high

## 📋 Command Options

### Basic Commands

```bash
# Full workflow with existing sheet
python main.py --sheet-id YOUR_SHEET_ID

# Interactive mode (prompts for sheet ID)
python main.py --interactive

# Skip scraping (clean and export existing CSVs only)
python main.py --skip-scraping --sheet-id YOUR_SHEET_ID

# Skip export (scrape and clean only)
python main.py --skip-export

# Full workflow with auto-cleanup (deletes raw CSVs after success)
python main.py --sheet-id YOUR_SHEET_ID --auto-cleanup
```

### Advanced Options

```bash
# Custom config file
python main.py --config custom_config.yaml --sheet-id YOUR_SHEET_ID

# View help
python main.py --help
```

## 🔄 How It Works

### The Complete Flow

```
┌────────────────────────────────────────────────────────────┐
│ STEP 1: SCRAPE NEW VENDORS                                 │
│ - Google Maps search for configured cities/categories      │
│ - Extract vendor details                                   │
│ - Save to: output/vendors_YYYYMMDD_HHMMSS.csv (raw)       │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ STEP 2: CLEAN & MERGE RAW DATA                            │
│ - Find all raw CSV files                                   │
│ - Standardize fields (phone, address, names, etc.)        │
│ - Deduplicate within new data                             │
│ - Save to: output/vendors_cleaned_YYYYMMDD_HHMMSS.csv     │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ STEP 3: DOWNLOAD EXISTING GOOGLE SHEETS DATA              │
│ - Connect to your Google Sheet                            │
│ - Download all existing vendors                           │
│ - Prepare for merge with new data                         │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ STEP 4: SMART DEDUPLICATION                               │
│ - Merge: Existing + New data                              │
│ - Strategy 1: Exact phone match                           │
│ - Strategy 2: Fuzzy name + address (85%/80%)              │
│ - Strategy 3: Name + city (90%)                           │
│ - Keep most complete data from all sources                │
│ - Save to: output/vendors_final_YYYYMMDD_HHMMSS.csv       │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ STEP 5: EXPORT TO GOOGLE SHEETS                           │
│ - Upload final deduplicated data                          │
│ - Replace existing data (no accumulation)                 │
│ - Apply formatting (colors, filters, frozen rows)         │
│ - Generate summary statistics                             │
└────────────────────────────────────────────────────────────┘
                         ↓
┌────────────────────────────────────────────────────────────┐
│ STEP 6: CLEANUP (Optional)                                │
│ - Delete raw CSV files if --auto-cleanup enabled          │
│ - Keep cleaned and final CSVs for audit                   │
└────────────────────────────────────────────────────────────┘
```

## 🎯 Key Features

### 1. Smart Deduplication with Existing Data

**Problem:** Without this, every scraper run adds duplicates to your Sheet

**Solution:**
```bash
# Run 1:
python main.py --sheet-id ABC123
# Scrapes 50 vendors → Uploads 50 to Sheet

# Run 2 (week later):
python main.py  # Uses saved sheet ID
# Scrapes 60 vendors (20 are duplicates)
# Downloads 50 existing from Sheet
# Deduplicates: 110 total → 90 unique
# Uploads 90 to Sheet (replaces old data)

# Result: No duplicates accumulated!
```

### 2. Only Cleaned Data in Sheets

**Before integrated workflow:**
- Raw CSV → Manual cleaning → Manual deduplication → Manual export
- Risk of uploading messy data

**With integrated workflow:**
- Automatic cleaning pipeline
- Only final, validated data goes to Sheets
- Quality scores ensure data completeness

### 3. Saved Sheet ID

First run:
```bash
python main.py --sheet-id YOUR_SHEET_ID
```

This saves the sheet ID to `config/sheet_id.txt`

All subsequent runs:
```bash
python main.py  # Automatically uses saved sheet ID
```

### 4. Flexible Workflows

```bash
# Daily scraping workflow
python main.py

# Data-only update (no new scraping)
python main.py --skip-scraping

# Scrape without export (testing)
python main.py --skip-export

# Clean house (delete raw files)
python main.py --auto-cleanup
```

## 📊 Example Scenarios

### Scenario 1: First Run

```bash
$ python main.py --interactive

🎉 WEDDING VENDOR SCRAPER - itsmy.wedding
🚀 Integrated Workflow: Scrape → Clean → Deduplicate → Export

📝 Google Sheet ID not found
Enter your Google Sheet ID (or press Enter to skip export): 1a2b3c4d5e6f

⚙️  Workflow Configuration:
   Scraping:       ✓ RUN
   Cleaning:       ✓ RUN
   Deduplication:  ✓ RUN
   Export:         ✓ RUN
   Auto-cleanup:   ✗ NO
   Sheet ID:       1a2b3c4d5e6f

❓ Proceed with this configuration?
   Enter 'yes' to continue: yes

📍 STEP 1: SCRAPING VENDORS
✓ Scraped 50 vendors

🧹 STEP 2: CLEANING & DEDUPLICATION
✓ Cleaned 50 → 48 unique vendors

🔄 STEP 3: MERGE WITH EXISTING SHEETS DATA
📥 Downloading existing data...
   No existing data found
✅ Using new data only

📤 STEP 4: EXPORT TO GOOGLE SHEETS
✅ Exported 48 vendors to Google Sheets

📊 WORKFLOW SUMMARY
📍 Scraping: 50 vendors
🧹 Cleaning: 48 vendors
🔄 Merging: 0 existing + 48 new = 48 final
📤 Export: 48 vendors

✅ WORKFLOW COMPLETE
```

### Scenario 2: Second Run (Week Later)

```bash
$ python main.py

🎉 WEDDING VENDOR SCRAPER - itsmy.wedding
📋 Using saved Sheet ID: 1a2b3c4d5e6f

📍 STEP 1: SCRAPING VENDORS
✓ Scraped 60 vendors

🧹 STEP 2: CLEANING & DEDUPLICATION
✓ Cleaned 60 → 55 unique vendors

🔄 STEP 3: MERGE WITH EXISTING SHEETS DATA
📥 Downloading existing data from Google Sheets...
   ✓ Downloaded 48 existing records

🔄 Merging new data with existing data...
   Existing: 48 records
   New: 55 records
   Combined: 103 records

🔍 Deduplicating combined data...
   Found 20 duplicate groups
✅ Deduplication complete: 103 → 83 unique vendors

📊 GOOGLE SHEETS MERGE REPORT
Existing vendors in Sheets: 48
Newly scraped vendors:      55
Total before dedup:         103
Duplicates removed:         20
New unique vendors:         35
Final vendor count:         83

📤 STEP 4: EXPORT TO GOOGLE SHEETS
✅ Updated existing sheet with 83 vendors

📊 WORKFLOW SUMMARY
📍 Scraping: 60 vendors
🧹 Cleaning: 55 vendors
🔄 Merging: 48 existing + 35 new = 83 final
📤 Export: 83 vendors

✅ WORKFLOW COMPLETE
```

### Scenario 3: Clean Existing Data Only

```bash
$ python main.py --skip-scraping

⚙️  Workflow Configuration:
   Scraping:       ⏭️  SKIP
   Cleaning:       ✓ RUN

🧹 STEP 2: CLEANING & DEDUPLICATION
📂 Found 3 raw CSV files:
   - vendors_20260208_221125.csv
   - vendors_20260208_235209.csv
   - vendors_20260209_120000.csv

✓ Merged 150 records → 110 unique vendors

🔄 STEP 3: MERGE WITH EXISTING SHEETS DATA
✓ Merged with 83 existing → 140 final

📤 STEP 4: EXPORT TO GOOGLE SHEETS
✅ Updated sheet with 140 vendors

✅ WORKFLOW COMPLETE
```

## 🛡️ Data Safety

### What Gets Deleted?

**With `--auto-cleanup`:**
- ✅ **Deleted:** Raw CSV files (`vendors_YYYYMMDD_HHMMSS.csv`)
- ✅ **Kept:** Cleaned CSVs (`vendors_cleaned_*.csv`)
- ✅ **Kept:** Final CSVs (`vendors_final_*.csv`)
- ✅ **Kept:** Reports (`cleaning_report_*.txt`)

**Without `--auto-cleanup` (default):**
- ✅ **Kept:** Everything

### Backup Strategy

```bash
# 1. Raw data is in Google Sheets (always accessible)
# 2. Cleaned CSVs are always kept
# 3. Final CSVs show exactly what was uploaded

# If you need to restore:
# Download from Google Sheets → CSV → Re-import
```

## ⚙️ Configuration

### Sheet ID Storage

The sheet ID is saved to `config/sheet_id.txt`:

```
1a2b3c4d5e6f7g8h9i0j
```

You can:
- Edit this file manually
- Override with `--sheet-id` argument
- Remove file to start fresh

### Scraping Configuration

Edit `config/config.yaml`:

```yaml
cities:
  - Trivandrum
  - Kochi
  - Thrissur

categories:
  - wedding caterers
  - wedding photographers
  - wedding decorators

scraping:
  max_results_per_search: 50
  rate_limit_delay: 3
  headless: false
```

## 🐛 Troubleshooting

### Issue: "No sheet ID found"

```bash
# Solution 1: Provide sheet ID
python main.py --sheet-id YOUR_SHEET_ID

# Solution 2: Use interactive mode
python main.py --interactive

# Solution 3: Create config/sheet_id.txt manually
echo "YOUR_SHEET_ID" > config/sheet_id.txt
```

### Issue: "Could not connect to Google Sheets"

```bash
# Check credentials exist
ls config/google_credentials.json

# If missing, follow setup guide
cat docs/GOOGLE_SHEETS_QUICKSTART.md

# Test connection
python verify_setup.py
```

### Issue: "Too many duplicates kept/removed"

```bash
# Adjust thresholds by editing:
# processors/deduplicator.py

# Or run standalone cleaning with custom thresholds:
python clean_data.py --name-threshold 90 --address-threshold 85
```

### Issue: "Scraper getting blocked"

```bash
# Increase delays in config/config.yaml
scraping:
  rate_limit_delay: 5  # Increase this

# Or run in non-headless mode
scraping:
  headless: false
```

## 📈 Performance

**Typical Run Times:**
- Scraping: ~5-10 min (50 vendors)
- Cleaning: ~5-10 sec (100 records)
- Deduplication: ~10-20 sec (200 records)
- Sheets Export: ~5-10 sec
- **Total: ~6-12 minutes**

**Scalability:**
- Can handle 10,000+ vendors
- Deduplication scales linearly
- Google Sheets has 10M cells limit

## 💡 Best Practices

### 1. Regular Scraping

```bash
# Weekly workflow
python main.py

# This ensures:
# - Fresh vendor data
# - Updated ratings/reviews
# - New vendors discovered
```

### 2. Review Reports

```bash
# After each run, check:
cat output/cleaning_report_*.txt

# Look for:
# - Data quality scores
# - Missing fields
# - Duplicate patterns
```

### 3. Backup Sheet ID

```bash
# Keep a copy of your sheet ID
cat config/sheet_id.txt

# Or bookmark your sheet URL
```

### 4. Monitor Data Quality

```python
# Check quality trends
import pandas as pd

df = pd.read_csv('output/vendors_final_*.csv')
print(f"Avg quality: {df['quality_score'].mean():.1f}")
print(f"High quality: {(df['quality_score'] >= 80).sum()}")
```

### 5. Test Before Production

```bash
# Test scraping only
python main.py --skip-export

# Review data
cat output/vendors_cleaned_*.csv | head -20

# Then export
python main.py --skip-scraping --sheet-id YOUR_SHEET_ID
```

## 🔄 Comparison: Old vs New

### Old Workflow (3 Commands)

```bash
# Step 1: Scrape
python main_scraper_only.py
# Output: vendors_20260209.csv (raw, messy)

# Step 2: Clean manually
python clean_data.py --input output/vendors_20260209.csv
# Output: vendors_cleaned_20260209.csv

# Step 3: Export manually
python export_to_sheets.py --input output/vendors_cleaned_20260209.csv
# Problem: Duplicates accumulate in Sheets!

# Step 4: Manual deduplication in Sheets (ugh!)
```

**Problems:**
- ❌ Manual steps between each phase
- ❌ Easy to forget cleaning
- ❌ Duplicates accumulate in Sheets
- ❌ Need to track which files to use

### New Workflow (1 Command)

```bash
# Just this!
python main.py
```

**Benefits:**
- ✅ Fully automated pipeline
- ✅ Always cleans before export
- ✅ Deduplicates against existing Sheets data
- ✅ No duplicate accumulation
- ✅ Single source of truth

## 📚 Related Documentation

- **Quick Start:** `QUICKSTART.md`
- **Data Cleaning:** `DATA_CLEANING_GUIDE.md`
- **Google Sheets:** `GOOGLE_SHEETS_QUICKSTART.md`
- **System Overview:** `SYSTEM_OVERVIEW.md`

## 🎓 Advanced Usage

### Custom Workflow Scripts

```python
from integrated_workflow import run_integrated_workflow

# Run with custom parameters
stats = run_integrated_workflow(
    skip_scraping=False,
    skip_export=False,
    auto_cleanup=True,
    sheet_id='YOUR_SHEET_ID'
)

print(f"Final vendors: {stats['merging']['final_count']}")
```

### Scheduled Runs

```bash
# Linux/Mac crontab
0 0 * * 0 cd /path/to/scraper && python main.py >> logs/weekly.log 2>&1

# Windows Task Scheduler
# Create task to run: python main.py
# Schedule: Weekly, Sunday 12:00 AM
```

---

**Last Updated:** 2026-02-09
**Version:** 2.0.0 (Integrated Workflow)
