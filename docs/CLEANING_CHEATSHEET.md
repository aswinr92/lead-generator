# Data Cleaning Cheat Sheet

Quick reference for common data cleaning operations.

## 🚀 Quick Commands

```bash
# Clean all CSV files at once
python clean_data.py --merge-all

# Clean single file
python clean_data.py --input output/vendors_20240208.csv

# Custom output location
python clean_data.py --merge-all --output cleaned_vendors.csv

# Adjust deduplication sensitivity
python clean_data.py --merge-all --name-threshold 90 --address-threshold 85
```

## 📦 Installation

```bash
# Install new dependencies
pip install -r requirements.txt

# Or install individually
pip install thefuzz python-Levenshtein phonenumbers
```

## 🔧 Module Usage

### Clean Data
```python
from processors.data_cleaner import clean_vendor_data

df = clean_vendor_data('input.csv', 'output.csv')
```

### Deduplicate
```python
from processors.deduplicator import deduplicate_vendors
import pandas as pd

df = pd.read_csv('input.csv')
df_clean, log = deduplicate_vendors(df)
df_clean.to_csv('output.csv', index=False)
```

### Merge Files
```python
from processors.csv_merger import merge_all_vendor_csvs

df = merge_all_vendor_csvs(
    input_dir='output',
    output_file='merged.csv',
    clean_and_dedupe=True
)
```

## 🎯 Common Operations

### Filter High-Quality Records
```python
df = pd.read_csv('vendors_cleaned.csv')
high_quality = df[df['quality_score'] >= 80]
```

### Filter by City
```python
tvm = df[df['city'] == 'Thiruvananthapuram']
```

### Filter by Rating
```python
top_rated = df[(df['rating'] >= 4.5) & (df['reviews_count'] >= 10)]
```

### Find Missing Data
```python
missing_phone = df[df['phone'].isna() | (df['phone'] == '')]
missing_name = df[df['name'].isna() | (df['name'] == '')]
```

### Export Subset
```python
caterers = df[df['category'].str.contains('Caterer', case=False)]
caterers.to_csv('caterers_only.csv', index=False)
```

## 📊 Data Quality Score

| Score | Quality | Action |
|-------|---------|--------|
| 80-100 | High | Ready to use |
| 50-79 | Medium | Review and enhance |
| 0-49 | Low | Needs attention |

**Score Breakdown:**
- Name: 20 pts
- Phone: 25 pts
- Address: 15 pts
- Website: 10 pts
- Rating: 15 pts
- Reviews: 10 pts
- Category: 5 pts

## 🔍 Deduplication Strategies

| Strategy | Priority | Match Criteria |
|----------|----------|----------------|
| Phone Match | High | Exact phone number |
| Name + Address | Medium | 85% name + 80% address similarity |
| Name + City | Low | 90% name + same city (no phone) |

## 🧹 Cleaning Rules

### Phone Numbers
- Format: `+91XXXXXXXXXX`
- Removes: spaces, dashes, parentheses
- Validates: using phonenumbers library
- Filters: fake numbers (1111111111, etc.)

### Business Names
- Format: Title Case
- Preserves: acronyms (ABC stays ABC)
- Removes: extra whitespace

### Addresses
- Standardizes: city names
- Extracts: pincode (6 digits)
- Removes: extra whitespace/newlines

### Websites
- Adds: https:// if missing
- Removes: tracking parameters
- Cleans: URL fragments

## 📁 File Structure

```
output/
├── vendors_20240208_100000.csv      # Raw scraper output
├── vendors_20240208_120000.csv      # Another run
├── vendors_cleaned_20240208.csv     # Cleaned & merged
├── cleaning_report_20240208.txt     # Human-readable report
└── cleaning_report_20240208.json    # Machine-readable report
```

## 🐛 Troubleshooting

| Problem | Solution |
|---------|----------|
| Too many duplicates kept | Lower thresholds: `--name-threshold 80 --address-threshold 75` |
| Wrong vendors merged | Raise thresholds: `--name-threshold 90 --address-threshold 85` |
| Missing names | Re-scrape with improved scraper |
| Invalid phones | Automatic - cleaner removes them |

## 📈 Typical Results

**Before:**
- 5 CSV files
- 1000 records total
- 20% incomplete data
- 15% duplicates

**After:**
- 1 cleaned CSV
- 850 unique records
- 5% incomplete data
- 0% duplicates
- Quality score: 75/100

## 🔄 Workflow

1. **Scrape** multiple times → `vendors_*.csv`
2. **Merge** all files → `python clean_data.py --merge-all`
3. **Review** reports → `cleaning_report_*.txt`
4. **Export** to Sheets → `python export_to_sheets.py`

## 💡 Pro Tips

1. **Keep raw data** - Never delete original CSVs
2. **Regular cleaning** - Clean after each scraper run
3. **Review reports** - Check for patterns in issues
4. **Adjust thresholds** - Fine-tune based on results
5. **Manual review** - Check low-quality records

## 🎓 Learn More

- Full Guide: `docs/DATA_CLEANING_GUIDE.md`
- Examples: `examples/cleaning_example.py`
- API Docs: Read module docstrings

---

**Quick Help:**
```bash
python clean_data.py --help
```
