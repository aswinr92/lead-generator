# Wedding Vendor Scraper - itsmy.wedding

A modular Python scraper for building a wedding vendor database by extracting data from Google Maps, WedMeGood, and WeddingWire.

## 🆕 What's New in v2.0 - Integrated Workflow

**One command does everything!**

```bash
python main.py  # Scrape → Clean → Deduplicate → Export
```

**Key Features:**
- ✅ **Smart Deduplication** - Downloads existing Google Sheets data and deduplicates against it
- ✅ **No Duplicate Accumulation** - Every run ensures unique vendors only
- ✅ **Only Cleaned Data to Sheets** - Raw data never uploaded
- ✅ **Auto-Cleanup Option** - Delete raw CSVs after successful merge
- ✅ **Saved Sheet ID** - Configure once, run anytime

[Read What's New →](WHATS_NEW.md) | [Integrated Workflow Guide →](docs/INTEGRATED_WORKFLOW_GUIDE.md)

## 🎯 Project Overview

**Goal**: Scrape 10,000 wedding vendors across Kerala cities for categories like caterers, photographers, decorators, and venues.

**Phase 1** (Current): Google Maps scraper with CSV export

**Phase 2**: Google Sheets export with formatting and statistics ✅

**Phase 3** (Current): Data cleaning & deduplication ✅

**Future Phases**:
- WedMeGood & WeddingWire scrapers
- Supabase integration
- Automated enrichment

## 🏗️ Project Structure

```
wedding-vendor-scraper/
├── scrapers/              # Scraper modules
│   └── google_maps_scraper.py
├── processors/            # Data processing modules
│   ├── data_cleaner.py   # Field standardization
│   ├── deduplicator.py   # Duplicate removal
│   └── csv_merger.py     # Multi-file merger
├── exporters/             # Export modules
│   └── google_sheets_exporter.py
├── config/                # Configuration files
│   ├── config.yaml
│   └── google_credentials.json  # (You create this)
├── docs/                  # Documentation
│   ├── SYSTEM_OVERVIEW.md
│   ├── DATA_CLEANING_GUIDE.md
│   ├── CLEANING_CHEATSHEET.md
│   └── GOOGLE_SHEETS_QUICKSTART.md
├── examples/              # Usage examples
│   └── cleaning_example.py
├── output/                # CSV output files
├── main.py               # Main scraper entry point
├── clean_data.py         # Data cleaning script
├── export_to_sheets.py   # Google Sheets export script
├── requirements.txt      # Python dependencies
└── README.md
```

## 🚀 Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Setup Google Sheets (One-Time)

Follow the guide: `docs/GOOGLE_SHEETS_QUICKSTART.md`

### 3. Run Everything with One Command! ⭐

```bash
# Interactive mode (recommended for first time)
python main.py --interactive

# Or direct with sheet ID
python main.py --sheet-id YOUR_SHEET_ID

# Future runs (uses saved sheet ID)
python main.py
```

**That's it!** The system will:
1. ✅ Scrape vendors from Google Maps
2. ✅ Clean and standardize all data
3. ✅ Download existing vendors from your Google Sheet
4. ✅ Deduplicate everything together (no duplicates!)
5. ✅ Upload only unique, cleaned vendors to Sheets

**No more manual steps. No duplicate accumulation. One command. Done.** 🎉

### Alternative: Individual Steps (Advanced)

```bash
# Step 1: Scrape only
python main_scraper_only.py

# Step 2: Clean only
python clean_data.py --merge-all

# Step 3: Export only
python export_to_sheets.py
```

## ⚙️ Configuration

### Cities (Kerala)
- Trivandrum
- Kochi
- Guruvayoor
- Thrissur
- Kozhikode

### Categories
- Wedding caterers
- Wedding photographers
- Wedding decorators
- Wedding venues

### Scraping Settings
- `max_results_per_search`: 50 (adjust based on needs)
- `scroll_pause_time`: 2 seconds
- `rate_limit_delay`: 3 seconds (to avoid getting blocked)
- `headless`: false (set to true to hide browser)

## 📊 Output Format

### Raw Scraper Output
CSV file (`vendors_YYYYMMDD_HHMMSS.csv`) with columns:
- `name`: Vendor business name
- `category`: Business category
- `rating`: Google Maps rating
- `reviews_count`: Number of reviews
- `address`: Full address
- `phone`: Contact phone number
- `website`: Business website URL
- `url`: Google Maps profile URL
- `search_query`: Original search query
- `scraped_at`: Timestamp

### Cleaned Output
Enhanced CSV (`vendors_cleaned_YYYYMMDD_HHMMSS.csv`) with:
- All above fields (standardized and validated)
- `city`: Extracted city name
- `pincode`: Extracted pincode
- `quality_score`: Data completeness score (0-100)
- Detailed cleaning report (`cleaning_report_YYYYMMDD_HHMMSS.txt`)

## 🛡️ Anti-Detection Features

- Random user agents
- Rate limiting between requests
- Browser automation stealth mode
- Human-like scrolling behavior
- Configurable delays

## 🐛 Troubleshooting

### Chrome Driver Issues
The script auto-downloads ChromeDriver via `webdriver-manager`. Ensure you have Chrome browser installed.

### Getting Blocked
If Google detects automation:
1. Increase `rate_limit_delay` in config
2. Reduce `max_results_per_search`
3. Add longer random delays
4. Use residential proxies (future feature)

### Missing Data
Some vendors may not have all fields (phone, website). This is normal - the scraper extracts what's available. The data cleaner will handle missing fields and calculate quality scores for each vendor.

## 🧹 Data Cleaning & Deduplication

### Why Clean Data?

When running the scraper multiple times:
- Accumulates duplicate vendors
- Inconsistent data formats
- Missing fields in some records
- Need to merge intelligently

### Cleaning Features

**Smart Deduplication:**
- Exact phone number matching
- Fuzzy name matching (85% similarity)
- Address proximity detection
- Intelligent merging (keeps most complete data)

**Data Standardization:**
- Phone numbers: E164 format (`+919995062979`)
- Business names: Title Case with preserved acronyms
- Addresses: City standardization (Trivandrum → Thiruvananthapuram)
- Websites: URL validation, tracking parameter removal
- Ratings: Validated float conversion

**Data Enrichment:**
- City extraction from addresses
- Pincode extraction (6 digits)
- Quality scoring (0-100 based on completeness)

### Usage

```bash
# Clean all CSV files in output directory
python clean_data.py --merge-all

# Clean single file
python clean_data.py --input output/vendors_20260208.csv

# Custom deduplication thresholds
python clean_data.py --merge-all --name-threshold 90 --address-threshold 85
```

### Real Results

From 100 records (2 CSV files):
- **38% deduplication rate** → 62 unique vendors
- **100% data completeness** → All fields filled
- **78.2 avg quality score** → High quality data
- **55 valid phone numbers** → E164 format
- **4.75 avg rating** → Quality vendors

### Reports Generated

After cleaning:
- `vendors_cleaned_YYYYMMDD_HHMMSS.csv` - Cleaned data
- `cleaning_report_YYYYMMDD_HHMMSS.txt` - Human-readable report
- `cleaning_report_YYYYMMDD_HHMMSS.json` - Machine-readable stats

### Learn More

- **Quick Reference:** `docs/CLEANING_CHEATSHEET.md`
- **Full Guide:** `docs/DATA_CLEANING_GUIDE.md`
- **Examples:** `examples/cleaning_example.py`

## 📤 Export to Google Sheets

After scraping, export your vendor data to Google Sheets for collaboration and tracking:

### Prerequisites

1. **Google Cloud Setup** (one-time):
   - Follow the detailed guide: `docs/GOOGLE_SHEETS_SETUP.md`
   - Create a Google Cloud project
   - Enable Google Sheets API
   - Create service account and download credentials
   - Save credentials to `config/google_credentials.json`

### Usage

```bash
python export_to_sheets.py
```

The script will:
1. Load the latest CSV from `output/`
2. Show data preview (vendors count, categories, cities)
3. Prompt: Create new sheet or update existing
4. Export with professional formatting

### Features

- **Professional Formatting**: Bold headers, frozen rows, auto-filters, auto-sized columns
- **Color-Coded Ratings**:
  - Green (≥4.0) - Excellent
  - Yellow (3.0-4.0) - Good
  - Red (<3.0) - Needs improvement
- **Onboarding Tracking**: Dropdown column to track vendor outreach status
- **Statistics Dashboard**: Automated summary with:
  - Total vendors and averages
  - Breakdown by category and city
  - Phone/website coverage stats
  - Top 5 highest rated vendors
  - Top 5 most reviewed vendors
- **Shareable URLs**: Collaborate with your team in real-time

### Example Output

After export, you'll get:
- Shareable Google Sheets URL
- Two tabs: "Vendor Data" + "Summary & Statistics"
- Service account email (for sharing)

## 🔄 Interrupt & Resume

Press `Ctrl+C` to stop scraping. Partial results will be saved to `output/vendors_partial_TIMESTAMP.csv`.

## 📈 Scaling Tips

To reach 10,000 vendors:
1. Run scraper in batches (avoid long sessions)
2. Rotate search queries (different keywords)
3. Consider running on VPS/cloud for stability
4. Use proxy rotation (implement in next phase)

## 🔮 Roadmap

- [x] Phase 1: Google Maps scraper ✅
- [x] Phase 2: Google Sheets export ✅
- [x] Phase 3: Data cleaning & deduplication ✅ (current)
- [ ] Phase 4: WedMeGood scraper
- [ ] Phase 5: WeddingWire scraper
- [ ] Phase 6: Supabase integration
- [ ] Phase 7: Automated data enrichment
- [ ] Phase 8: Email extraction enhancement
- [ ] Phase 9: Proxy rotation support

## 📝 Notes

- **Legal**: Ensure compliance with websites' Terms of Service
- **Ethics**: Use scraped data responsibly and for legitimate business purposes
- **Rate Limiting**: Be respectful of server resources

## 🤝 Contributing

This is a startup project for itsmy.wedding. Built modular for future productization.

---

Built with ❤️ for the wedding industry in Kerala
