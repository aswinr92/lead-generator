# Wedding Vendor Platform — Project Summary

**Platform:** itsmy.wedding
**Market:** Kerala, India wedding industry
**Goal:** Build a comprehensive vendor database, score business opportunities, and feed the itsmy.wedding marketplace.

---

## What We've Built

A multi-stage data pipeline that:
1. Scrapes vendor listings from Google Maps
2. Cleans, deduplicates, and enriches the data
3. Discovers Instagram/Facebook profiles for every vendor
4. Scores each vendor for sales opportunity
5. Exports to Google Sheets (current) → Supabase (next)
6. Surfaces insights through an interactive analytics dashboard

---

## Tech Stack

| Layer | Tool |
|---|---|
| Scraping | Python · Selenium · Chrome WebDriver |
| Anti-detection | `fake-useragent` · headless Chrome · randomised delays |
| Data processing | `pandas` · `thefuzz` (fuzzy dedup) · `phonenumbers` |
| Social discovery | `requests` · DuckDuckGo HTML search |
| Current storage | Google Sheets via `gspread` |
| Target storage | Supabase (PostgreSQL) |
| Analytics | `VendorOpportunityAnalyzer` (custom Python) |
| Dashboard | Streamlit · Plotly |
| Config | YAML (`config/config.yaml`) |
| Caching | JSON files in `cache/` (scraper + social finder) |

---

## Data Pipeline Architecture

```
Google Maps
    │
    ▼
OptimizedGoogleMapsVendorScraper     ← headless Chrome, 3-4× faster than original
    │  output/vendors_TIMESTAMP.csv
    ▼
VendorDataCleaner                    ← phone E.164, address normalisation,
    │                                   URL classification, social split
    ▼
VendorDeduplicator                   ← 3 strategies:
    │                                   1. exact phone match
    │                                   2. fuzzy name+address (85%/80%)
    │                                   3. name+city (90%, no-phone fallback)
    ▼
GoogleSheetsExporter                 ← professional formatting, colour-coded
    │                                   ratings, onboarding dropdown, stats tab
    ▼
SocialMediaFinder  (backfill)        ← per vendor, in priority order:
    │                                   1. listed (already in sheet)
    │                                   2. scraped from vendor's real website
    │                                   3. DuckDuckGo site:instagram.com search
    ▼
SocialMediaEnricher                  ← best-effort follower counts via
    │                                   requests (cached, graceful failure)
    ▼
VendorOpportunityAnalyzer            ← scores, tiers, pitches, LTV
    │
    ▼
Streamlit Dashboard / CSV exports    ← sales team ready
```

---

## Target Geography

**16 Kerala cities** across 3 tiers:

| City Tier | Cities |
|---|---|
| Tier 1 (priority) | Kochi · Thiruvananthapuram · Kozhikode · Thrissur |
| Tier 2 | Kollam · Alappuzha · Palakkad · Kannur · Malappuram |
| Tier 3 | Kasaragod · Pathanamthitta · Idukki · Wayanad · Guruvayur · Munnar · Varkala |

---

## Vendor Categories Scraped

Currently configured in `config/config.yaml`:
- Bridal wear designers (most recently scraped — others already complete in sheet)

**16 categories defined in Supabase schema:**
Wedding Planner · Event Management · Wedding Venue · Photographer · Videographer · Caterer · Decorator · Makeup Artist · Mehendi Artist · DJ · Live Band · Choreographer · Bridal Wear · Jewelry · Florist · Lighting

---

## Current Data Schema

### Google Sheets columns (live)

**Core (scraped from Google Maps)**

| Column | Description |
|---|---|
| `name` | Business name (title-cased) |
| `category` | Vendor category |
| `rating` | Google Maps rating (0–5) |
| `reviews_count` | Number of Google reviews |
| `address` | Full address (standardised) |
| `phone` | E.164 format (`+919995062979`) |
| `website` | Real website URL only (social media removed) |
| `url` | Google Maps profile link |
| `search_query` | Original search used |
| `scraped_at` | ISO timestamp |

**Derived (data cleaner)**

| Column | Values |
|---|---|
| `city` | Extracted from address |
| `pincode` | 6-digit Indian pincode |
| `quality_score` | 0–100 completeness score |
| `website_type` | `website` / `instagram` / `facebook` / `none` |
| `digital_presence` | `full_website` / `social_only` / `none` |

**Social media (finder + enricher)**

| Column | Source |
|---|---|
| `instagram` | Profile URL |
| `facebook` | Page URL |
| `instagram_found_via` | `listed` / `website_link` / `search` |
| `facebook_found_via` | `listed` / `website_link` / `search` |
| `instagram_followers` | Scraped count (best-effort) |
| `facebook_followers` | Scraped count (best-effort) |

**Tracking (manual)**

| Column | Values |
|---|---|
| `onboarding_status` | Not Started · In Progress · Contacted · Onboarded · Rejected |

---

## Supabase Schema (next step)

7 tables designed in `database/supabase_setup.py`:

```
vendors              — core vendor record (UUID PK, google_place_id unique)
vendor_analytics     — opportunity score, tier, LTV, pitch (1:1 with vendors)
vendor_interactions  — CRM: calls, emails, meetings, conversions
vendor_onboarding    — subscription tier, activation date, churn risk
vendor_reviews       — review text, sentiment score, keywords
categories           — reference: 16 categories with avg ticket size
cities               — reference: Kerala cities with tier classification
```

**Pre-built SQL views:**
- `hot_leads` — rating 4.7+, 150+ reviews, no website, has phone
- `tier_summary` — vendor count + opportunity value per tier
- `category_performance` — avg rating, review count, website gap per category

**Key decisions:**
- UUIDs as primary keys (not auto-increment)
- `google_place_id` as natural unique key for upsert idempotency
- Row Level Security enabled on all tables
- Trigger auto-updates `vendor_analytics` when vendor website/social changes

---

## Opportunity Scoring Model

Each vendor is scored 0–100 (capped):

| Dimension | Points | Criteria |
|---|---|---|
| Digital presence gap | 0–40 | No presence=40 · Social only=30 · Basic site=20 |
| Quality | 0–25 | Rating 4.5+=25 · 4.0+=15 · 3.5+=5 |
| Market demand | 0–20 | Reviews 200+=20 · 100+=15 · 50+=10 · 20+=5 |
| Contactability | 0–10 | Phone available=10 |
| Geography | 0–5 | Tier 1 city=5 · other=3 |
| Social influence (bonus) | 0–15 | 50K+ followers=15 · 10K+=10 · 1K+=5 |

---

## Vendor Segmentation

### Tier 1 — Premium Conversion Targets
- Rating 4.5+ · Reviews 100+ · No website · Phone available
- Expected conversion: **25–35%**
- Pitch: "You have 300+ happy clients but no website — let's showcase your work!"

### Tier 2 — Growth Vendors
- Rating 4.0–4.5 · Reviews 20–100
- Expected conversion: **15–25%**
- Pitch: "Compete with top vendors — upgrade your digital presence"

### Tier 3 — Entry Level
- Rating <4.0 or reviews <20
- Expected conversion: **5–15%**
- Pitch: "Start your digital journey — free basic profile on itsmy.wedding"

---

## Revenue Opportunity Model (per 5,000 vendors)

| Product | Target vendors | Conv. rate | Price | Revenue |
|---|---|---|---|---|
| Website Creation | ~3,000 | 10% (300) | ₹20,000 | ₹60L (one-time) |
| Premium Profile | ~2,000 | 20% (400) | ₹500/mo | ₹24L/year |
| Digital Marketing | ~1,500 | 15% (225) | ₹5,000/mo | ₹1.35Cr/year |
| Lead Generation | ~500 | 30% (150) | ₹10,000/mo | ₹1.8Cr/year |
| **Total Year 1** | | | | **~₹3.75Cr+** |

### LTV Multipliers

| Factor | Multiplier |
|---|---|
| Rating 4.5+ | 1.5× |
| Rating 4.0+ | 1.2× |
| Reviews 200+ | 2.0× |
| Reviews 100+ | 1.5× |
| No website | 1.3× |
| Followers 50K+ | 2.5× |
| Followers 10K–50K | 1.8× |
| Followers 1K–10K | 1.2× |

---

## Cross-Sell Opportunity Segments

| Segment | Target criteria | Key pitch |
|---|---|---|
| Website Creation | No website · 4.0+ rating · 50+ reviews | "Your 200 clients deserve a website" |
| Social → Website | Has Instagram/Facebook, no real website | "Turn your followers into bookings" |
| Social Power Vendors | 10K+ followers | Co-marketing, brand ambassador, affiliate |
| Premium Profile | 4.5+ rating · no website | "Be the featured vendor in your city" |
| Digital Marketing | 4.5+ rating · <50 reviews | "You're great — more people need to find you" |
| Lead Generation | 4.0+ rating · 100+ reviews · premium category | Pay-per-lead / subscription model |

---

## Key Files Reference

```
main.py                              — full pipeline (scrape → clean → export)
config/config.yaml                   — cities, categories, scraping settings

scrapers/
  google_maps_scraper_optimized.py   — headless Chrome scraper (3-4× faster)

processors/
  data_cleaner.py                    — cleaning, social URL classification
  deduplicator.py                    — 3-strategy fuzzy dedup
  social_media_finder.py             — website scraping + DDG search discovery
  social_media_enricher.py           — follower count fetching (cached)
  backfill_find_socials.py           — ★ run to find IG/FB for all vendors
  backfill_social_media.py           — classify existing URLs in sheet
  sheets_deduplicator.py             — merge new scrape with existing sheet

exporters/
  google_sheets_exporter.py          — formatted export + stats tab

analytics/
  vendor_opportunity_analyzer.py     — scoring, segmentation, pitches, LTV
  vendor_insights_dashboard.py       — Streamlit dashboard (5 tabs)

database/
  supabase_setup.py                  — full PostgreSQL schema + SQL
  migrate_to_supabase.py             — migration from CSV/Sheets to Supabase
```

---

## Running the System

```bash
# 1. Full pipeline (scrape + clean + export to Sheets)
python main.py --sheet-id YOUR_SHEET_ID

# 2. Find Instagram/Facebook for all vendors (run after pipeline)
python processors/backfill_find_socials.py --sheet-id YOUR_SHEET_ID

# 3. Re-run follower counts only
python processors/backfill_find_socials.py --sheet-id ID --no-website --no-search

# 4. Run analytics + export sales CSVs
python run_analytics.py

# 5. Launch interactive dashboard
streamlit run analytics/vendor_insights_dashboard.py
```

---

## Next Steps

### Immediate
- [ ] Run `backfill_find_socials.py` on existing sheet to populate social media data
- [ ] Review `instagram_found_via='search'` rows manually before outreach
- [ ] Expand `config.yaml` categories to scrape remaining vendor types

### Short-term
- [ ] Migrate to Supabase (`python database/migrate_to_supabase.py`)
- [ ] Connect Supabase to itsmy.wedding platform (replace Google Sheets as source of truth)
- [ ] Build sales CRM using `vendor_interactions` table
- [ ] Add email extraction from vendor websites

### Medium-term
- [ ] Add WedMeGood and WeddingWire scrapers for cross-platform coverage
- [ ] Sentiment analysis on review text (`vendor_reviews` table is ready)
- [ ] WhatsApp broadcast list automation (Tier 1 outreach)
- [ ] Proxy rotation for scraper at scale (>500 vendors/run)

### Platform integration
- [ ] Supabase → itsmy.wedding real-time sync via Postgres triggers
- [ ] Vendor self-onboarding flow driven by `vendor_onboarding` table
- [ ] Lead routing engine using `lead_generation` opportunity segment
- [ ] Analytics dashboard served from Supabase views (`hot_leads`, `tier_summary`)

---

*Last updated: February 2026*
