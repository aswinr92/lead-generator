"""
Wedding Vendor Scraper - Main Entry Point
Scrapes wedding vendors from Google Maps and saves to CSV.
"""

import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path
from scrapers.google_maps_scraper import GoogleMapsVendorScraper


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def generate_search_queries(cities: list, categories: list) -> list:
    """Generate Google Maps search queries from cities and categories."""
    queries = []
    for city in cities:
        for category in categories:
            query = f"{category} in {city}"
            queries.append(query)
    return queries


def save_to_csv(vendors: list, output_path: str):
    """Save vendor data to CSV file."""
    if not vendors:
        print("⚠️  No vendors to save")
        return

    # Create DataFrame
    df = pd.DataFrame(vendors)

    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Save to CSV
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"\n💾 Saved {len(vendors)} vendors to {output_path}")

    # Display summary
    print("\n📈 Summary:")
    print(f"   Total vendors: {len(vendors)}")
    print(f"   With phone: {df['phone'].notna().sum()}")
    print(f"   With website: {df['website'].notna().sum()}")
    print(f"   Avg rating: {df['rating'].replace('', None).astype(float).mean():.2f}")


def main():
    """Main execution function."""
    print("=" * 60)
    print("🎉 Wedding Vendor Scraper - itsmy.wedding")
    print("=" * 60)

    # Load configuration
    print("\n📋 Loading configuration...")
    config = load_config()

    cities = config['cities']
    categories = config['categories']
    scraping_config = config['scraping']

    print(f"   Cities: {', '.join(cities)}")
    print(f"   Categories: {', '.join(categories)}")
    print(f"   Max results per search: {scraping_config['max_results_per_search']}")

    # Generate search queries
    queries = generate_search_queries(cities, categories)
    print(f"\n🔎 Generated {len(queries)} search queries")

    # Initialize scraper
    print("\n🚀 Starting scraper...")
    scraper = GoogleMapsVendorScraper(
        headless=scraping_config['headless'],
        implicit_wait=scraping_config['implicit_wait']
    )

    all_vendors = []

    try:
        # Scrape each query
        for idx, query in enumerate(queries, 1):
            print(f"\n[{idx}/{len(queries)}] Processing: {query}")

            vendors = scraper.search_vendors(
                query=query,
                max_results=scraping_config['max_results_per_search']
            )

            all_vendors.extend(vendors)
            print(f"   ✓ Collected {len(vendors)} vendors")

            # Rate limiting between searches
            if idx < len(queries):
                import time
                import random
                delay = scraping_config['rate_limit_delay']
                print(f"   ⏳ Waiting {delay}s before next search...")
                time.sleep(delay + random.uniform(0, 2))

        # Generate output filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"output/vendors_{timestamp}.csv"

        # Save results
        save_to_csv(all_vendors, output_path)

    except KeyboardInterrupt:
        print("\n\n⚠️  Scraping interrupted by user")

        # Save partial results
        if all_vendors:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"output/vendors_partial_{timestamp}.csv"
            save_to_csv(all_vendors, output_path)

    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up
        scraper.close()
        print("\n" + "=" * 60)
        print("✨ Scraping completed!")
        print("=" * 60)


if __name__ == "__main__":
    main()
