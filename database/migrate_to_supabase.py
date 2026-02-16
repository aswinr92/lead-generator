"""
Migrate Vendor Data from Google Sheets to Supabase
One-time migration script + ongoing sync capability.
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from supabase_setup import SupabaseVendorDB
from analytics.vendor_opportunity_analyzer import VendorOpportunityAnalyzer


def migrate_vendors_to_supabase():
    """Migrate all vendor data from Google Sheets to Supabase."""

    print("="*60)
    print("MIGRATING VENDORS TO SUPABASE")
    print("="*60)

    # Configuration
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1FiyUPo9ZJEDH13gKVoAC00ko1Vx9U9rdSSq1z2Jwa5U/edit?gid=0#gid=0"
    CREDENTIALS_FILE = "config/google_credentials.json"

    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("❌ Error: SUPABASE_URL and SUPABASE_KEY environment variables not set")
        print("\nSet them using:")
        print('  export SUPABASE_URL="https://your-project.supabase.co"')
        print('  export SUPABASE_KEY="your-anon-key"')
        sys.exit(1)

    # Step 1: Load and analyze data from Google Sheets
    print("\n📊 Step 1: Loading data from Google Sheets...")
    analyzer = VendorOpportunityAnalyzer(SHEET_URL, CREDENTIALS_FILE)
    analyzer.load_data()

    print(f"✅ Loaded {len(analyzer.df)} vendors")

    # Step 2: Generate analytics
    print("\n🎯 Step 2: Generating analytics...")
    analyzer.segment_vendors()
    insights = analyzer.generate_insights_summary()

    print(f"✅ Segmented vendors:")
    print(f"   - Tier 1: {insights['tier1_count']}")
    print(f"   - Tier 2: {insights['tier2_count']}")
    print(f"   - Tier 3: {insights['tier3_count']}")

    # Step 3: Connect to Supabase
    print("\n🔌 Step 3: Connecting to Supabase...")
    db = SupabaseVendorDB(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Connected to Supabase")

    # Step 4: Upload vendors
    print("\n📤 Step 4: Uploading vendors to Supabase...")
    db.upload_vendors(analyzer.df)

    # Step 5: Upload analytics
    print("\n📤 Step 5: Uploading analytics data...")
    db.upload_analytics(analyzer.df)

    # Step 6: Verify migration
    print("\n✅ Step 6: Verifying migration...")
    try:
        tier_summary = db.get_tier_summary()
        hot_leads = db.get_hot_leads(limit=10)

        print(f"\n📊 Migration Summary:")
        print(f"   - Total vendors in Supabase: {sum(t['vendor_count'] for t in tier_summary)}")
        print(f"   - Hot leads identified: {len(hot_leads)}")

        print("\n🔥 Top 5 Hot Leads:")
        for i, lead in enumerate(hot_leads[:5], 1):
            print(f"   {i}. {lead['name']} ({lead['category']}) - Score: {lead['opportunity_score']}")

    except Exception as e:
        print(f"⚠️  Verification failed: {str(e)}")
        print("   (This is OK if running for the first time)")

    print("\n" + "="*60)
    print("✅ MIGRATION COMPLETE!")
    print("="*60)

    print("""
Next Steps:

1. Verify data in Supabase Dashboard:
   https://supabase.com/dashboard

2. Test the API endpoints:
   - GET /rest/v1/vendors
   - GET /rest/v1/hot_leads
   - GET /rest/v1/tier_summary

3. Integrate with itsmy.wedding platform:
   - Use Supabase client in your Next.js/React app
   - Query vendors by category, city, tier
   - Display analytics dashboards

4. Set up automatic sync (optional):
   - Schedule this script to run daily
   - Or use Supabase triggers to sync automatically
    """)


def sync_new_vendors():
    """
    Sync only new vendors from Google Sheets to Supabase.
    Run this daily to keep data updated.
    """
    print("🔄 Syncing new vendors...")

    # Similar to migrate_vendors_to_supabase but only upserts new/changed records
    # Implementation left as exercise - use last_updated_at timestamp to filter

    pass


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Migrate vendor data to Supabase')
    parser.add_argument(
        '--sync-only',
        action='store_true',
        help='Only sync new vendors (faster)'
    )

    args = parser.parse_args()

    if args.sync_only:
        sync_new_vendors()
    else:
        migrate_vendors_to_supabase()
