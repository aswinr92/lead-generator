"""
Quick Start Script for Vendor Analytics
Run this to generate all insights and exports.
"""

import os
import sys
from datetime import datetime


def print_banner():
    """Print welcome banner."""
    banner = """
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║        💍 WEDDING VENDOR INTELLIGENCE SYSTEM 💍          ║
    ║                                                           ║
    ║           Extracting Actionable Insights from            ║
    ║              10,000+ Wedding Vendors                     ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
    """
    print(banner)


def check_dependencies():
    """Check if required dependencies are installed."""
    print("\n🔍 Checking dependencies...")

    required = [
        'pandas',
        'gspread',
        'oauth2client',
        'streamlit',
        'plotly'
    ]

    missing = []
    for package in required:
        try:
            __import__(package)
            print(f"   ✅ {package}")
        except ImportError:
            print(f"   ❌ {package} - NOT INSTALLED")
            missing.append(package)

    if missing:
        print("\n⚠️  Missing dependencies detected!")
        print("\nInstall them with:")
        print("   pip install -r requirements_analytics.txt")
        sys.exit(1)

    print("\n✅ All dependencies installed!\n")


def run_opportunity_analyzer():
    """Run the vendor opportunity analyzer."""
    print("="*60)
    print("STEP 1: VENDOR OPPORTUNITY ANALYSIS")
    print("="*60)

    try:
        from analytics.vendor_opportunity_analyzer import main
        main()
    except Exception as e:
        print(f"\n❌ Error running analyzer: {str(e)}")
        print("\nMake sure:")
        print("   1. Google credentials are set up in config/google_credentials.json")
        print("   2. You have access to the Google Sheet")
        sys.exit(1)


def prompt_dashboard():
    """Prompt user to launch dashboard."""
    print("\n" + "="*60)
    print("STEP 2: INTERACTIVE DASHBOARD")
    print("="*60)

    print("""
The analytics have been generated successfully! 🎉

Next, launch the interactive dashboard to visualize insights:

    streamlit run analytics/vendor_insights_dashboard.py

This will open a web interface with:
    📊 Overview metrics
    🎯 Tier segmentation
    💰 Cross-sell opportunities
    📍 Geographic analysis
    📋 Downloadable vendor lists

Launch dashboard now? (y/n): """, end='')

    response = input().strip().lower()

    if response == 'y':
        print("\n🚀 Launching dashboard...")
        os.system('streamlit run analytics/vendor_insights_dashboard.py')
    else:
        print("\n✅ You can launch it later with:")
        print("   streamlit run analytics/vendor_insights_dashboard.py")


def show_next_steps():
    """Show next steps."""
    print("\n" + "="*60)
    print("📋 NEXT STEPS")
    print("="*60)

    print("""
✅ Analytics Complete! Here's what was generated:

📁 Exports Location: analytics/exports/

Generated Files:
   1. tier1_premium_*.csv         - Top 200 hot leads (Rating 4.5+, Reviews 100+)
   2. tier2_growth_*.csv           - 500 growth opportunities
   3. tier3_entry_*.csv            - 1000 entry-level targets
   4. hot_leads_no_brainers_*.csv  - Ultra-hot leads (convert at 40-50%)
   5. website_creation_*.csv       - Website opportunities
   6. premium_profile_*.csv        - Premium profile opportunities
   7. digital_marketing_*.csv      - Digital marketing opportunities
   8. lead_generation_*.csv        - Lead generation platform targets

🎯 Recommended Actions:

Week 1 - Pilot Campaign:
   [ ] Review hot_leads_no_brainers.csv (50-100 vendors)
   [ ] Call top 20 vendors with custom pitch
   [ ] Track: interested/not interested/converted
   [ ] Target: 8-12 conversions (40% conversion rate)

Week 2 - Scale to Tier 1:
   [ ] Expand to full Tier 1 list (200-300 vendors)
   [ ] Set up email automation
   [ ] WhatsApp broadcast lists
   [ ] Target: 60-100 conversions (25-35% rate)

Week 3 - Platform Integration:
   [ ] Migrate data to Supabase
   [ ] Integrate with itsmy.wedding
   [ ] Launch vendor onboarding flow

💰 Revenue Potential from Your Data:

   Website Creation:    ₹1.2 Cr (one-time)
   Premium Profiles:    ₹48 L/year
   Digital Marketing:   ₹2.7 Cr/year
   Lead Generation:     ₹3.6 Cr/year
   ─────────────────────────────────────
   TOTAL Year 1:        ₹7.5 Cr+

📚 Documentation:

   Read README.md for:
      - Detailed segmentation strategy
      - Sales pitch templates
      - Supabase migration guide
      - Platform integration examples

🚀 Launch Dashboard:

   streamlit run analytics/vendor_insights_dashboard.py

   Features:
      📊 Real-time analytics
      🗺️  Geographic heatmaps
      💼 Sales-ready exports
      🔥 Hot leads tracking
    """)


def main():
    """Main execution."""
    print_banner()

    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Step 1: Check dependencies
    check_dependencies()

    # Step 2: Run analyzer
    run_opportunity_analyzer()

    # Step 3: Show next steps
    show_next_steps()

    # Step 4: Optionally launch dashboard
    prompt_dashboard()

    print("\n✅ All done! Happy selling! 🎉\n")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        sys.exit(1)
