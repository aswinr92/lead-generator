"""
Vendor Opportunity Analyzer
Analyzes vendor data to identify cross-selling opportunities and generate actionable insights.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import gspread
from oauth2client.service_account import ServiceAccountCredentials


class VendorOpportunityAnalyzer:
    """Analyzes vendor data to identify sales opportunities."""

    def __init__(self, sheet_url: str, credentials_file: str):
        """
        Initialize analyzer with Google Sheets connection.

        Args:
            sheet_url: Google Sheets URL
            credentials_file: Path to Google credentials JSON
        """
        self.sheet_url = sheet_url
        self.credentials_file = credentials_file
        self.df = None
        self.insights = {}

    def load_data(self) -> pd.DataFrame:
        """Load vendor data from Google Sheets."""
        print("📊 Loading data from Google Sheets...")

        # Set up Google Sheets connection
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file, scope
        )
        client = gspread.authorize(creds)

        # Open sheet and get data
        sheet = client.open_by_url(self.sheet_url)
        worksheet = sheet.get_worksheet(0)  # First worksheet

        # Convert to DataFrame
        data = worksheet.get_all_records()
        self.df = pd.DataFrame(data)

        # Clean data types
        self.df['rating'] = pd.to_numeric(self.df['rating'], errors='coerce')
        self.df['reviews_count'] = pd.to_numeric(self.df['reviews_count'], errors='coerce')
        self.df['quality_score'] = pd.to_numeric(self.df['quality_score'], errors='coerce')

        print(f"✅ Loaded {len(self.df)} vendors")
        return self.df

    def calculate_opportunity_score(self, row: pd.Series) -> int:
        """
        Calculate opportunity score for a vendor (0-100).

        Scoring criteria:
        - Digital presence gap: 40 points
        - Quality indicator: 25 points
        - Market demand: 20 points
        - Contactability: 10 points
        - Geography: 5 points
        """
        score = 0

        # Digital presence gap (max 40 points)
        digital_presence = row.get('digital_presence', '')
        website = str(row.get('website', '') or '')
        instagram = str(row.get('instagram', '') or '')
        facebook = str(row.get('facebook', '') or '')

        if digital_presence == 'none' or (not website and not instagram and not facebook):
            score += 40  # No digital presence at all — highest opportunity
        elif digital_presence == 'social_only' or (not website and (instagram or facebook)):
            score += 30  # Social media only — still needs a real website
        elif self._is_basic_website(website):
            score += 20  # Has a basic/social website

        # Quality indicator (max 25 points)
        if row['rating'] >= 4.5:
            score += 25
        elif row['rating'] >= 4.0:
            score += 15
        elif row['rating'] >= 3.5:
            score += 5

        # Market demand (max 20 points)
        if row['reviews_count'] >= 200:
            score += 20
        elif row['reviews_count'] >= 100:
            score += 15
        elif row['reviews_count'] >= 50:
            score += 10
        elif row['reviews_count'] >= 20:
            score += 5

        # Contactability (max 10 points)
        if not pd.isna(row['phone']) and row['phone'] != '':
            score += 10

        # Geography - Tier 1 cities (max 5 points)
        tier_1_cities = ['Kochi', 'Thiruvananthapuram', 'Kozhikode', 'Thrissur']
        if row.get('city') in tier_1_cities:
            score += 5
        else:
            score += 3

        # Social influence bonus (max 15 points — applied last, still capped at 100)
        ig_followers = self._safe_int(row.get('instagram_followers'))
        fb_followers = self._safe_int(row.get('facebook_followers'))
        max_followers = max(ig_followers, fb_followers)

        if max_followers >= 50000:
            score += 15
        elif max_followers >= 10000:
            score += 10
        elif max_followers >= 1000:
            score += 5

        return min(score, 100)

    @staticmethod
    def _safe_int(val) -> int:
        """Safely convert a value to int, returning 0 on failure."""
        try:
            if val is None or str(val).strip() in ('', 'nan', 'None'):
                return 0
            return int(float(str(val).replace(',', '')))
        except (ValueError, TypeError):
            return 0

    def _is_basic_website(self, url: str) -> bool:
        """Check if website is a low-quality placeholder (e.g. free link-in-bio sites)."""
        if pd.isna(url) or url == '':
            return False
        # Social media URLs are now split into separate columns, but handle legacy data
        basic_indicators = ['facebook.com', 'instagram.com', 'welcomeyou.in', 'linktr.ee', 'linkin.bio']
        return any(indicator in str(url).lower() for indicator in basic_indicators)

    def segment_vendors(self) -> pd.DataFrame:
        """
        Segment vendors into Tier 1/2/3 based on opportunity.

        Returns:
            DataFrame with added 'tier' and 'opportunity_score' columns
        """
        print("\n🎯 Segmenting vendors...")

        # Calculate opportunity score for each vendor
        self.df['opportunity_score'] = self.df.apply(
            self.calculate_opportunity_score, axis=1
        )

        # Tier 1: Premium Conversion Targets (Top 15%)
        # Rating 4.5+, Reviews 100+, No website, Phone available
        tier1_mask = (
            (self.df['rating'] >= 4.5) &
            (self.df['reviews_count'] >= 100) &
            (self.df['website'].isna() | (self.df['website'] == '')) &
            (self.df['phone'].notna() & (self.df['phone'] != ''))
        )

        # Tier 2: Growth Vendors (Middle 40%)
        # Rating 4.0-4.5, Reviews 20-100
        tier2_mask = (
            (self.df['rating'] >= 4.0) &
            (self.df['rating'] < 4.5) &
            (self.df['reviews_count'] >= 20) &
            (self.df['reviews_count'] < 100) &
            ~tier1_mask
        )

        # Tier 3: Entry-Level (Bottom 45%)
        # Everything else
        tier3_mask = ~(tier1_mask | tier2_mask)

        # Assign tiers
        self.df.loc[tier1_mask, 'tier'] = 'Tier 1 - Premium'
        self.df.loc[tier2_mask, 'tier'] = 'Tier 2 - Growth'
        self.df.loc[tier3_mask, 'tier'] = 'Tier 3 - Entry'

        # Calculate estimated revenue potential
        self.df['estimated_ltv'] = self.df.apply(self._calculate_ltv, axis=1)

        # Generate sales pitch
        self.df['recommended_pitch'] = self.df.apply(self._generate_pitch, axis=1)

        print(f"✅ Tier 1 (Premium): {tier1_mask.sum()} vendors")
        print(f"✅ Tier 2 (Growth): {tier2_mask.sum()} vendors")
        print(f"✅ Tier 3 (Entry): {tier3_mask.sum()} vendors")

        return self.df

    def _calculate_ltv(self, row: pd.Series) -> int:
        """Calculate estimated lifetime value for vendor."""
        # Base LTV by category
        category_ltv = {
            'Wedding Planner': 500000,
            'Event Planner': 500000,
            'Event Management': 400000,
            'Wedding venue': 300000,
            'Banquet Hall': 300000,
            'Photographer': 150000,
            'Videographer': 150000,
            'Caterer': 100000,
            'Makeup Artist': 80000,
            'Decorator': 80000,
            'Mehendi Artist': 60000,
        }

        # Get base LTV
        ltv = 50000  # Default
        for key in category_ltv:
            if key.lower() in str(row['category']).lower():
                ltv = category_ltv[key]
                break

        # Quality multiplier
        if row['rating'] >= 4.5:
            ltv *= 1.5
        elif row['rating'] >= 4.0:
            ltv *= 1.2

        # Scale multiplier
        if row['reviews_count'] >= 200:
            ltv *= 2.0
        elif row['reviews_count'] >= 100:
            ltv *= 1.5

        # Digital gap multiplier
        if pd.isna(row['website']) or row['website'] == '':
            ltv *= 1.3

        # Social influence multiplier — high-follower vendors have more platform value
        max_followers = max(
            self._safe_int(row.get('instagram_followers')),
            self._safe_int(row.get('facebook_followers'))
        )
        if max_followers >= 50000:
            ltv *= 2.5   # Brand ambassador / co-marketing potential
        elif max_followers >= 10000:
            ltv *= 1.8
        elif max_followers >= 1000:
            ltv *= 1.2

        return int(ltv)

    def _generate_pitch(self, row: pd.Series) -> str:
        """Generate recommended sales pitch based on vendor profile."""
        tier = row.get('tier', '')
        ig_followers = self._safe_int(row.get('instagram_followers'))
        fb_followers = self._safe_int(row.get('facebook_followers'))
        max_followers = max(ig_followers, fb_followers)

        # Build social proof line
        if max_followers >= 50000:
            social_line = f"With {max_followers:,} followers on social media, you're already an influencer! "
        elif max_followers >= 10000:
            social_line = f"Your {max_followers:,} social media followers are waiting to book you. "
        elif max_followers >= 1000:
            social_line = f"Your {max_followers:,} followers show real demand. "
        else:
            social_line = ""

        has_website = str(row.get('website') or '').startswith('http')
        has_social = bool(row.get('instagram') or row.get('facebook'))
        reviews = self._safe_int(row.get('reviews_count'))
        rating = row.get('rating', 0)

        if 'Tier 1' in tier:
            if max_followers >= 10000:
                return (
                    f"{social_line}But {reviews} Google reviews and no real website means you're leaving "
                    f"bookings on the table — let's build your itsmy.wedding profile today!"
                )
            return (
                f"You have {reviews} happy clients with a {rating} rating, but no website! "
                f"Let's showcase your work with a premium profile on itsmy.wedding."
            )
        elif 'Tier 2' in tier:
            if has_social and not has_website:
                return (
                    f"{social_line}Turn your social following into a bookable website. "
                    f"With {reviews} reviews, you're ready to scale with itsmy.wedding!"
                )
            return (
                f"Compete with top vendors — upgrade your digital presence. "
                f"With {reviews} reviews, you're ready to scale!"
            )
        else:
            if max_followers >= 1000:
                return (
                    f"{social_line}Convert your followers to paying clients — "
                    f"start with a free profile on itsmy.wedding."
                )
            return (
                "Start your digital journey — get a free basic profile on "
                "itsmy.wedding and reach more customers."
            )

    def identify_cross_sell_opportunities(self) -> Dict[str, pd.DataFrame]:
        """
        Identify specific cross-selling opportunities.

        Returns:
            Dictionary with opportunity type as key and DataFrame as value
        """
        print("\n💡 Identifying cross-sell opportunities...")

        opportunities = {}

        # Helper: no real website (may have social or nothing)
        no_real_website = self.df['website'].fillna('') == ''
        has_social = (
            (self.df.get('instagram', pd.Series([''] * len(self.df))).fillna('') != '') |
            (self.df.get('facebook', pd.Series([''] * len(self.df))).fillna('') != '')
        )

        # 1. Website Creation Service (no real website, qualified by rating/reviews)
        opportunities['website_creation'] = self.df[
            no_real_website &
            (self.df['rating'] >= 4.0) &
            (self.df['reviews_count'] >= 50)
        ].sort_values('opportunity_score', ascending=False)

        # 1b. Social → Website Upgrade (has Instagram/Facebook but no real website)
        opportunities['social_to_website'] = self.df[
            no_real_website &
            has_social &
            (self.df['rating'] >= 3.5)
        ].sort_values('opportunity_score', ascending=False)

        # 2. Premium Profile (High performers without a real website)
        opportunities['premium_profile'] = self.df[
            (self.df['rating'] >= 4.5) &
            no_real_website
        ].sort_values('reviews_count', ascending=False)

        # 3. Digital Marketing (Great service, poor visibility)
        opportunities['digital_marketing'] = self.df[
            (self.df['rating'] >= 4.5) &
            (self.df['reviews_count'] < 50) &
            (self.df['website'].isna() | (self.df['website'] == ''))
        ].sort_values('rating', ascending=False)

        # 4. Lead Generation Platform (Established vendors)
        high_value_categories = [
            'Caterer', 'Photographer', 'Videographer', 'Wedding venue',
            'Event Planner', 'Wedding Planner', 'Banquet Hall'
        ]
        cat_mask = self.df['category'].str.contains(
            '|'.join(high_value_categories),
            case=False,
            na=False
        )
        opportunities['lead_generation'] = self.df[
            (self.df['rating'] >= 4.0) &
            (self.df['reviews_count'] >= 100) &
            cat_mask
        ].sort_values('reviews_count', ascending=False)

        # 5. Social Power Vendors (10K+ followers — co-marketing / brand partner potential)
        ig_followers_num = pd.to_numeric(
            self.df.get('instagram_followers', pd.Series([0] * len(self.df))),
            errors='coerce'
        ).fillna(0)
        fb_followers_num = pd.to_numeric(
            self.df.get('facebook_followers', pd.Series([0] * len(self.df))),
            errors='coerce'
        ).fillna(0)
        max_followers_series = ig_followers_num.combine(fb_followers_num, max)

        # Add max_followers temp column for sorting
        self.df['_max_followers'] = max_followers_series
        opportunities['social_power_vendors'] = self.df[
            max_followers_series >= 10000
        ].sort_values('_max_followers', ascending=False)
        self.df.drop(columns=['_max_followers'], inplace=True, errors='ignore')

        # 6. Category Leaders (Top 3 per category per city)
        opportunities['category_leaders'] = self._identify_category_leaders()

        # Print summary
        print(f"✅ Website Creation: {len(opportunities['website_creation'])} vendors")
        print(f"✅ Social → Website: {len(opportunities['social_to_website'])} vendors")
        print(f"✅ Social Power (10K+): {len(opportunities['social_power_vendors'])} vendors")
        print(f"✅ Premium Profile: {len(opportunities['premium_profile'])} vendors")
        print(f"✅ Digital Marketing: {len(opportunities['digital_marketing'])} vendors")
        print(f"✅ Lead Generation: {len(opportunities['lead_generation'])} vendors")
        print(f"✅ Category Leaders: {len(opportunities['category_leaders'])} vendors")

        return opportunities

    def _identify_category_leaders(self) -> pd.DataFrame:
        """Identify top 3 vendors in each category per city."""
        leaders = []

        for city in self.df['city'].unique():
            for category in self.df['category'].unique():
                city_cat = self.df[
                    (self.df['city'] == city) &
                    (self.df['category'] == category)
                ].sort_values('reviews_count', ascending=False).head(3)

                leaders.append(city_cat)

        return pd.concat(leaders).drop_duplicates()

    def generate_insights_summary(self) -> Dict:
        """Generate high-level insights summary."""
        print("\n📈 Generating insights summary...")

        insights = {
            'total_vendors': len(self.df),
            'total_opportunity_value': self.df['estimated_ltv'].sum(),

            # Digital presence breakdown
            'vendors_with_real_website': len(self.df[
                self.df['website'].fillna('') != ''
            ]),
            'vendors_social_only': len(self.df[
                self.df.get('digital_presence', pd.Series([''] * len(self.df))).fillna('') == 'social_only'
            ]),
            'vendors_without_website': len(self.df[
                self.df['website'].fillna('') == ''
            ]),
            'vendors_without_phone': len(self.df[
                self.df['phone'].isna() | (self.df['phone'] == '')
            ]),

            # Quality metrics
            'high_rated_vendors': len(self.df[self.df['rating'] >= 4.5]),
            'avg_rating': self.df['rating'].mean(),
            'avg_reviews': self.df['reviews_count'].mean(),

            # Tier distribution
            'tier1_count': len(self.df[self.df['tier'] == 'Tier 1 - Premium']),
            'tier2_count': len(self.df[self.df['tier'] == 'Tier 2 - Growth']),
            'tier3_count': len(self.df[self.df['tier'] == 'Tier 3 - Entry']),

            # Revenue potential
            'tier1_revenue': self.df[
                self.df['tier'] == 'Tier 1 - Premium'
            ]['estimated_ltv'].sum(),
            'tier2_revenue': self.df[
                self.df['tier'] == 'Tier 2 - Growth'
            ]['estimated_ltv'].sum(),
            'tier3_revenue': self.df[
                self.df['tier'] == 'Tier 3 - Entry'
            ]['estimated_ltv'].sum(),

            # Geographic distribution
            'cities': self.df['city'].value_counts().to_dict(),
            'categories': self.df['category'].value_counts().to_dict(),

            # Quick wins
            'no_brainer_count': len(self.df[
                (self.df['rating'] >= 4.7) &
                (self.df['reviews_count'] >= 150) &
                (self.df['website'].isna() | (self.df['website'] == '')) &
                (self.df['phone'].notna() & (self.df['phone'] != ''))
            ]),

            # Social media reach
            'vendors_with_instagram': len(self.df[self.df.get('instagram', pd.Series([''] * len(self.df))).fillna('') != '']),
            'vendors_with_facebook': len(self.df[self.df.get('facebook', pd.Series([''] * len(self.df))).fillna('') != '']),
            'vendors_10k_plus': len(self.df[
                pd.to_numeric(self.df.get('instagram_followers', 0), errors='coerce').fillna(0).combine(
                    pd.to_numeric(self.df.get('facebook_followers', 0), errors='coerce').fillna(0), max
                ) >= 10000
            ]),
            'vendors_50k_plus': len(self.df[
                pd.to_numeric(self.df.get('instagram_followers', 0), errors='coerce').fillna(0).combine(
                    pd.to_numeric(self.df.get('facebook_followers', 0), errors='coerce').fillna(0), max
                ) >= 50000
            ]),
            'total_instagram_followers': int(
                pd.to_numeric(self.df.get('instagram_followers', 0), errors='coerce').fillna(0).sum()
            ),
        }

        self.insights = insights
        return insights

    def export_sales_lists(self, output_dir: str = 'analytics/exports'):
        """Export prioritized sales lists as CSV files."""
        print(f"\n📤 Exporting sales lists to {output_dir}...")

        import os
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Export by tier
        for tier in ['Tier 1 - Premium', 'Tier 2 - Growth', 'Tier 3 - Entry']:
            tier_df = self.df[self.df['tier'] == tier].sort_values(
                'opportunity_score', ascending=False
            )

            # Select key columns for sales team
            base_cols = [
                'name', 'category', 'city', 'phone', 'rating',
                'reviews_count', 'website', 'instagram', 'facebook',
                'digital_presence', 'instagram_followers', 'facebook_followers',
                'opportunity_score', 'estimated_ltv', 'recommended_pitch', 'tier'
            ]
            export_cols = [c for c in base_cols if c in self.df.columns]

            tier_name = tier.replace(' ', '_').replace('-', '').lower()
            filename = f"{output_dir}/{tier_name}_{timestamp}.csv"
            tier_df[export_cols].to_csv(filename, index=False)
            print(f"✅ Exported {len(tier_df)} vendors to {filename}")

        # Export cross-sell opportunities
        opportunities = self.identify_cross_sell_opportunities()
        for opp_type, opp_df in opportunities.items():
            filename = f"{output_dir}/{opp_type}_{timestamp}.csv"
            opp_df[export_cols].head(200).to_csv(filename, index=False)
            print(f"✅ Exported {len(opp_df)} vendors to {filename}")

        # Export "No-Brainer" hot leads
        no_brainers = self.df[
            (self.df['rating'] >= 4.7) &
            (self.df['reviews_count'] >= 150) &
            (self.df['website'].isna() | (self.df['website'] == '')) &
            (self.df['phone'].notna() & (self.df['phone'] != ''))
        ].sort_values('reviews_count', ascending=False)

        filename = f"{output_dir}/hot_leads_no_brainers_{timestamp}.csv"
        no_brainers[export_cols].to_csv(filename, index=False)
        print(f"✅ Exported {len(no_brainers)} hot leads to {filename}")

        print("\n🎉 All exports complete!")

    def print_insights_report(self):
        """Print a formatted insights report."""
        if not self.insights:
            self.generate_insights_summary()

        print("\n" + "="*60)
        print("📊 VENDOR INSIGHTS REPORT")
        print("="*60)

        print(f"\n📈 OVERVIEW")
        print(f"   Total Vendors: {self.insights['total_vendors']:,}")
        print(f"   Total Opportunity Value: ₹{self.insights['total_opportunity_value']/10000000:.2f} Cr")
        print(f"   Avg Rating: {self.insights['avg_rating']:.2f}")
        print(f"   Avg Reviews: {self.insights['avg_reviews']:.0f}")

        print(f"\n🎯 TIER DISTRIBUTION")
        print(f"   Tier 1 (Premium): {self.insights['tier1_count']} vendors "
              f"(₹{self.insights['tier1_revenue']/10000000:.2f} Cr)")
        print(f"   Tier 2 (Growth): {self.insights['tier2_count']} vendors "
              f"(₹{self.insights['tier2_revenue']/10000000:.2f} Cr)")
        print(f"   Tier 3 (Entry): {self.insights['tier3_count']} vendors "
              f"(₹{self.insights['tier3_revenue']/10000000:.2f} Cr)")

        total = self.insights['total_vendors']
        print(f"\n💎 DIGITAL PRESENCE BREAKDOWN")
        print(f"   Real Website: {self.insights['vendors_with_real_website']} vendors "
              f"({self.insights['vendors_with_real_website']/total*100:.1f}%)")
        print(f"   Social Only (Instagram/Facebook): {self.insights['vendors_social_only']} vendors "
              f"({self.insights['vendors_social_only']/total*100:.1f}%)")
        print(f"   No Presence: "
              f"{total - self.insights['vendors_with_real_website'] - self.insights['vendors_social_only']} vendors")
        print(f"\n📱 SOCIAL MEDIA REACH")
        print(f"   Have Instagram:      {self.insights.get('vendors_with_instagram', 0)} vendors")
        print(f"   Have Facebook:       {self.insights.get('vendors_with_facebook', 0)} vendors")
        print(f"   10K+ followers:      {self.insights.get('vendors_10k_plus', 0)} vendors")
        print(f"   50K+ followers:      {self.insights.get('vendors_50k_plus', 0)} vendors")
        ig_total = self.insights.get('total_instagram_followers', 0)
        if ig_total > 0:
            print(f"   Total IG followers:  {ig_total:,}")

        print(f"\n💎 OPPORTUNITY GAPS")
        print(f"   No Real Website: {self.insights['vendors_without_website']} vendors "
              f"({self.insights['vendors_without_website']/total*100:.1f}%)")
        print(f"   High-Rated (4.5+): {self.insights['high_rated_vendors']} vendors")
        print(f"   Hot Leads (No-Brainers): {self.insights['no_brainer_count']} vendors")

        print(f"\n🌍 TOP CITIES")
        top_cities = sorted(
            self.insights['cities'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        for city, count in top_cities:
            print(f"   {city}: {count} vendors")

        print(f"\n📋 TOP CATEGORIES")
        top_categories = sorted(
            self.insights['categories'].items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        for category, count in top_categories:
            print(f"   {category}: {count} vendors")

        print("\n" + "="*60)


def main():
    """Main execution function."""
    # Configuration
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1FiyUPo9ZJEDH13gKVoAC00ko1Vx9U9rdSSq1z2Jwa5U/edit?gid=0#gid=0"
    CREDENTIALS_FILE = "config/google_credentials.json"

    # Initialize analyzer
    analyzer = VendorOpportunityAnalyzer(SHEET_URL, CREDENTIALS_FILE)

    # Load data
    analyzer.load_data()

    # Segment vendors
    analyzer.segment_vendors()

    # Generate insights
    analyzer.generate_insights_summary()

    # Print report
    analyzer.print_insights_report()

    # Export sales lists
    analyzer.export_sales_lists()

    print("\n✅ Analysis complete!")


if __name__ == "__main__":
    main()
