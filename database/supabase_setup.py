"""
Supabase Database Setup for Wedding Vendor Platform
Sets up tables and relationships for itsmy.wedding platform.
"""

from supabase import create_client, Client
import os
from typing import Dict, List
from datetime import datetime


class SupabaseVendorDB:
    """
    Supabase database manager for wedding vendor platform.

    Tables:
    - vendors: Main vendor information
    - vendor_analytics: Opportunity scores and tiers
    - vendor_interactions: Sales interactions log
    - vendor_reviews: Review data for sentiment analysis
    - categories: Vendor categories
    - cities: Kerala cities
    """

    def __init__(self, supabase_url: str, supabase_key: str):
        """Initialize Supabase client."""
        self.client: Client = create_client(supabase_url, supabase_key)
        self.url = supabase_url
        self.key = supabase_key

    def create_schema(self):
        """
        Create database schema.

        Note: Execute these SQL commands in Supabase SQL Editor:
        """
        sql_schema = """
-- ============================================
-- WEDDING VENDOR PLATFORM DATABASE SCHEMA
-- ============================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- 1. VENDORS TABLE (Main table)
-- ============================================
CREATE TABLE IF NOT EXISTS vendors (
    -- Identifiers
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    google_place_id VARCHAR UNIQUE,
    name VARCHAR NOT NULL,

    -- Contact Information
    phone VARCHAR,
    email VARCHAR,
    website VARCHAR,

    -- Social Media
    instagram_handle VARCHAR,
    facebook_url VARCHAR,

    -- Business Details
    category VARCHAR,
    subcategories TEXT[],
    services_offered TEXT[],

    -- Location
    address TEXT,
    city VARCHAR,
    state VARCHAR DEFAULT 'Kerala',
    pincode VARCHAR,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),

    -- Reputation Metrics
    google_rating DECIMAL(2, 1),
    google_reviews_count INTEGER,
    quality_score INTEGER,

    -- Business Intelligence
    years_in_business INTEGER,
    price_range VARCHAR, -- budget/mid/premium/luxury

    -- Metadata
    google_maps_url TEXT,
    search_query VARCHAR,
    first_scraped_at TIMESTAMP DEFAULT NOW(),
    last_updated_at TIMESTAMP DEFAULT NOW(),

    -- Indexes
    created_at TIMESTAMP DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_vendors_city ON vendors(city);
CREATE INDEX idx_vendors_category ON vendors(category);
CREATE INDEX idx_vendors_rating ON vendors(google_rating);
CREATE INDEX idx_vendors_google_place_id ON vendors(google_place_id);

-- ============================================
-- 2. VENDOR ANALYTICS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS vendor_analytics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_id UUID REFERENCES vendors(id) ON DELETE CASCADE,

    -- Segmentation
    tier VARCHAR, -- 'Tier 1 - Premium', 'Tier 2 - Growth', 'Tier 3 - Entry'
    opportunity_score INTEGER, -- 0-100
    estimated_ltv INTEGER, -- Estimated lifetime value in rupees

    -- Digital Maturity
    has_website BOOLEAN,
    has_social_media BOOLEAN,
    digital_maturity_level VARCHAR, -- none/basic/intermediate/advanced

    -- Opportunity Flags
    website_opportunity BOOLEAN DEFAULT FALSE,
    profile_opportunity BOOLEAN DEFAULT FALSE,
    marketing_opportunity BOOLEAN DEFAULT FALSE,
    lead_gen_opportunity BOOLEAN DEFAULT FALSE,

    -- Sales Intelligence
    recommended_pitch TEXT,
    expected_conversion_rate DECIMAL(4, 2), -- Percentage
    priority_score INTEGER, -- 1-10 (10 = highest)

    -- Tracking
    calculated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(vendor_id)
);

CREATE INDEX idx_analytics_tier ON vendor_analytics(tier);
CREATE INDEX idx_analytics_opportunity_score ON vendor_analytics(opportunity_score);

-- ============================================
-- 3. VENDOR INTERACTIONS TABLE (Sales CRM)
-- ============================================
CREATE TABLE IF NOT EXISTS vendor_interactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_id UUID REFERENCES vendors(id) ON DELETE CASCADE,

    -- Interaction Details
    interaction_type VARCHAR, -- call/email/meeting/whatsapp
    interaction_date TIMESTAMP DEFAULT NOW(),
    outcome VARCHAR, -- interested/not_interested/callback/converted/rejected

    -- Sales Info
    sales_rep VARCHAR,
    product_pitched VARCHAR, -- website/profile/marketing/leads
    pricing_discussed VARCHAR,

    -- Follow-up
    next_action VARCHAR,
    next_action_date DATE,
    notes TEXT,

    -- Conversion Tracking
    converted BOOLEAN DEFAULT FALSE,
    conversion_date TIMESTAMP,
    conversion_value INTEGER, -- Revenue in rupees

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_interactions_vendor ON vendor_interactions(vendor_id);
CREATE INDEX idx_interactions_date ON vendor_interactions(interaction_date);
CREATE INDEX idx_interactions_outcome ON vendor_interactions(outcome);

-- ============================================
-- 4. VENDOR ONBOARDING STATUS TABLE
-- ============================================
CREATE TABLE IF NOT EXISTS vendor_onboarding (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_id UUID REFERENCES vendors(id) ON DELETE CASCADE,

    -- Onboarding Status
    status VARCHAR, -- contacted/interested/onboarding/active/churned
    onboarding_date DATE,
    activation_date DATE,

    -- Subscription Details
    subscription_tier VARCHAR, -- free/basic/premium/enterprise
    monthly_revenue INTEGER,
    subscription_start_date DATE,
    subscription_end_date DATE,

    -- Engagement Metrics
    last_login TIMESTAMP,
    profile_completion_percentage INTEGER,
    leads_received_30d INTEGER,
    leads_converted_30d INTEGER,

    -- Churn Risk
    churn_risk_score INTEGER, -- 0-100 (100 = high risk)

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),

    UNIQUE(vendor_id)
);

CREATE INDEX idx_onboarding_status ON vendor_onboarding(status);
CREATE INDEX idx_onboarding_subscription ON vendor_onboarding(subscription_tier);

-- ============================================
-- 5. VENDOR REVIEWS TABLE (Sentiment Analysis)
-- ============================================
CREATE TABLE IF NOT EXISTS vendor_reviews (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    vendor_id UUID REFERENCES vendors(id) ON DELETE CASCADE,

    -- Review Data
    reviewer_name VARCHAR,
    rating INTEGER, -- 1-5
    review_text TEXT,
    review_date DATE,

    -- Sentiment Analysis
    sentiment_score DECIMAL(3, 2), -- -1 to 1
    sentiment_label VARCHAR, -- negative/neutral/positive
    keywords TEXT[],
    mentioned_services TEXT[],

    -- Source
    source VARCHAR, -- google/facebook/itsmy.wedding

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_reviews_vendor ON vendor_reviews(vendor_id);
CREATE INDEX idx_reviews_sentiment ON vendor_reviews(sentiment_score);

-- ============================================
-- 6. CATEGORIES TABLE (Reference)
-- ============================================
CREATE TABLE IF NOT EXISTS categories (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR UNIQUE NOT NULL,
    display_name VARCHAR,
    avg_ticket_size INTEGER, -- Average transaction value
    description TEXT,
    icon VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert default categories
INSERT INTO categories (name, display_name, avg_ticket_size) VALUES
    ('wedding_planner', 'Wedding Planner', 500000),
    ('event_planner', 'Event Management', 400000),
    ('venue', 'Wedding Venue', 300000),
    ('photographer', 'Photographer', 150000),
    ('videographer', 'Videographer', 150000),
    ('caterer', 'Caterer', 100000),
    ('decorator', 'Decorator', 80000),
    ('makeup_artist', 'Makeup Artist', 80000),
    ('mehendi_artist', 'Mehendi Artist', 60000),
    ('dj', 'DJ Service', 50000),
    ('live_band', 'Live Band', 100000),
    ('choreographer', 'Wedding Choreographer', 50000),
    ('bridal_wear', 'Bridal Wear Designer', 200000),
    ('jewelry', 'Jewelry Designer', 500000),
    ('florist', 'Florist', 100000),
    ('lighting', 'Lighting Specialist', 80000)
ON CONFLICT (name) DO NOTHING;

-- ============================================
-- 7. CITIES TABLE (Reference)
-- ============================================
CREATE TABLE IF NOT EXISTS cities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR UNIQUE NOT NULL,
    tier INTEGER, -- 1 = Tier 1 (Kochi, TVM), 2 = Tier 2, 3 = Tier 3
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Insert Kerala cities
INSERT INTO cities (name, tier) VALUES
    ('Kochi', 1),
    ('Thiruvananthapuram', 1),
    ('Kozhikode', 1),
    ('Thrissur', 1),
    ('Kollam', 2),
    ('Alappuzha', 2),
    ('Palakkad', 2),
    ('Kannur', 2),
    ('Malappuram', 2),
    ('Kasaragod', 3),
    ('Pathanamthitta', 3),
    ('Idukki', 3),
    ('Wayanad', 3)
ON CONFLICT (name) DO NOTHING;

-- ============================================
-- 8. VIEWS FOR ANALYTICS
-- ============================================

-- Hot Leads View
CREATE OR REPLACE VIEW hot_leads AS
SELECT
    v.id,
    v.name,
    v.category,
    v.city,
    v.phone,
    v.google_rating,
    v.google_reviews_count,
    v.website,
    va.opportunity_score,
    va.estimated_ltv,
    va.tier,
    va.recommended_pitch
FROM vendors v
JOIN vendor_analytics va ON v.id = va.vendor_id
WHERE
    v.google_rating >= 4.7
    AND v.google_reviews_count >= 150
    AND (v.website IS NULL OR v.website = '')
    AND v.phone IS NOT NULL
ORDER BY va.opportunity_score DESC;

-- Tier Summary View
CREATE OR REPLACE VIEW tier_summary AS
SELECT
    va.tier,
    COUNT(*) as vendor_count,
    SUM(va.estimated_ltv) as total_opportunity_value,
    AVG(v.google_rating) as avg_rating,
    AVG(v.google_reviews_count) as avg_reviews
FROM vendors v
JOIN vendor_analytics va ON v.id = va.vendor_id
GROUP BY va.tier;

-- Category Performance View
CREATE OR REPLACE VIEW category_performance AS
SELECT
    v.category,
    COUNT(*) as vendor_count,
    AVG(v.google_rating) as avg_rating,
    AVG(v.google_reviews_count) as avg_reviews,
    COUNT(*) FILTER (WHERE v.website IS NULL OR v.website = '') as without_website,
    AVG(va.opportunity_score) as avg_opportunity_score
FROM vendors v
JOIN vendor_analytics va ON v.id = va.vendor_id
GROUP BY v.category
ORDER BY vendor_count DESC;

-- ============================================
-- 9. ROW LEVEL SECURITY (RLS)
-- ============================================

-- Enable RLS on all tables
ALTER TABLE vendors ENABLE ROW LEVEL SECURITY;
ALTER TABLE vendor_analytics ENABLE ROW LEVEL SECURITY;
ALTER TABLE vendor_interactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE vendor_onboarding ENABLE ROW LEVEL SECURITY;
ALTER TABLE vendor_reviews ENABLE ROW LEVEL SECURITY;

-- Create policies (adjust based on your auth setup)
-- Example: Allow authenticated users to read all vendors
CREATE POLICY "Allow read access to vendors" ON vendors
    FOR SELECT
    TO authenticated
    USING (true);

-- ============================================
-- 10. FUNCTIONS
-- ============================================

-- Function to update vendor analytics
CREATE OR REPLACE FUNCTION update_vendor_analytics()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE vendor_analytics
    SET
        has_website = (NEW.website IS NOT NULL AND NEW.website != ''),
        has_social_media = (NEW.instagram_handle IS NOT NULL OR NEW.facebook_url IS NOT NULL)
    WHERE vendor_id = NEW.id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-update analytics
CREATE TRIGGER vendors_update_analytics
AFTER UPDATE ON vendors
FOR EACH ROW
EXECUTE FUNCTION update_vendor_analytics();

-- ============================================
-- SCHEMA CREATION COMPLETE
-- ============================================
"""

        print("="*60)
        print("SUPABASE SCHEMA SQL")
        print("="*60)
        print("\nCopy the SQL below and execute in Supabase SQL Editor:\n")
        print(sql_schema)
        print("\n" + "="*60)

        return sql_schema

    def upload_vendors(self, vendors_df):
        """
        Upload vendor data to Supabase.

        Args:
            vendors_df: Pandas DataFrame with vendor data
        """
        print(f"\n📤 Uploading {len(vendors_df)} vendors to Supabase...")

        # Convert DataFrame to list of dicts
        vendors_data = vendors_df.to_dict('records')

        batch_size = 100
        success_count = 0

        for i in range(0, len(vendors_data), batch_size):
            batch = vendors_data[i:i+batch_size]

            # Clean data for Supabase
            cleaned_batch = []
            for vendor in batch:
                cleaned_vendor = {
                    'name': vendor.get('name'),
                    'google_place_id': vendor.get('url', '').split('/')[-1] if vendor.get('url') else None,
                    'category': vendor.get('category'),
                    'phone': str(vendor.get('phone', '')) if vendor.get('phone') else None,
                    'website': vendor.get('website') if vendor.get('website') else None,
                    'address': vendor.get('address'),
                    'city': vendor.get('city'),
                    'pincode': vendor.get('pincode'),
                    'google_rating': float(vendor.get('rating')) if vendor.get('rating') else None,
                    'google_reviews_count': int(vendor.get('reviews_count')) if vendor.get('reviews_count') else None,
                    'quality_score': int(vendor.get('quality_score')) if vendor.get('quality_score') else None,
                    'google_maps_url': vendor.get('url'),
                    'search_query': vendor.get('search_query'),
                }
                cleaned_batch.append(cleaned_vendor)

            try:
                # Upsert vendors (update if exists, insert if not)
                result = self.client.table('vendors').upsert(
                    cleaned_batch,
                    on_conflict='google_place_id'
                ).execute()

                success_count += len(cleaned_batch)
                print(f"  ✅ Uploaded batch {i//batch_size + 1}: {success_count}/{len(vendors_data)}")

            except Exception as e:
                print(f"  ❌ Error uploading batch {i//batch_size + 1}: {str(e)}")

        print(f"\n✅ Successfully uploaded {success_count} vendors to Supabase!")

    def upload_analytics(self, analytics_df):
        """Upload vendor analytics data."""
        print(f"\n📤 Uploading {len(analytics_df)} analytics records to Supabase...")

        analytics_data = []
        for _, row in analytics_df.iterrows():
            # Get vendor ID from database
            vendor_result = self.client.table('vendors').select('id').eq(
                'name', row['name']
            ).execute()

            if vendor_result.data:
                vendor_id = vendor_result.data[0]['id']

                analytics_data.append({
                    'vendor_id': vendor_id,
                    'tier': row.get('tier'),
                    'opportunity_score': int(row.get('opportunity_score', 0)),
                    'estimated_ltv': int(row.get('estimated_ltv', 0)),
                    'has_website': bool(row.get('website')),
                    'recommended_pitch': row.get('recommended_pitch'),
                })

        # Batch upload
        if analytics_data:
            try:
                result = self.client.table('vendor_analytics').upsert(
                    analytics_data,
                    on_conflict='vendor_id'
                ).execute()
                print(f"✅ Uploaded {len(analytics_data)} analytics records!")
            except Exception as e:
                print(f"❌ Error uploading analytics: {str(e)}")

    def get_hot_leads(self, limit: int = 100):
        """Get hot leads from database."""
        result = self.client.table('hot_leads').select('*').limit(limit).execute()
        return result.data

    def get_tier_summary(self):
        """Get tier summary statistics."""
        result = self.client.table('tier_summary').select('*').execute()
        return result.data


# Example usage
def setup_supabase_example():
    """Example of setting up Supabase database."""

    # Set your Supabase credentials
    SUPABASE_URL = os.getenv('SUPABASE_URL', 'https://your-project.supabase.co')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY', 'your-anon-key')

    # Initialize
    db = SupabaseVendorDB(SUPABASE_URL, SUPABASE_KEY)

    # Print schema SQL
    db.create_schema()

    print("\n" + "="*60)
    print("SETUP INSTRUCTIONS")
    print("="*60)
    print("""
1. Create a Supabase project at https://supabase.com

2. Go to SQL Editor in Supabase dashboard

3. Copy and execute the SQL schema printed above

4. Get your credentials from Settings > API:
   - Project URL (SUPABASE_URL)
   - anon/public key (SUPABASE_KEY)

5. Set environment variables:
   export SUPABASE_URL="https://your-project.supabase.co"
   export SUPABASE_KEY="your-anon-key"

6. Run migration script:
   python database/migrate_to_supabase.py
    """)


if __name__ == "__main__":
    setup_supabase_example()
