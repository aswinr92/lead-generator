"""
Vendor Insights Dashboard
Interactive Streamlit dashboard for vendor opportunity analysis.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from analytics.vendor_opportunity_analyzer import VendorOpportunityAnalyzer


# Page configuration
st.set_page_config(
    page_title="Vendor Insights Dashboard - itsmy.wedding",
    page_icon="💍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #FF1493;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
    }
    </style>
""", unsafe_allow_html=True)


def _get_credentials_path() -> str:
    """
    Return a path to the Google service account JSON.

    Priority:
      1. Local file (config/google_credentials.json) — development
      2. Streamlit secrets [gcp_service_account] — Streamlit Community Cloud

    This lets the same codebase run locally and in the cloud without changes.
    """
    local_path = "config/google_credentials.json"
    if os.path.exists(local_path):
        return local_path

    # Running on Streamlit Cloud — reconstruct credentials from st.secrets
    # if "gcp_service_account" in st.secrets:
    #     import json
    #     import tempfile
    #     creds_dict = dict(st.secrets["gcp_service_account"])
    #     tmp = tempfile.NamedTemporaryFile(
    #         mode="w", suffix=".json", delete=False, dir=tempfile.gettempdir()
    #     )
    #     json.dump(creds_dict, tmp)
    #     tmp.close()
    #     return tmp.name
    
    json_env = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if json_env:
        import json, tempfile
        creds_dict = json.loads(json_env)

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
        json.dump(creds_dict, tmp)
        tmp.close()
        return tmp.name

    raise FileNotFoundError(
        "Google credentials not found.\n"
        "• Locally: add config/google_credentials.json\n"
        "• Streamlit Cloud: add [gcp_service_account] in App Settings → Secrets"
    )


SHEET_URL = "https://docs.google.com/spreadsheets/d/1FiyUPo9ZJEDH13gKVoAC00ko1Vx9U9rdSSq1z2Jwa5U/edit?gid=0#gid=0"

# CSV cache paths — tried in order; first writable location wins
_CSV_CACHE_PATHS = [
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "vendors_cache.csv"),
    "/tmp/vendors_cache.csv",
]
_CSV_MAX_AGE_HOURS = 24


def _find_fresh_csv() -> str | None:
    """Return path to a CSV cache that exists and is < 24 hours old, or None."""
    import time
    for path in _CSV_CACHE_PATHS:
        if os.path.exists(path):
            age_hours = (time.time() - os.path.getmtime(path)) / 3600
            if age_hours < _CSV_MAX_AGE_HOURS:
                return path
    return None


def _save_csv(df: pd.DataFrame):
    """Save DataFrame to CSV cache (first writable path wins)."""
    for path in _CSV_CACHE_PATHS:
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_csv(path, index=False)
            return path
        except Exception:
            continue
    return None


@st.cache_data(ttl=1800, show_spinner=False)  # 30-minute in-memory cache
def load_vendor_data(force_sheets: bool = False) -> tuple:
    """
    Load vendor data and run analysis.

    Fast path  — reads from CSV cache  (~2 s, used if cache < 24 h old)
    Slow path  — reads from Google Sheets (~60 s, saves to CSV afterwards)

    Returns (df, insights, opportunities, source_label)
    """
    # ── Fast path: CSV cache ──────────────────────────────────────────
    if not force_sheets:
        csv_path = _find_fresh_csv()
        if csv_path:
            try:
                df_raw = pd.read_csv(csv_path)
                age_h = (os.path.getmtime(csv_path))
                import time
                age_h = (time.time() - os.path.getmtime(csv_path)) / 3600
                analyzer = VendorOpportunityAnalyzer(SHEET_URL, _get_credentials_path())
                analyzer.df = df_raw          # inject CSV data — skip Sheets load
                analyzer.segment_vendors()
                return (
                    analyzer.df,
                    analyzer.generate_insights_summary(),
                    analyzer.identify_cross_sell_opportunities(),
                    f"CSV cache ({age_h:.0f}h old)",
                )
            except Exception:
                pass  # corrupted CSV — fall through to Sheets

    # ── Slow path: Google Sheets ──────────────────────────────────────
    analyzer = VendorOpportunityAnalyzer(SHEET_URL, _get_credentials_path())
    analyzer.load_data()
    analyzer.segment_vendors()

    saved = _save_csv(analyzer.df)
    source = "Google Sheets (live)" + (f" → cached to {os.path.basename(saved)}" if saved else "")

    return (
        analyzer.df,
        analyzer.generate_insights_summary(),
        analyzer.identify_cross_sell_opportunities(),
        source,
    )


# Header
st.markdown('<div class="main-header">💍 Wedding Vendor Intelligence Dashboard</div>',
            unsafe_allow_html=True)

# ── Lazy data loading ─────────────────────────────────────────────────────
# The app starts instantly and serves the health-check immediately.
# Data is only loaded when the user requests it (avoids 503 timeouts on cloud).

if "vendor_data" not in st.session_state:
    st.markdown("---")
    col_l, col_c, col_r = st.columns([1, 2, 1])
    with col_c:
        st.markdown("### 📊 Dashboard Ready")

        csv_path = _find_fresh_csv()
        if csv_path:
            import time as _t
            age_h = (_t.time() - os.path.getmtime(csv_path)) / 3600
            st.success(
                f"CSV cache available ({age_h:.0f}h old) — "
                f"loading will take **~2 seconds**."
            )
        else:
            st.info(
                "No local cache found. First load fetches from Google Sheets "
                "and takes **~60 seconds**. Subsequent loads use a CSV cache (~2 s)."
            )

        st.markdown(" ")
        if st.button("▶  Load Vendor Data", type="primary", use_container_width=True):
            with st.spinner("Loading vendor data…"):
                try:
                    data = load_vendor_data()
                    st.session_state["vendor_data"] = data
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    st.exception(e)
    st.stop()

# Unpack session state (persists for the session; no re-fetch on page interaction)
if "vendor_data" not in st.session_state:
    st.error("Session state lost — please refresh the page.")
    st.stop()

df, insights, opportunities, _data_source = st.session_state["vendor_data"]

# Sidebar filters
st.sidebar.header("🔍 Filters")

selected_tier = st.sidebar.multiselect(
    "Tier",
    options=df['tier'].unique(),
    default=df['tier'].unique()
)

selected_cities = st.sidebar.multiselect(
    "City",
    options=sorted(df['city'].unique()),
    default=[]
)

selected_categories = st.sidebar.multiselect(
    "Category",
    options=sorted(df['category'].unique()),
    default=[]
)

rating_range = st.sidebar.slider(
    "Rating Range",
    min_value=0.0,
    max_value=5.0,
    value=(0.0, 5.0),
    step=0.1
)

st.sidebar.markdown("---")
st.sidebar.caption(f"Data source: {_data_source}")
if st.sidebar.button("🔄 Refresh from Sheets", help="Re-fetch live data from Google Sheets (~60s)"):
    st.cache_data.clear()
    if "vendor_data" in st.session_state:
        del st.session_state["vendor_data"]
    st.rerun()

# Apply filters
filtered_df = df.copy()
filtered_df = filtered_df[filtered_df['tier'].isin(selected_tier)]

if selected_cities:
    filtered_df = filtered_df[filtered_df['city'].isin(selected_cities)]

if selected_categories:
    filtered_df = filtered_df[filtered_df['category'].isin(selected_categories)]

filtered_df = filtered_df[
    (filtered_df['rating'] >= rating_range[0]) &
    (filtered_df['rating'] <= rating_range[1])
]

# Main tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Overview",
    "🎯 Segmentation",
    "💰 Opportunities",
    "📍 Geographic Analysis",
    "📋 Vendor Lists"
])

# TAB 1: OVERVIEW
with tab1:
    st.header("Business Intelligence Overview")

    # Key metrics
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "Total Vendors",
            f"{len(filtered_df):,}",
            delta=f"{len(filtered_df) - len(df)}" if selected_cities or selected_categories else None
        )

    with col2:
        total_value = filtered_df['estimated_ltv'].sum() / 10000000
        st.metric(
            "Total Opportunity Value",
            f"₹{total_value:.2f} Cr"
        )

    with col3:
        no_website = len(filtered_df[
            filtered_df['website'].isna() | (filtered_df['website'] == '')
        ])
        pct = (no_website / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
        st.metric(
            "Without Website",
            f"{no_website:,}",
            delta=f"{pct:.1f}%"
        )

    with col4:
        high_rated = len(filtered_df[filtered_df['rating'] >= 4.5])
        st.metric(
            "High Rated (4.5+)",
            f"{high_rated:,}"
        )

    with col5:
        if 'instagram_followers' in filtered_df.columns or 'facebook_followers' in filtered_df.columns:
            ig_f = pd.to_numeric(filtered_df.get('instagram_followers', 0), errors='coerce').fillna(0)
            fb_f = pd.to_numeric(filtered_df.get('facebook_followers', 0), errors='coerce').fillna(0)
            power_vendors = (ig_f.combine(fb_f, max) >= 10000).sum()
            st.metric("Social Power (10K+)", f"{power_vendors:,}")
        else:
            has_any_social = len(filtered_df[
                (filtered_df.get('instagram', pd.Series([''] * len(filtered_df))).fillna('') != '') |
                (filtered_df.get('facebook', pd.Series([''] * len(filtered_df))).fillna('') != '')
            ])
            st.metric("Have Social Media", f"{has_any_social:,}")

    st.divider()

    # Charts row 1
    col1, col2 = st.columns(2)

    with col1:
        # Rating distribution
        st.subheader("Rating Distribution")
        rating_bins = pd.cut(
            filtered_df['rating'],
            bins=[0, 3.0, 3.5, 4.0, 4.5, 5.0],
            labels=['<3.0', '3.0-3.5', '3.5-4.0', '4.0-4.5', '4.5+']
        )
        rating_counts = rating_bins.value_counts().sort_index()

        fig = px.bar(
            x=rating_counts.index,
            y=rating_counts.values,
            labels={'x': 'Rating Range', 'y': 'Number of Vendors'},
            color=rating_counts.values,
            color_continuous_scale='Blues'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Reviews distribution
        st.subheader("Reviews Distribution")
        review_bins = pd.cut(
            filtered_df['reviews_count'],
            bins=[0, 20, 50, 100, 200, float('inf')],
            labels=['<20', '20-50', '50-100', '100-200', '200+']
        )
        review_counts = review_bins.value_counts().sort_index()

        fig = px.bar(
            x=review_counts.index,
            y=review_counts.values,
            labels={'x': 'Review Count Range', 'y': 'Number of Vendors'},
            color=review_counts.values,
            color_continuous_scale='Greens'
        )
        st.plotly_chart(fig, use_container_width=True)

    # Charts row 2 — Social media follower distribution (shown when data available)
    ig_followers_col = pd.to_numeric(filtered_df.get('instagram_followers', pd.Series()), errors='coerce').dropna()
    fb_followers_col = pd.to_numeric(filtered_df.get('facebook_followers', pd.Series()), errors='coerce').dropna()

    if len(ig_followers_col) > 0 or len(fb_followers_col) > 0:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Instagram Follower Distribution")
            follower_bins = pd.cut(
                ig_followers_col,
                bins=[0, 1000, 5000, 10000, 50000, float('inf')],
                labels=['<1K', '1K–5K', '5K–10K', '10K–50K', '50K+']
            )
            follower_counts = follower_bins.value_counts().sort_index()
            fig = px.bar(
                x=follower_counts.index,
                y=follower_counts.values,
                labels={'x': 'Follower Range', 'y': 'Number of Vendors'},
                color=follower_counts.values,
                color_continuous_scale='Purples'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Social Media Source")
            if 'instagram_found_via' in filtered_df.columns:
                ig_sources = filtered_df[filtered_df['instagram'].fillna('') != '']['instagram_found_via'].value_counts()
                source_labels = {
                    'listed': 'Listed on Google Maps',
                    'website_link': 'Found on website',
                    'search': 'Found via search (verify)'
                }
                display_labels = [source_labels.get(s, s) for s in ig_sources.index]
                fig = go.Figure(data=[go.Pie(
                    labels=display_labels,
                    values=ig_sources.values,
                    hole=0.4,
                    marker=dict(colors=['#00D26A', '#2196F3', '#FFB703'])
                )])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Run backfill_find_socials.py to populate social media source data")

    col1, col2 = st.columns(2)

    with col1:
        # Top categories
        st.subheader("Top 10 Categories")
        top_categories = filtered_df['category'].value_counts().head(10)

        fig = px.pie(
            values=top_categories.values,
            names=top_categories.index,
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Digital presence (3-way breakdown if new columns exist)
        st.subheader("Digital Presence Breakdown")
        if 'digital_presence' in filtered_df.columns:
            web_count = (filtered_df['digital_presence'] == 'full_website').sum()
            social_count = (filtered_df['digital_presence'] == 'social_only').sum()
            none_count = (filtered_df['digital_presence'] == 'none').sum()
            labels = ['Real Website', 'Social Only\n(Instagram/Facebook)', 'No Presence']
            values = [web_count, social_count, none_count]
            colors = ['#00D26A', '#FFB703', '#FF4B4B']
        else:
            has_website = len(filtered_df[
                filtered_df['website'].notna() & (filtered_df['website'] != '')
            ])
            no_website = len(filtered_df) - has_website
            labels = ['Has Website', 'No Website']
            values = [has_website, no_website]
            colors = ['#00D26A', '#FF4B4B']

        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=0.4,
            marker=dict(colors=colors)
        )])
        st.plotly_chart(fig, use_container_width=True)

# TAB 2: SEGMENTATION
with tab2:
    st.header("Vendor Segmentation Analysis")

    # Tier distribution
    col1, col2, col3 = st.columns(3)

    tier1_count = len(filtered_df[filtered_df['tier'] == 'Tier 1 - Premium'])
    tier2_count = len(filtered_df[filtered_df['tier'] == 'Tier 2 - Growth'])
    tier3_count = len(filtered_df[filtered_df['tier'] == 'Tier 3 - Entry'])

    tier1_value = filtered_df[
        filtered_df['tier'] == 'Tier 1 - Premium'
    ]['estimated_ltv'].sum() / 10000000

    tier2_value = filtered_df[
        filtered_df['tier'] == 'Tier 2 - Growth'
    ]['estimated_ltv'].sum() / 10000000

    tier3_value = filtered_df[
        filtered_df['tier'] == 'Tier 3 - Entry'
    ]['estimated_ltv'].sum() / 10000000

    with col1:
        st.markdown("### 🏆 Tier 1 - Premium")
        st.metric("Vendors", f"{tier1_count:,}")
        st.metric("Opportunity Value", f"₹{tier1_value:.2f} Cr")
        st.markdown("""
        **Profile:**
        - Rating: 4.5+
        - Reviews: 100+
        - No website
        - Phone available

        **Expected Conversion:** 25-35%
        """)

    with col2:
        st.markdown("### 📈 Tier 2 - Growth")
        st.metric("Vendors", f"{tier2_count:,}")
        st.metric("Opportunity Value", f"₹{tier2_value:.2f} Cr")
        st.markdown("""
        **Profile:**
        - Rating: 4.0-4.5
        - Reviews: 20-100
        - May have basic website

        **Expected Conversion:** 15-25%
        """)

    with col3:
        st.markdown("### 🌱 Tier 3 - Entry")
        st.metric("Vendors", f"{tier3_count:,}")
        st.metric("Opportunity Value", f"₹{tier3_value:.2f} Cr")
        st.markdown("""
        **Profile:**
        - Rating: <4.0
        - Reviews: <20
        - Usually no website

        **Expected Conversion:** 5-15%
        """)

    st.divider()

    # Opportunity score distribution
    st.subheader("Opportunity Score Distribution")
    fig = px.histogram(
        filtered_df,
        x='opportunity_score',
        nbins=20,
        color='tier',
        labels={'opportunity_score': 'Opportunity Score', 'count': 'Number of Vendors'}
    )
    st.plotly_chart(fig, use_container_width=True)

    # Category breakdown by tier
    st.subheader("Category Breakdown by Tier")
    tier_category = pd.crosstab(filtered_df['tier'], filtered_df['category'])
    fig = px.bar(
        tier_category,
        barmode='group',
        labels={'value': 'Count', 'variable': 'Category'}
    )
    st.plotly_chart(fig, use_container_width=True)

# TAB 3: OPPORTUNITIES
with tab3:
    st.header("💰 Cross-Selling Opportunities")

    # Opportunity cards
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("1. Website Creation Service")
        website_opp = opportunities['website_creation']
        st.metric("Target Vendors", f"{len(website_opp):,}")

        potential_revenue = len(website_opp) * 20000 * 0.1  # 10% conversion @ ₹20K
        st.metric("Potential Revenue (10% conv)", f"₹{potential_revenue/100000:.2f} L")

        st.markdown("""
        **Target:** Rating 4.0+, Reviews 50+, No Website

        **Pricing:**
        - Basic: ₹10,000
        - Standard: ₹25,000
        - Premium: ₹50,000
        """)

        with st.expander("View Top 10 Targets"):
            st.dataframe(
                website_opp[['name', 'category', 'city', 'rating', 'reviews_count', 'phone']].head(10),
                use_container_width=True
            )

    with col2:
        st.subheader("2. Premium Profile on itsmy.wedding")
        profile_opp = opportunities['premium_profile']
        st.metric("Target Vendors", f"{len(profile_opp):,}")

        monthly_revenue = len(profile_opp) * 500 * 0.2  # 20% conversion @ ₹500/mo
        st.metric("Potential MRR (20% conv)", f"₹{monthly_revenue/100000:.2f} L")

        st.markdown("""
        **Target:** Rating 4.5+, No Website

        **Pricing:**
        - Free: Basic profile
        - Premium: ₹500/month
        - Enterprise: ₹2,000/month
        """)

        with st.expander("View Top 10 Targets"):
            st.dataframe(
                profile_opp[['name', 'category', 'city', 'rating', 'reviews_count', 'phone']].head(10),
                use_container_width=True
            )

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("3. Digital Marketing Services")
        marketing_opp = opportunities['digital_marketing']
        st.metric("Target Vendors", f"{len(marketing_opp):,}")

        monthly_revenue = len(marketing_opp) * 5000 * 0.15  # 15% conversion @ ₹5K/mo
        st.metric("Potential MRR (15% conv)", f"₹{monthly_revenue/100000:.2f} L")

        st.markdown("""
        **Target:** Rating 4.5+, Reviews <50, No Website

        **Services:**
        - Google Maps optimization
        - Social media setup
        - WhatsApp Business catalog
        - GMB management

        **Pricing:** ₹5,000/month
        """)

        with st.expander("View Top 10 Targets"):
            st.dataframe(
                marketing_opp[['name', 'category', 'city', 'rating', 'reviews_count', 'phone']].head(10),
                use_container_width=True
            )

    with col2:
        st.subheader("4. Lead Generation Platform")
        lead_opp = opportunities['lead_generation']
        st.metric("Target Vendors", f"{len(lead_opp):,}")

        monthly_revenue = len(lead_opp) * 10000 * 0.3  # 30% conversion @ ₹10K/mo
        st.metric("Potential MRR (30% conv)", f"₹{monthly_revenue/100000:.2f} L")

        st.markdown("""
        **Target:** Rating 4.0+, Reviews 100+, Premium Categories

        **Model:**
        - Pay-per-lead: ₹200-500
        - Subscription: ₹10,000/month
        - Commission: 2-5% of booking

        **Categories:** Caterer, Photographer, Videographer, Venue, Planner
        """)

        with st.expander("View Top 10 Targets"):
            st.dataframe(
                lead_opp[['name', 'category', 'city', 'rating', 'reviews_count', 'phone']].head(10),
                use_container_width=True
            )

    # Social → Website Upgrade opportunity
    if 'social_to_website' in opportunities:
        st.divider()
        col1, col2 = st.columns([1, 2])
        social_opp = opportunities['social_to_website']

        with col1:
            st.subheader("5. Social → Website Upgrade")
            st.metric("Target Vendors", f"{len(social_opp):,}")

            potential_revenue = len(social_opp) * 15000 * 0.20  # 20% conv @ ₹15K
            st.metric("Potential Revenue (20% conv)", f"₹{potential_revenue/100000:.2f} L")

            st.markdown("""
            **Target:** Has Instagram/Facebook but NO real website

            **Pitch:** "You're already on Instagram with [X] followers —
            let's turn that into a bookable website that converts visits into weddings!"

            **Pricing:**
            - Starter: ₹8,000 (template + IG integration)
            - Standard: ₹20,000 (custom design)
            - Premium: ₹40,000 (full brand + booking system)
            """)

        with col2:
            if len(social_opp) > 0:
                display_cols = ['name', 'category', 'city', 'rating', 'reviews_count',
                                'instagram', 'facebook', 'instagram_followers', 'facebook_followers']
                show_cols = [c for c in display_cols if c in social_opp.columns]
                st.dataframe(social_opp[show_cols].head(15), use_container_width=True)

    # Social Power Vendors (10K+ followers)
    if 'social_power_vendors' in opportunities and len(opportunities['social_power_vendors']) > 0:
        st.divider()
        power_df = opportunities['social_power_vendors']
        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader("⚡ Social Power Vendors")
            st.metric("10K+ Followers", f"{len(power_df):,}")

            ig_f = pd.to_numeric(power_df.get('instagram_followers', 0), errors='coerce').fillna(0)
            fb_f = pd.to_numeric(power_df.get('facebook_followers', 0), errors='coerce').fillna(0)
            total_reach = int(ig_f.sum() + fb_f.sum())
            st.metric("Total Combined Reach", f"{total_reach:,}")

            st.markdown("""
            **Why they matter:**
            Wedding vendors with 10K+ followers are *micro-influencers*
            in your market — they drive brand awareness and referrals.

            **Partnership opportunities:**
            - Co-branded content (featured vendor posts)
            - Affiliate / referral revenue share
            - Premium verified badge on itsmy.wedding
            - Brand ambassador programme
            - Sponsored listings / priority ranking

            **Pitch:** "Your 25,000 followers trust you — let's help you
            monetise that influence through itsmy.wedding."
            """)

        with col2:
            power_show_cols_base = [
                'name', 'category', 'city', 'rating',
                'instagram', 'instagram_followers',
                'facebook', 'facebook_followers',
                'website', 'phone'
            ]
            power_show_cols = [c for c in power_show_cols_base if c in power_df.columns]
            st.dataframe(power_df[power_show_cols].head(20), use_container_width=True)

    st.divider()

    # Hot leads (No-Brainers)
    st.subheader("🔥 HOT LEADS - No-Brainers")
    no_brainers = filtered_df[
        (filtered_df['rating'] >= 4.7) &
        (filtered_df['reviews_count'] >= 150) &
        (filtered_df['website'].isna() | (filtered_df['website'] == '')) &
        (filtered_df['phone'].notna() & (filtered_df['phone'] != ''))
    ].sort_values('reviews_count', ascending=False)

    st.metric("Ultra-Hot Leads", f"{len(no_brainers):,}")
    st.markdown("**These vendors need you NOW. Expected conversion: 40-50%**")

    if len(no_brainers) > 0:
        st.dataframe(
            no_brainers[['name', 'category', 'city', 'rating', 'reviews_count', 'phone', 'estimated_ltv']].head(20),
            use_container_width=True
        )
    else:
        st.info("No hot leads match current filters")

# TAB 4: GEOGRAPHIC ANALYSIS
with tab4:
    st.header("📍 Geographic Distribution")

    # City analysis
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Vendors by City")
        city_counts = filtered_df['city'].value_counts().head(15)

        fig = px.bar(
            x=city_counts.values,
            y=city_counts.index,
            orientation='h',
            labels={'x': 'Number of Vendors', 'y': 'City'},
            color=city_counts.values,
            color_continuous_scale='Viridis'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Opportunity Value by City")
        city_value = filtered_df.groupby('city')['estimated_ltv'].sum().sort_values(ascending=False).head(15)

        fig = px.bar(
            x=city_value.values / 10000000,
            y=city_value.index,
            orientation='h',
            labels={'x': 'Opportunity Value (₹ Cr)', 'y': 'City'},
            color=city_value.values,
            color_continuous_scale='Reds'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

    # City heatmap
    st.subheader("City Performance Heatmap")
    city_metrics = filtered_df.groupby('city').agg({
        'name': 'count',
        'rating': 'mean',
        'reviews_count': 'mean',
        'opportunity_score': 'mean'
    }).round(2)
    city_metrics.columns = ['Vendor Count', 'Avg Rating', 'Avg Reviews', 'Avg Opportunity Score']
    city_metrics = city_metrics.sort_values('Vendor Count', ascending=False).head(10)

    st.dataframe(city_metrics, use_container_width=True)

# TAB 5: VENDOR LISTS
with tab5:
    st.header("📋 Vendor Contact Lists")

    st.markdown("""
    Download prioritized vendor lists for your sales team.
    Filter the data using the sidebar, then download the segments you need.
    """)

    # Export options
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Export by Tier")

        for tier in ['Tier 1 - Premium', 'Tier 2 - Growth', 'Tier 3 - Entry']:
            tier_df = filtered_df[filtered_df['tier'] == tier].sort_values(
                'opportunity_score', ascending=False
            )

            base_export_cols = [
                'name', 'category', 'city', 'phone', 'rating', 'reviews_count',
                'website', 'instagram', 'facebook', 'digital_presence',
                'instagram_followers', 'facebook_followers',
                'opportunity_score', 'estimated_ltv', 'recommended_pitch'
            ]
            export_cols = [c for c in base_export_cols if c in filtered_df.columns]

            st.download_button(
                label=f"Download {tier} ({len(tier_df)} vendors)",
                data=tier_df[export_cols].to_csv(index=False),
                file_name=f"{tier.lower().replace(' ', '_')}.csv",
                mime="text/csv",
                key=f"download_{tier}"
            )

    with col2:
        st.subheader("Export by Opportunity")

        for opp_name, opp_label in [
            ('website_creation', 'Website Creation'),
            ('social_to_website', 'Social → Website'),
            ('premium_profile', 'Premium Profile'),
            ('digital_marketing', 'Digital Marketing'),
            ('lead_generation', 'Lead Generation')
        ]:
            if opp_name not in opportunities:
                continue
            opp_df = opportunities[opp_name]

            base_export_cols = [
                'name', 'category', 'city', 'phone', 'rating', 'reviews_count',
                'website', 'instagram', 'facebook', 'digital_presence',
                'instagram_followers', 'facebook_followers',
                'opportunity_score', 'estimated_ltv', 'recommended_pitch'
            ]
            export_cols = [c for c in base_export_cols if c in opp_df.columns]

            st.download_button(
                label=f"Download {opp_label} ({len(opp_df)} vendors)",
                data=opp_df[export_cols].head(200).to_csv(index=False),
                file_name=f"{opp_name}_targets.csv",
                mime="text/csv",
                key=f"download_{opp_name}"
            )

    st.divider()

    # Preview selected tier
    st.subheader("Preview Vendor Data")
    preview_tier = st.selectbox(
        "Select tier to preview:",
        options=['All'] + list(filtered_df['tier'].unique())
    )

    if preview_tier == 'All':
        preview_df = filtered_df
    else:
        preview_df = filtered_df[filtered_df['tier'] == preview_tier]

    preview_base_cols = [
        'name', 'category', 'city', 'rating', 'reviews_count',
        'website', 'instagram', 'facebook', 'digital_presence',
        'instagram_followers', 'facebook_followers',
        'phone', 'tier', 'opportunity_score', 'estimated_ltv'
    ]
    preview_cols = [c for c in preview_base_cols if c in preview_df.columns]
    st.dataframe(
        preview_df[preview_cols].sort_values('opportunity_score', ascending=False).head(100),
        use_container_width=True
    )

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>💍 Vendor Intelligence Dashboard - itsmy.wedding</p>
    <p>Last updated: {}</p>
</div>
""".format(pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')), unsafe_allow_html=True)
