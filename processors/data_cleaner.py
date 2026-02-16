"""
Data Cleaner Module
Cleans and standardizes vendor data fields.
"""

import re
import pandas as pd
import phonenumbers
from phonenumbers import NumberParseException
from urllib.parse import urlparse, urlunparse, parse_qs
from typing import Optional


class VendorDataCleaner:
    """Cleans and standardizes wedding vendor data."""

    # City name standardizations
    CITY_MAPPINGS = {
        'trivandrum': 'Thiruvananthapuram',
        'tvm': 'Thiruvananthapuram',
        'cochin': 'Kochi',
        'calicut': 'Kozhikode',
        'trichur': 'Thrissur',
        'alleppey': 'Alappuzha',
        'palghat': 'Palakkad',
    }

    # Invalid phone patterns
    INVALID_PHONE_PATTERNS = [
        r'^1{10,}$',  # All 1s
        r'^0{10,}$',  # All 0s
        r'^9{10,}$',  # All 9s
        r'^1234567890$',  # Sequential
        r'^0000000000$',
    ]

    # Business name cleanup patterns
    NAME_CLEANUP_PATTERNS = [
        (r'\s+', ' '),  # Multiple spaces to single
        (r'^\s+|\s+$', ''),  # Trim
    ]

    def __init__(self, default_country='IN'):
        """
        Initialize the data cleaner.

        Args:
            default_country: Default country code for phone numbers
        """
        self.default_country = default_country

    def clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean entire dataframe.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        df = df.copy()

        # Clean each column
        df['name'] = df['name'].apply(self.clean_business_name)
        df['phone'] = df['phone'].apply(self.clean_phone_number)
        df['address'] = df['address'].apply(self.clean_address)
        df['website'] = df['website'].apply(self.clean_website)
        df['rating'] = df['rating'].apply(self.clean_rating)
        df['reviews_count'] = df['reviews_count'].apply(self.clean_reviews_count)
        df['category'] = df['category'].apply(self.clean_category)

        # Classify and split social media URLs into separate columns
        df = self._classify_and_split_social_media(df)

        return df

    def _classify_url(self, url: str) -> str:
        """Classify a URL as 'website', 'instagram', 'facebook', or 'none'."""
        if not url or pd.isna(url) or str(url).strip() == '':
            return 'none'
        url_lower = str(url).lower()
        if 'instagram.com' in url_lower:
            return 'instagram'
        if 'facebook.com' in url_lower or 'fb.com' in url_lower:
            return 'facebook'
        if url_lower.startswith('http'):
            return 'website'
        return 'none'

    def _classify_and_split_social_media(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Classify website URLs and split social media into separate columns.

        Adds columns:
        - website_type:        'website' | 'instagram' | 'facebook' | 'none'
        - instagram:           Instagram URL (moved from website if applicable)
        - facebook:            Facebook URL (moved from website if applicable)
        - digital_presence:    'full_website' | 'social_only' | 'none'
        - instagram_followers: empty string (filled by SocialMediaEnricher)
        - facebook_followers:  empty string (filled by SocialMediaEnricher)
        """
        df = df.copy()

        # Preserve existing social columns if they already exist (backfill case)
        for col in ('instagram', 'facebook', 'instagram_followers', 'facebook_followers'):
            if col not in df.columns:
                df[col] = ''

        # Classify each URL
        df['website_type'] = df['website'].apply(self._classify_url)

        # Move social media URLs into their own columns; clear from website
        instagram_mask = df['website_type'] == 'instagram'
        facebook_mask = df['website_type'] == 'facebook'

        df.loc[instagram_mask, 'instagram'] = df.loc[instagram_mask, 'website']
        df.loc[instagram_mask, 'website'] = ''

        df.loc[facebook_mask, 'facebook'] = df.loc[facebook_mask, 'website']
        df.loc[facebook_mask, 'website'] = ''

        # Set digital_presence
        has_website = df['website'].str.startswith('http', na=False)
        has_social = (df['instagram'].fillna('') != '') | (df['facebook'].fillna('') != '')

        df['digital_presence'] = 'none'
        df.loc[has_social & ~has_website, 'digital_presence'] = 'social_only'
        df.loc[has_website, 'digital_presence'] = 'full_website'

        return df

    def clean_business_name(self, name: str) -> str:
        """
        Clean and standardize business name.

        Args:
            name: Raw business name

        Returns:
            Cleaned business name
        """
        if pd.isna(name) or not str(name).strip():
            return ""

        name = str(name).strip()

        # Apply cleanup patterns
        for pattern, replacement in self.NAME_CLEANUP_PATTERNS:
            name = re.sub(pattern, replacement, name)

        # Title case (preserve acronyms)
        words = name.split()
        cleaned_words = []
        for word in words:
            # Keep all-caps words (acronyms)
            if word.isupper() and len(word) > 1:
                cleaned_words.append(word)
            else:
                cleaned_words.append(word.capitalize())

        return ' '.join(cleaned_words)

    def clean_phone_number(self, phone: str) -> str:
        """
        Clean and standardize phone number.

        Args:
            phone: Raw phone number

        Returns:
            Cleaned phone number in E164 format or empty string
        """
        if pd.isna(phone) or not str(phone).strip():
            return ""

        phone = str(phone).strip()

        # Remove common separators
        phone = re.sub(r'[\s\-\(\)\.]', '', phone)

        # Check for invalid patterns
        for pattern in self.INVALID_PHONE_PATTERNS:
            if re.match(pattern, phone):
                return ""

        # Try to parse and format
        try:
            # Try parsing as-is
            parsed = phonenumbers.parse(phone, self.default_country)

            # Validate
            if phonenumbers.is_valid_number(parsed):
                # Return in E164 format (+91XXXXXXXXXX)
                return phonenumbers.format_number(
                    parsed,
                    phonenumbers.PhoneNumberFormat.E164
                )
        except NumberParseException:
            # Try adding country code
            try:
                if not phone.startswith('+') and not phone.startswith('91'):
                    phone = '+91' + phone
                    parsed = phonenumbers.parse(phone, self.default_country)

                    if phonenumbers.is_valid_number(parsed):
                        return phonenumbers.format_number(
                            parsed,
                            phonenumbers.PhoneNumberFormat.E164
                        )
            except:
                pass

        # If all else fails, return standardized format
        # Remove non-digits
        digits = re.sub(r'\D', '', phone)

        # Must be 10 or 12 digits (with country code)
        if len(digits) == 10:
            return f"+91{digits}"
        elif len(digits) == 12 and digits.startswith('91'):
            return f"+{digits}"

        return ""

    def clean_address(self, address: str) -> str:
        """
        Clean and standardize address.

        Args:
            address: Raw address

        Returns:
            Cleaned address
        """
        if pd.isna(address) or not str(address).strip():
            return ""

        address = str(address).strip()

        # Remove excessive whitespace and newlines
        address = re.sub(r'\s+', ' ', address)
        address = re.sub(r'\n+', ', ', address)

        # Standardize city names
        for old_city, new_city in self.CITY_MAPPINGS.items():
            address = re.sub(
                rf'\b{old_city}\b',
                new_city,
                address,
                flags=re.IGNORECASE
            )

        return address.strip()

    def extract_pincode(self, address: str) -> Optional[str]:
        """
        Extract pincode from address.

        Args:
            address: Address string

        Returns:
            Pincode or None
        """
        if pd.isna(address):
            return None

        # Indian pincode pattern (6 digits)
        match = re.search(r'\b\d{6}\b', str(address))
        if match:
            return match.group(0)

        return None

    def extract_city(self, address: str) -> Optional[str]:
        """
        Extract city from address.

        Args:
            address: Address string

        Returns:
            City name or None
        """
        if pd.isna(address):
            return None

        address = str(address)

        # Check for known cities
        for city in self.CITY_MAPPINGS.values():
            if city.lower() in address.lower():
                return city

        # Try to extract from Kerala addresses
        # Usually format: ..., City, Kerala PIN
        match = re.search(r',\s*([^,]+),\s*Kerala', address, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            # Clean and title case
            city = self.clean_business_name(city)
            return city

        return None

    def clean_website(self, website: str) -> str:
        """
        Clean and standardize website URL.

        Args:
            website: Raw website URL

        Returns:
            Cleaned URL
        """
        if pd.isna(website) or not str(website).strip():
            return ""

        website = str(website).strip()

        # Add https:// if missing protocol
        if not website.startswith(('http://', 'https://')):
            website = 'https://' + website

        try:
            # Parse URL
            parsed = urlparse(website)

            # Remove tracking parameters
            query_params = parse_qs(parsed.query)

            # Common tracking parameters to remove
            tracking_params = [
                'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
                'fbclid', 'gclid', 'msclkid', 'ref', 'source'
            ]

            # Keep only non-tracking params
            clean_params = {
                k: v for k, v in query_params.items()
                if k not in tracking_params
            }

            # Rebuild query string
            from urllib.parse import urlencode
            clean_query = urlencode(clean_params, doseq=True) if clean_params else ''

            # Rebuild URL
            clean_url = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                clean_query,
                ''  # Remove fragment
            ))

            return clean_url

        except:
            return website

    def clean_rating(self, rating) -> float:
        """
        Clean and convert rating to float.

        Args:
            rating: Raw rating value

        Returns:
            Float rating or 0.0
        """
        if pd.isna(rating) or rating == '':
            return 0.0

        try:
            rating_float = float(str(rating).strip())

            # Validate range (Google Maps is 0-5)
            if 0 <= rating_float <= 5:
                return round(rating_float, 1)
        except (ValueError, TypeError):
            pass

        return 0.0

    def clean_reviews_count(self, count) -> int:
        """
        Clean and convert reviews count to integer.

        Args:
            count: Raw reviews count

        Returns:
            Integer count or 0
        """
        if pd.isna(count) or count == '':
            return 0

        try:
            # Remove commas and convert
            count_str = str(count).replace(',', '').strip()
            return int(count_str)
        except (ValueError, TypeError):
            pass

        return 0

    def clean_category(self, category: str) -> str:
        """
        Clean and standardize category.

        Args:
            category: Raw category

        Returns:
            Cleaned category
        """
        if pd.isna(category) or not str(category).strip():
            return ""

        category = str(category).strip()

        # Title case
        category = self.clean_business_name(category)

        return category

    def add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived fields like city, pincode.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with added fields
        """
        df = df.copy()

        # Extract city and pincode from address
        df['city'] = df['address'].apply(self.extract_city)
        df['pincode'] = df['address'].apply(self.extract_pincode)

        # Add data quality score
        df['quality_score'] = df.apply(self._calculate_quality_score, axis=1)

        return df

    def _calculate_quality_score(self, row) -> int:
        """Calculate data quality score (0-100) for a record."""
        score = 0

        # Name (20 points)
        if row.get('name') and len(str(row['name']).strip()) > 0:
            score += 20

        # Phone (25 points) - most important
        if row.get('phone') and str(row['phone']).startswith('+91'):
            score += 25

        # Address (15 points)
        if row.get('address') and len(str(row['address']).strip()) > 10:
            score += 15

        # Digital presence (10 points for real website, 5 for social-only)
        if row.get('website') and str(row['website']).startswith('http'):
            score += 10
        elif row.get('instagram') or row.get('facebook'):
            score += 5

        # Rating (15 points)
        if row.get('rating') and float(row.get('rating', 0)) > 0:
            score += 15

        # Reviews count (10 points)
        if row.get('reviews_count') and int(row.get('reviews_count', 0)) > 0:
            score += 10

        # Category (5 points)
        if row.get('category') and len(str(row['category']).strip()) > 0:
            score += 5

        return score


def clean_vendor_data(input_csv: str, output_csv: str = None) -> pd.DataFrame:
    """
    Clean vendor data from CSV file.

    Args:
        input_csv: Path to input CSV file
        output_csv: Path to output CSV file (optional)

    Returns:
        Cleaned DataFrame
    """
    # Load data
    df = pd.read_csv(input_csv)

    print(f"📂 Loaded {len(df)} records from {input_csv}")

    # Initialize cleaner
    cleaner = VendorDataCleaner()

    # Clean data
    print("\n🧹 Cleaning data...")
    df_cleaned = cleaner.clean_dataframe(df)

    # Add derived fields
    print("➕ Adding derived fields...")
    df_cleaned = cleaner.add_derived_fields(df_cleaned)

    # Save if output path provided
    if output_csv:
        df_cleaned.to_csv(output_csv, index=False)
        print(f"\n💾 Saved cleaned data to {output_csv}")

    return df_cleaned


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python data_cleaner.py <input_csv> [output_csv]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    clean_vendor_data(input_file, output_file)
