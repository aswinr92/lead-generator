"""
Google Sheets Exporter for wedding vendor data.

This module provides functionality to export vendor data to Google Sheets
with professional formatting, color-coding, data validation, and statistics.
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from typing import Dict, Optional, List
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_exponential
import yaml


class GoogleSheetsExporter:
    """Exports wedding vendor data to Google Sheets with formatting and statistics."""

    SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
    ]

    def __init__(self, credentials_path: str = "config/google_credentials.json", config: Optional[Dict] = None):
        """
        Initialize Google Sheets exporter.

        Args:
            credentials_path: Path to service account JSON credentials
            config: Configuration dictionary (loaded from config.yaml)

        Raises:
            FileNotFoundError: If credentials file doesn't exist
            ValueError: If authentication fails
        """
        if not Path(credentials_path).exists():
            raise FileNotFoundError(
                f"❌ Credentials file not found: {credentials_path}\n"
                f"   See docs/GOOGLE_SHEETS_SETUP.md for setup instructions"
            )

        self.credentials_path = credentials_path
        self.config = config or self._load_default_config()
        self.client = self._authenticate()
        self.service_account_email = self._get_service_account_email()

    def _load_default_config(self) -> Dict:
        """Load default configuration from config.yaml."""
        config_path = Path("config/config.yaml")
        if config_path.exists():
            with open(config_path, 'r') as f:
                full_config = yaml.safe_load(f)
                return full_config.get('google_sheets', {})
        return {}

    def _authenticate(self) -> gspread.Client:
        """
        Authenticate with Google Sheets API using service account.

        Returns:
            Authenticated gspread client

        Raises:
            ValueError: If authentication fails
        """
        try:
            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=self.SCOPES
            )
            return gspread.authorize(creds)
        except Exception as e:
            raise ValueError(f"❌ Authentication failed: {e}")

    def _get_service_account_email(self) -> str:
        """Extract service account email from credentials."""
        try:
            creds = Credentials.from_service_account_file(self.credentials_path)
            return creds.service_account_email
        except Exception:
            return "unknown"

    def export_to_sheet(
        self,
        df: pd.DataFrame,
        sheet_id: Optional[str] = None,
        sheet_name: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Export DataFrame to Google Sheet with formatting and statistics.

        Args:
            df: Vendor data DataFrame with columns:
                name, category, rating, reviews_count, address, phone,
                website, url, search_query, scraped_at
            sheet_id: Existing spreadsheet ID (None to create new)
            sheet_name: Name for the spreadsheet

        Returns:
            Dictionary with:
                - sheet_id: Spreadsheet ID
                - sheet_url: Shareable URL
                - data_tab: Name of data tab
                - summary_tab: Name of summary tab
                - service_account_email: Email to share with

        Raises:
            gspread.exceptions.SpreadsheetNotFound: If sheet_id is invalid
            gspread.exceptions.APIError: If API call fails
        """
        # Validate input
        if df.empty:
            raise ValueError("❌ DataFrame is empty. No data to export.")

        # Add onboarding status column
        df_export = self._add_onboarding_status_column(df.copy())

        # Create or open spreadsheet
        spreadsheet = self._create_or_open_sheet(sheet_id, sheet_name)

        # Get or create data worksheet
        data_tab_name = self.config.get('data_tab_name', 'Vendor Data')
        try:
            data_worksheet = spreadsheet.worksheet(data_tab_name)
            # Clear existing data
            data_worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            # Use the first worksheet and rename it
            data_worksheet = spreadsheet.get_worksheet(0)
            data_worksheet.update_title(data_tab_name)

        # Write and format data
        self._write_data_tab(data_worksheet, df_export)
        self._format_data_tab(data_worksheet, len(df_export))
        self._color_code_ratings(data_worksheet, df_export)
        self._add_data_validation(data_worksheet, len(df_export))

        # Create summary tab
        summary_tab_name = self._create_summary_tab(spreadsheet, df)

        return {
            'sheet_id': spreadsheet.id,
            'sheet_url': spreadsheet.url,
            'data_tab': data_tab_name,
            'summary_tab': summary_tab_name,
            'service_account_email': self.service_account_email
        }

    def update_sheet(self, df: pd.DataFrame, sheet_id: str) -> str:
        """
        Update an existing Google Sheet with vendor data.
    
        Args:
            df: Vendor DataFrame
            sheet_id: Existing Google Sheet ID
        
        Returns:
            Sheet URL
        """
        result = self.export_to_sheet(df, sheet_id=sheet_id, sheet_name=None)
        return result['sheet_url']


    def create_and_export(self, df: pd.DataFrame, sheet_name: str) -> str:
        """
        Create a new Google Sheet and export vendor data.
    
        Args:
            df: Vendor DataFrame
            sheet_name: Name for the new sheet
        
        Returns:
            Sheet URL
        """
        result = self.export_to_sheet(df, sheet_id=None, sheet_name=sheet_name)
        return result['sheet_url']

    def _add_onboarding_status_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add empty onboarding_status column for manual tracking."""
        df['onboarding_status'] = ''
        return df

    def _create_or_open_sheet(
        self,
        sheet_id: Optional[str],
        sheet_name: Optional[str]
    ) -> gspread.Spreadsheet:
        """
        Create new spreadsheet or open existing one.

        Args:
            sheet_id: Existing sheet ID or None
            sheet_name: Name for new sheet

        Returns:
            Spreadsheet object

        Raises:
            gspread.exceptions.SpreadsheetNotFound: If sheet_id is invalid
        """
        if sheet_id:
            try:
                return self.client.open_by_key(sheet_id)
            except gspread.exceptions.SpreadsheetNotFound:
                raise gspread.exceptions.SpreadsheetNotFound(
                    f"❌ Spreadsheet not found with ID: {sheet_id}\n"
                    f"   Verify the Sheet ID or create a new sheet"
                )
        else:
            # Create new spreadsheet
            if not sheet_name:
                sheet_name = self.config.get('default_sheet_name', 'Wedding Vendors')
            return self.client.create(sheet_name)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def _write_data_tab(self, worksheet: gspread.Worksheet, df: pd.DataFrame):
        """
        Write vendor data to worksheet with retry logic.

        Args:
            worksheet: Target worksheet
            df: DataFrame with vendor data
        """
        # Prepare data: header + rows
        header = df.columns.tolist()
        df_dict = df.fillna('').to_dict('split')
        rows = [[str(cell) for cell in row] for row in df_dict['data']]
        all_data = [header] + rows

        # Batch update (more efficient than row-by-row)
        try:
            worksheet.update('A1', all_data)
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                print("⚠️  Rate limit hit, retrying...")
                raise  # Trigger retry
            elif "insufficient permissions" in str(e).lower():
                raise PermissionError(
                    f"❌ Service account lacks write permissions\n"
                    f"   Share the sheet with: {self.service_account_email}\n"
                    f"   Give 'Editor' role"
                )
            raise

    def _format_data_tab(self, worksheet: gspread.Worksheet, num_rows: int):
        """
        Apply formatting to data worksheet.

        Formatting includes:
        - Bold header row
        - Blue background for header
        - White text for header
        - Frozen header row
        - Auto-resize columns
        - Enable filters

        Args:
            worksheet: Target worksheet
            num_rows: Number of data rows (excluding header)
        """
        formatting_config = self.config.get('formatting', {})

        # Format header row (row 1)
        if formatting_config.get('bold_headers', True):
            worksheet.format('A1:K1', {
                'textFormat': {'bold': True},
                'backgroundColor': {'red': 0.26, 'green': 0.52, 'blue': 0.96},  # Google blue
                'horizontalAlignment': 'CENTER'
            })

        # Freeze header row
        if formatting_config.get('freeze_header', True):
            worksheet.freeze(rows=1)

        # Auto-resize columns
        if formatting_config.get('auto_resize_columns', True):
            self._auto_resize_columns(worksheet)

        # Enable filters
        if formatting_config.get('enable_filters', True):
            worksheet.set_basic_filter(f'A1:K{num_rows + 1}')

    def _auto_resize_columns(self, worksheet: gspread.Worksheet):
        """Auto-resize all columns based on content."""
        try:
            # Request auto-resize for all columns
            spreadsheet = worksheet.spreadsheet
            requests = [{
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': worksheet.id,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': 11  # Columns A-K (0-10)
                    }
                }
            }]
            spreadsheet.batch_update({'requests': requests})
        except Exception as e:
            print(f"⚠️  Could not auto-resize columns: {e}")

    def _color_code_ratings(self, worksheet: gspread.Worksheet, df: pd.DataFrame):
        """
        Apply conditional formatting to rating column.

        Color scheme:
        - Green: rating >= 4.0 (excellent)
        - Yellow: rating >= 3.0 (good)
        - Red: rating < 3.0 (poor)

        Args:
            worksheet: Target worksheet
            df: DataFrame with rating data
        """
        rating_colors = self.config.get('formatting', {}).get('rating_colors', {})

        # Get thresholds
        excellent_threshold = rating_colors.get('excellent', {}).get('threshold', 4.0)
        good_threshold = rating_colors.get('good', {}).get('threshold', 3.0)

        # Get colors (hex to RGB)
        excellent_color = self._hex_to_rgb(rating_colors.get('excellent', {}).get('color', '#d9ead3'))
        good_color = self._hex_to_rgb(rating_colors.get('good', {}).get('color', '#fff2cc'))
        poor_color = self._hex_to_rgb(rating_colors.get('poor', {}).get('color', '#f4cccc'))

        try:
            spreadsheet = worksheet.spreadsheet

            # Build conditional format rules
            rules = [
                {
                    'addConditionalFormatRule': {
                        'rule': {
                            'ranges': [{
                                'sheetId': worksheet.id,
                                'startRowIndex': 1,  # Skip header
                                'endRowIndex': len(df) + 1,
                                'startColumnIndex': 2,  # Column C (rating)
                                'endColumnIndex': 3
                            }],
                            'booleanRule': {
                                'condition': {
                                    'type': 'NUMBER_GREATER_THAN_EQ',
                                    'values': [{'userEnteredValue': str(excellent_threshold)}]
                                },
                                'format': {
                                    'backgroundColor': excellent_color
                                }
                            }
                        },
                        'index': 0
                    }
                },
                {
                    'addConditionalFormatRule': {
                        'rule': {
                            'ranges': [{
                                'sheetId': worksheet.id,
                                'startRowIndex': 1,
                                'endRowIndex': len(df) + 1,
                                'startColumnIndex': 2,
                                'endColumnIndex': 3
                            }],
                            'booleanRule': {
                                'condition': {
                                    'type': 'NUMBER_BETWEEN',
                                    'values': [
                                        {'userEnteredValue': str(good_threshold)},
                                        {'userEnteredValue': str(excellent_threshold - 0.01)}
                                    ]
                                },
                                'format': {
                                    'backgroundColor': good_color
                                }
                            }
                        },
                        'index': 1
                    }
                },
                {
                    'addConditionalFormatRule': {
                        'rule': {
                            'ranges': [{
                                'sheetId': worksheet.id,
                                'startRowIndex': 1,
                                'endRowIndex': len(df) + 1,
                                'startColumnIndex': 2,
                                'endColumnIndex': 3
                            }],
                            'booleanRule': {
                                'condition': {
                                    'type': 'NUMBER_LESS',
                                    'values': [{'userEnteredValue': str(good_threshold)}]
                                },
                                'format': {
                                    'backgroundColor': poor_color
                                }
                            }
                        },
                        'index': 2
                    }
                }
            ]

            spreadsheet.batch_update({'requests': rules})
        except Exception as e:
            print(f"⚠️  Could not apply color coding: {e}")

    def _hex_to_rgb(self, hex_color: str) -> Dict[str, float]:
        """Convert hex color to RGB dict for Google Sheets API."""
        hex_color = hex_color.lstrip('#')
        r = int(hex_color[0:2], 16) / 255.0
        g = int(hex_color[2:4], 16) / 255.0
        b = int(hex_color[4:6], 16) / 255.0
        return {'red': r, 'green': g, 'blue': b}

    def _add_data_validation(self, worksheet: gspread.Worksheet, num_rows: int):
        """
        Add dropdown data validation to onboarding_status column.

        Dropdown options:
        - Not Started
        - In Progress
        - Contacted
        - Onboarded
        - Rejected

        Args:
            worksheet: Target worksheet
            num_rows: Number of data rows
        """
        try:
            spreadsheet = worksheet.spreadsheet

            validation_rule = {
                'setDataValidation': {
                    'range': {
                        'sheetId': worksheet.id,
                        'startRowIndex': 1,  # Skip header
                        'endRowIndex': num_rows + 1,
                        'startColumnIndex': 10,  # Column K (onboarding_status)
                        'endColumnIndex': 11
                    },
                    'rule': {
                        'condition': {
                            'type': 'ONE_OF_LIST',
                            'values': [
                                {'userEnteredValue': 'Not Started'},
                                {'userEnteredValue': 'In Progress'},
                                {'userEnteredValue': 'Contacted'},
                                {'userEnteredValue': 'Onboarded'},
                                {'userEnteredValue': 'Rejected'}
                            ]
                        },
                        'showCustomUi': True,
                        'strict': False
                    }
                }
            }

            spreadsheet.batch_update({'requests': [validation_rule]})
        except Exception as e:
            print(f"⚠️  Could not add data validation: {e}")

    def _calculate_statistics(self, df: pd.DataFrame) -> Dict:
        """
        Calculate statistics from vendor data.

        Returns:
            Dictionary with statistics:
            - total_vendors
            - by_category
            - by_city
            - with_phone
            - without_phone
            - with_website
            - without_website
            - avg_rating
            - top_rated (top 5)
            - most_reviewed (top 5)
        """
        stats = {}

        # Total vendors
        stats['total_vendors'] = len(df)

        # By category
        stats['by_category'] = df['category'].value_counts().to_dict()

        # By city (extract from search_query)
        # search_query format: "wedding caterers in Trivandrum"
        cities = df['search_query'].str.extract(r'in (.+)$')[0]
        stats['by_city'] = cities.value_counts().to_dict()

        # Phone statistics
        stats['with_phone'] = df['phone'].notna().sum()
        stats['without_phone'] = df['phone'].isna().sum()

        # Digital presence statistics (use new columns if available, fall back to binary)
        if 'digital_presence' in df.columns:
            stats['with_real_website'] = (df['digital_presence'] == 'full_website').sum()
            stats['social_only'] = (df['digital_presence'] == 'social_only').sum()
            stats['no_presence'] = (df['digital_presence'] == 'none').sum()
            stats['with_website'] = stats['with_real_website']
            stats['without_website'] = stats['social_only'] + stats['no_presence']
        else:
            stats['with_website'] = df['website'].notna().sum()
            stats['without_website'] = df['website'].isna().sum()
            stats['with_real_website'] = stats['with_website']
            stats['social_only'] = 0
            stats['no_presence'] = stats['without_website']

        # Average rating
        ratings = pd.to_numeric(df['rating'], errors='coerce')
        stats['avg_rating'] = ratings.mean() if not ratings.isna().all() else 0

        # Top rated vendors (rating > 0, sorted by rating then reviews)
        df_with_rating = df[pd.to_numeric(df['rating'], errors='coerce') > 0].copy()
        df_with_rating['rating_num'] = pd.to_numeric(df_with_rating['rating'], errors='coerce')
        df_with_rating['reviews_num'] = pd.to_numeric(df_with_rating['reviews_count'], errors='coerce').fillna(0)
        top_rated = df_with_rating.nlargest(5, ['rating_num', 'reviews_num'])[['name', 'category', 'rating', 'reviews_count']]
        stats['top_rated'] = top_rated.to_dict('records')

        # Most reviewed vendors
        df_with_reviews = df[pd.to_numeric(df['reviews_count'], errors='coerce') > 0].copy()
        df_with_reviews['reviews_num'] = pd.to_numeric(df_with_reviews['reviews_count'], errors='coerce')
        most_reviewed = df_with_reviews.nlargest(5, 'reviews_num')[['name', 'category', 'rating', 'reviews_count']]
        stats['most_reviewed'] = most_reviewed.to_dict('records')

        return stats

    def _create_summary_tab(self, spreadsheet: gspread.Spreadsheet, df: pd.DataFrame) -> str:
        """
        Create summary and statistics tab.

        Args:
            spreadsheet: Target spreadsheet
            df: Original vendor DataFrame (without onboarding_status)

        Returns:
            Name of the summary tab
        """
        summary_tab_name = self.config.get('summary_tab_name', 'Summary & Statistics')

        # Calculate statistics
        stats = self._calculate_statistics(df)

        # Create or get summary worksheet
        try:
            summary_worksheet = spreadsheet.worksheet(summary_tab_name)
            summary_worksheet.clear()
        except gspread.exceptions.WorksheetNotFound:
            summary_worksheet = spreadsheet.add_worksheet(
                title=summary_tab_name,
                rows=100,
                cols=10
            )

        # Build summary content
        summary_data = []

        # Header
        summary_data.append(['Wedding Vendor Database - Summary & Statistics'])
        summary_data.append([''])

        # Overview
        summary_data.append(['OVERVIEW'])
        summary_data.append(['Total Vendors', stats['total_vendors']])
        summary_data.append(['Average Rating', f"{stats['avg_rating']:.2f}" if stats['avg_rating'] > 0 else 'N/A'])
        summary_data.append([''])

        # Contact Information
        summary_data.append(['CONTACT INFORMATION'])
        summary_data.append(['Vendors with Phone', stats['with_phone']])
        summary_data.append(['Vendors without Phone', stats['without_phone']])
        summary_data.append([''])

        # Digital Presence Breakdown
        summary_data.append(['DIGITAL PRESENCE'])
        summary_data.append(['Real Website', stats['with_real_website']])
        summary_data.append(['Instagram / Facebook Only', stats['social_only']])
        summary_data.append(['No Digital Presence', stats['no_presence']])
        summary_data.append([''])

        # Legacy compatibility row
        summary_data.append(['WEBSITE SUMMARY'])
        summary_data.append(['Vendors with Website (any)', stats['with_website']])
        summary_data.append(['Vendors without Website', stats['without_website']])
        summary_data.append([''])

        # By Category
        summary_data.append(['VENDORS BY CATEGORY'])
        for category, count in sorted(stats['by_category'].items(), key=lambda x: x[1], reverse=True):
            summary_data.append([category, count])
        summary_data.append([''])

        # By City
        summary_data.append(['VENDORS BY CITY'])
        for city, count in sorted(stats['by_city'].items(), key=lambda x: x[1], reverse=True):
            summary_data.append([city, count])
        summary_data.append([''])

        # Top Rated
        summary_data.append(['TOP 5 HIGHEST RATED VENDORS'])
        summary_data.append(['Name', 'Category', 'Rating', 'Reviews'])
        for vendor in stats['top_rated']:
            summary_data.append([
                vendor.get('name', ''),
                vendor.get('category', ''),
                vendor.get('rating', ''),
                vendor.get('reviews_count', '')
            ])
        summary_data.append([''])

        # Most Reviewed
        summary_data.append(['TOP 5 MOST REVIEWED VENDORS'])
        summary_data.append(['Name', 'Category', 'Rating', 'Reviews'])
        for vendor in stats['most_reviewed']:
            summary_data.append([
                vendor.get('name', ''),
                vendor.get('category', ''),
                vendor.get('rating', ''),
                vendor.get('reviews_count', '')
            ])

        # Convert any int64/float64 to native Python types
        summary_data_clean = []
        for row in summary_data:
            clean_row = []
            for cell in row:
                if isinstance(cell, (pd.Int64Dtype, pd.Float64Dtype)) or hasattr(cell, 'item'):
                    # Convert numpy/pandas types to Python native
                    clean_row.append(cell.item() if hasattr(cell, 'item') else cell)
                else:
                    clean_row.append(cell)
                summary_data_clean.append(clean_row)

        # Write data
        summary_worksheet.update('A1', summary_data_clean)

        # Format summary tab
        self._format_summary_tab(summary_worksheet)

        return summary_tab_name

    def _format_summary_tab(self, worksheet: gspread.Worksheet):
        """Apply formatting to summary worksheet."""
        try:
            # Bold and larger font for main header
            worksheet.format('A1', {
                'textFormat': {'bold': True, 'fontSize': 14},
                'horizontalAlignment': 'CENTER'
            })

            # Bold for section headers
            # Find all section headers (all caps text in column A)
            all_values = worksheet.get_all_values()
            for i, row in enumerate(all_values, start=1):
                if row[0] and row[0].isupper() and len(row[0]) > 3:
                    worksheet.format(f'A{i}:D{i}', {
                        'textFormat': {'bold': True},
                        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
                    })

            # Auto-resize columns
            self._auto_resize_columns(worksheet)

        except Exception as e:
            print(f"⚠️  Could not format summary tab: {e}")
