"""
Deduplicator Module
Identifies and merges duplicate vendor records across multiple CSV files.
"""

import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
try:
    from thefuzz import fuzz
except ImportError:
    from fuzzywuzzy import fuzz
from collections import defaultdict
import hashlib


class VendorDeduplicator:
    """Deduplicates vendor records using multiple matching strategies."""

    def __init__(
        self,
        name_similarity_threshold: float = 85.0,
        address_similarity_threshold: float = 80.0
    ):
        """
        Initialize deduplicator.

        Args:
            name_similarity_threshold: Minimum similarity score for name matching (0-100)
            address_similarity_threshold: Minimum similarity for address matching (0-100)
        """
        self.name_threshold = name_similarity_threshold
        self.address_threshold = address_similarity_threshold
        self.duplicate_log = []

    def deduplicate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        Deduplicate vendor records.

        Args:
            df: Input DataFrame with vendor records

        Returns:
            Tuple of (deduplicated DataFrame, duplicate log)
        """
        print(f"\n🔍 Starting deduplication on {len(df)} records...")

        # Reset duplicate log
        self.duplicate_log = []

        # Add temporary ID for tracking
        df = df.copy()
        df['_temp_id'] = range(len(df))

        # Find duplicates using multiple strategies
        duplicate_groups = self._find_duplicate_groups(df)

        print(f"   Found {len(duplicate_groups)} duplicate groups")

        # Merge duplicates
        df_deduped = self._merge_duplicates(df, duplicate_groups)

        print(f"✅ Deduplication complete: {len(df)} → {len(df_deduped)} records")
        print(f"   Removed {len(df) - len(df_deduped)} duplicates")

        return df_deduped, self.duplicate_log

    def _find_duplicate_groups(self, df: pd.DataFrame) -> List[List[int]]:
        """
        Find groups of duplicate records.

        Args:
            df: Input DataFrame

        Returns:
            List of duplicate groups (each group is a list of temp_ids)
        """
        duplicate_groups = []
        processed_ids = set()

        # Strategy 1: Exact phone match (highest priority)
        phone_groups = self._find_phone_duplicates(df)
        for group in phone_groups:
            if len(group) > 1:
                duplicate_groups.append(group)
                processed_ids.update(group)

        # Strategy 2: Name + Address similarity
        name_address_groups = self._find_name_address_duplicates(df, processed_ids)
        for group in name_address_groups:
            if len(group) > 1:
                duplicate_groups.append(group)
                processed_ids.update(group)

        # Strategy 3: Name similarity + same city (for records without phone)
        name_city_groups = self._find_name_city_duplicates(df, processed_ids)
        for group in name_city_groups:
            if len(group) > 1:
                duplicate_groups.append(group)
                processed_ids.update(group)

        return duplicate_groups

    def _find_phone_duplicates(self, df: pd.DataFrame) -> List[List[int]]:
        """Find duplicates based on exact phone match."""
        groups = []

        # Group by phone (exclude empty)
        phone_groups = df[df['phone'].notna() & (df['phone'] != '')].groupby('phone')

        for phone, group_df in phone_groups:
            if len(group_df) > 1:
                groups.append(group_df['_temp_id'].tolist())

        return groups

    def _find_name_address_duplicates(
        self,
        df: pd.DataFrame,
        processed_ids: set
    ) -> List[List[int]]:
        """Find duplicates based on name and address similarity."""
        groups = []

        # Filter out already processed and records without names
        df_unprocessed = df[
            ~df['_temp_id'].isin(processed_ids) &
            df['name'].notna() &
            (df['name'] != '') &
            df['address'].notna() &
            (df['address'] != '')
        ]

        # Compare each record with others
        processed_in_this_round = set()

        for idx1, row1 in df_unprocessed.iterrows():
            if row1['_temp_id'] in processed_in_this_round:
                continue

            duplicates = [row1['_temp_id']]

            for idx2, row2 in df_unprocessed.iterrows():
                if idx1 >= idx2 or row2['_temp_id'] in processed_in_this_round:
                    continue

                # Calculate similarities
                name_sim = fuzz.token_sort_ratio(
                    str(row1['name']).lower(),
                    str(row2['name']).lower()
                )

                address_sim = fuzz.partial_ratio(
                    str(row1['address']).lower(),
                    str(row2['address']).lower()
                )

                # Both name and address must be similar
                if (name_sim >= self.name_threshold and
                    address_sim >= self.address_threshold):
                    duplicates.append(row2['_temp_id'])
                    processed_in_this_round.add(row2['_temp_id'])

            if len(duplicates) > 1:
                groups.append(duplicates)
                processed_in_this_round.update(duplicates)

        return groups

    def _find_name_city_duplicates(
        self,
        df: pd.DataFrame,
        processed_ids: set
    ) -> List[List[int]]:
        """Find duplicates based on name similarity in same city."""
        groups = []

        # Filter unprocessed records with names but no phone
        df_unprocessed = df[
            ~df['_temp_id'].isin(processed_ids) &
            df['name'].notna() &
            (df['name'] != '') &
            (df['phone'].isna() | (df['phone'] == ''))
        ]

        # Add city if not present
        if 'city' not in df_unprocessed.columns:
            from .data_cleaner import VendorDataCleaner
            cleaner = VendorDataCleaner()
            df_unprocessed = df_unprocessed.copy()
            df_unprocessed['city'] = df_unprocessed['address'].apply(cleaner.extract_city)

        # Group by city
        processed_in_this_round = set()

        for city, city_df in df_unprocessed.groupby('city'):
            if pd.isna(city) or city == '':
                continue

            for idx1, row1 in city_df.iterrows():
                if row1['_temp_id'] in processed_in_this_round:
                    continue

                duplicates = [row1['_temp_id']]

                for idx2, row2 in city_df.iterrows():
                    if idx1 >= idx2 or row2['_temp_id'] in processed_in_this_round:
                        continue

                    # High name similarity threshold for this strategy
                    name_sim = fuzz.token_sort_ratio(
                        str(row1['name']).lower(),
                        str(row2['name']).lower()
                    )

                    if name_sim >= 90:  # Higher threshold
                        duplicates.append(row2['_temp_id'])
                        processed_in_this_round.add(row2['_temp_id'])

                if len(duplicates) > 1:
                    groups.append(duplicates)
                    processed_in_this_round.update(duplicates)

        return groups

    def _merge_duplicates(
        self,
        df: pd.DataFrame,
        duplicate_groups: List[List[int]]
    ) -> pd.DataFrame:
        """
        Merge duplicate records, keeping the most complete data.

        Args:
            df: Input DataFrame
            duplicate_groups: Groups of duplicate temp_ids

        Returns:
            DataFrame with merged records
        """
        # Track which rows to keep/remove
        rows_to_remove = set()
        merged_records = {}

        for group in duplicate_groups:
            # Get records in this group
            group_df = df[df['_temp_id'].isin(group)]

            # Find best record (highest quality score)
            if 'quality_score' in group_df.columns:
                best_idx = group_df['quality_score'].idxmax()
            else:
                # Fallback: most non-null fields
                best_idx = group_df.notna().sum(axis=1).idxmax()

            best_record = group_df.loc[best_idx].copy()

            # Merge data from all records
            merged_record = self._merge_group_data(group_df, best_record)

            # Store merged record
            merged_records[best_idx] = merged_record

            # Mark others for removal
            for temp_id in group:
                if df[df['_temp_id'] == temp_id].index[0] != best_idx:
                    rows_to_remove.add(df[df['_temp_id'] == temp_id].index[0])

            # Log the merge
            self._log_duplicate(group_df, merged_record)

        # Update merged records in dataframe
        for idx, record in merged_records.items():
            for col in record.index:
                if col != '_temp_id':
                    df.at[idx, col] = record[col]

        # Remove duplicate rows
        df_deduped = df[~df.index.isin(rows_to_remove)].copy()

        # Remove temporary ID
        df_deduped = df_deduped.drop(columns=['_temp_id'])

        return df_deduped

    def _merge_group_data(
        self,
        group_df: pd.DataFrame,
        best_record: pd.Series
    ) -> pd.Series:
        """
        Merge data from duplicate group, preferring most complete data.

        Args:
            group_df: DataFrame with duplicate records
            best_record: The best record to use as base

        Returns:
            Merged record
        """
        merged = best_record.copy()

        # For each field, take the most complete/best value
        for col in group_df.columns:
            if col in ['_temp_id', 'scraped_at', 'search_query']:
                continue

            # Skip if best record already has good data
            if pd.notna(merged[col]) and merged[col] != '':
                # Special handling for some fields
                if col == 'reviews_count':
                    # Take maximum reviews count
                    max_reviews = group_df[col].replace('', 0).fillna(0).astype(float).max()
                    if max_reviews > float(merged[col] or 0):
                        merged[col] = max_reviews
                elif col == 'rating':
                    # Take best rating (if similar number of reviews)
                    best_rating = group_df[col].replace('', 0).fillna(0).astype(float).max()
                    if best_rating > float(merged[col] or 0):
                        merged[col] = best_rating
                continue

            # Find best value from group
            for _, row in group_df.iterrows():
                value = row[col]
                if pd.notna(value) and value != '':
                    merged[col] = value
                    break

        return merged

    def _log_duplicate(self, group_df: pd.DataFrame, merged_record: pd.Series):
        """Log duplicate merge for reporting."""
        log_entry = {
            'duplicate_count': len(group_df),
            'merged_name': merged_record.get('name', ''),
            'merged_phone': merged_record.get('phone', ''),
            'merged_address': merged_record.get('address', ''),
            'original_records': []
        }

        for _, row in group_df.iterrows():
            log_entry['original_records'].append({
                'name': row.get('name', ''),
                'phone': row.get('phone', ''),
                'address': row.get('address', ''),
                'quality_score': row.get('quality_score', 0)
            })

        self.duplicate_log.append(log_entry)


def deduplicate_vendors(
    df: pd.DataFrame,
    name_threshold: float = 85.0,
    address_threshold: float = 80.0
) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Deduplicate vendor DataFrame.

    Args:
        df: Input DataFrame
        name_threshold: Name similarity threshold
        address_threshold: Address similarity threshold

    Returns:
        Tuple of (deduplicated DataFrame, duplicate log)
    """
    deduplicator = VendorDeduplicator(
        name_similarity_threshold=name_threshold,
        address_similarity_threshold=address_threshold
    )

    return deduplicator.deduplicate(df)


def merge_multiple_csvs(csv_files: List[str]) -> pd.DataFrame:
    """
    Merge multiple CSV files into a single DataFrame.

    Args:
        csv_files: List of CSV file paths

    Returns:
        Combined DataFrame
    """
    print(f"\n📂 Merging {len(csv_files)} CSV files...")

    dfs = []
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        print(f"   Loaded {len(df)} records from {csv_file}")
        dfs.append(df)

    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)

    print(f"✅ Combined {len(combined_df)} total records")

    return combined_df


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python deduplicator.py <input_csv> [output_csv]")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    df = pd.read_csv(input_file)
    df_deduped, log = deduplicate_vendors(df)

    if output_file:
        df_deduped.to_csv(output_file, index=False)
        print(f"\n💾 Saved deduplicated data to {output_file}")
