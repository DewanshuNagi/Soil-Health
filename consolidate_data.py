import os
import pandas as pd
import numpy as np
from pathlib import Path
import logging
from typing import Dict, List, Tuple, Optional
import re
from datetime import datetime


class SoilDataConsolidator:
    def __init__(self, raw_data_path: str = "data/raw", processed_data_path: str = "data/processed"):
        """
        Initialize the consolidator with data paths

        Args:
            raw_data_path: Path to raw scraped data
            processed_data_path: Path to save processed data
        """
        self.raw_data_path = Path(raw_data_path)
        self.processed_data_path = Path(processed_data_path)

        # Create processed directory if it doesn't exist
        self.processed_data_path.mkdir(parents=True, exist_ok=True)

        # Setup logging
        self.setup_logging()

        # Standard column mappings for different data formats
        self.column_mappings = {
            # Common variations in column names
            'village': ['village', 'village_name', 'Village', 'Village Name'],
            'farmer_name': ['farmer_name', 'farmer name', 'Farmer Name', 'Farmer_Name'],
            'mobile': ['mobile', 'mobile_no', 'Mobile', 'Mobile No', 'phone'],
            'survey_no': ['survey_no', 'survey number', 'Survey No', 'Survey_No'],
            'area_hectare': ['area_hectare', 'area (hectare)', 'Area', 'area'],
            'sample_id': ['sample_id', 'sample id', 'Sample ID', 'Sample_ID'],
            'collection_date': ['collection_date', 'date', 'Date', 'Collection Date'],

            # Macro nutrients
            'ph': ['ph', 'pH', 'PH'],
            'ec': ['ec', 'EC', 'electrical_conductivity'],
            'oc': ['oc', 'OC', 'organic_carbon'],
            'nitrogen': ['n', 'N', 'nitrogen', 'Nitrogen'],
            'phosphorus': ['p', 'P', 'phosphorus', 'Phosphorus'],
            'potassium': ['k', 'K', 'potassium', 'Potassium'],

            # Micro nutrients
            'iron': ['fe', 'Fe', 'iron', 'Iron'],
            'manganese': ['mn', 'Mn', 'manganese', 'Manganese'],
            'copper': ['cu', 'Cu', 'copper', 'Copper'],
            'zinc': ['zn', 'Zn', 'zinc', 'Zinc'],
            'boron': ['b', 'B', 'boron', 'Boron'],
            'sulphur': ['s', 'S', 'sulphur', 'Sulphur', 'sulfur', 'Sulfur']
        }

        # Standard data types for columns
        self.column_dtypes = {
            'year': 'str',
            'state': 'str',
            'district': 'str',
            'block': 'str',
            'village': 'str',
            'farmer_name': 'str',
            'mobile': 'str',
            'survey_no': 'str',
            'area_hectare': 'float64',
            'sample_id': 'str',
            'collection_date': 'str',
            'ph': 'float64',
            'ec': 'float64',
            'oc': 'float64',
            'nitrogen': 'float64',
            'phosphorus': 'float64',
            'potassium': 'float64',
            'iron': 'float64',
            'manganese': 'float64',
            'copper': 'float64',
            'zinc': 'float64',
            'boron': 'float64',
            'sulphur': 'float64'
        }

    def setup_logging(self):
        """Setup logging configuration"""
        log_file = self.processed_data_path / 'consolidation.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def find_csv_files(self) -> Dict[str, List[Tuple[Path, str]]]:
        """
        Find all CSV files in the raw data directory

        Returns:
            Dictionary with nutrient types as keys and list of (file_path, nutrient_type) tuples
        """
        csv_files = {'macro': [], 'micro': []}

        if not self.raw_data_path.exists():
            self.logger.error(f"Raw data path does not exist: {self.raw_data_path}")
            return csv_files

        # Walk through all directories
        for root, dirs, files in os.walk(self.raw_data_path):
            for file in files:
                if file.endswith('.csv'):
                    file_path = Path(root) / file

                    # Determine nutrient type from filename
                    if 'macro' in file.lower():
                        csv_files['macro'].append((file_path, 'macro'))
                    elif 'micro' in file.lower():
                        csv_files['micro'].append((file_path, 'micro'))
                    else:
                        # Try to determine from directory structure or content
                        self.logger.warning(f"Could not determine nutrient type for: {file_path}")

        self.logger.info(f"Found {len(csv_files['macro'])} macro files and {len(csv_files['micro'])} micro files")
        return csv_files

    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize column names using the column mappings

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with standardized column names
        """
        df_copy = df.copy()
        original_columns = df_copy.columns.tolist()

        # Create reverse mapping for faster lookup
        reverse_mapping = {}
        for standard_name, variations in self.column_mappings.items():
            for variation in variations:
                reverse_mapping[variation.lower()] = standard_name

        # Rename columns
        new_columns = []
        for col in original_columns:
            standard_name = reverse_mapping.get(col.lower(), col.lower())
            new_columns.append(standard_name)

        df_copy.columns = new_columns
        return df_copy

    def extract_metadata_from_path(self, file_path: Path) -> Dict[str, str]:
        """
        Extract metadata (year, state, district, block) from file path

        Args:
            file_path: Path to CSV file

        Returns:
            Dictionary with metadata
        """
        parts = file_path.parts
        metadata = {}

        try:
            # Find the raw data index
            raw_index = None
            for i, part in enumerate(parts):
                if part == 'raw':
                    raw_index = i
                    break

            if raw_index is not None and len(parts) > raw_index + 4:
                metadata['year'] = parts[raw_index + 1]
                metadata['state'] = parts[raw_index + 2].replace('_', ' ').replace('-', '/')
                metadata['district'] = parts[raw_index + 3].replace('_', ' ').replace('-', '/')

                # Extract block name from filename (remove nutrient type suffix)
                filename = file_path.stem
                block_name = re.sub(r'_(macro|micro)nutrient$', '', filename, flags=re.IGNORECASE)
                block_name = re.sub(r'_(macro|micro)$', '', block_name, flags=re.IGNORECASE)
                metadata['block'] = block_name.replace('_', ' ').replace('-', '/')

        except (IndexError, AttributeError) as e:
            self.logger.warning(f"Could not extract metadata from path {file_path}: {e}")

        return metadata

    def clean_numeric_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and standardize numeric columns

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with cleaned numeric columns
        """
        df_copy = df.copy()

        # Define numeric columns that should be cleaned
        numeric_columns = ['ph', 'ec', 'oc', 'nitrogen', 'phosphorus', 'potassium',
                           'iron', 'manganese', 'copper', 'zinc', 'boron', 'sulphur', 'area_hectare']

        for col in numeric_columns:
            if col in df_copy.columns:
                # Convert to string first to handle mixed types
                df_copy[col] = df_copy[col].astype(str)

                # Remove common non-numeric characters
                df_copy[col] = df_copy[col].str.replace(r'[^\d.-]', '', regex=True)
                df_copy[col] = df_copy[col].str.replace(r'--+', '', regex=True)  # Remove multiple dashes

                # Replace empty strings and common missing value indicators
                df_copy[col] = df_copy[col].replace(['', '-', 'NA', 'N/A', 'null', 'NULL', 'None'], np.nan)

                # Convert to numeric
                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

                # Handle outliers (values that are clearly wrong)
                if col == 'ph':
                    df_copy[col] = df_copy[col].where((df_copy[col] >= 0) & (df_copy[col] <= 14))
                elif col == 'area_hectare':
                    df_copy[col] = df_copy[col].where(df_copy[col] >= 0)
                elif col in ['nitrogen', 'phosphorus', 'potassium', 'iron', 'manganese', 'copper', 'zinc', 'boron',
                             'sulphur']:
                    df_copy[col] = df_copy[col].where(df_copy[col] >= 0)  # Nutrients should be non-negative

        return df_copy

    def process_single_file(self, file_path: Path, nutrient_type: str) -> Optional[pd.DataFrame]:
        """
        Process a single CSV file

        Args:
            file_path: Path to CSV file
            nutrient_type: Type of nutrient data ('macro' or 'micro')

        Returns:
            Processed DataFrame or None if processing failed
        """
        try:
            # Read CSV with error handling
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='latin-1')
            except Exception as e:
                self.logger.error(f"Could not read {file_path}: {e}")
                return None

            if df.empty:
                self.logger.warning(f"Empty file: {file_path}")
                return None

            # Standardize column names
            df = self.standardize_column_names(df)

            # Extract metadata from file path
            metadata = self.extract_metadata_from_path(file_path)

            # Add metadata columns
            for key, value in metadata.items():
                df[key] = value

            # Add nutrient type
            df['nutrient_type'] = nutrient_type

            # Clean numeric columns
            df = self.clean_numeric_columns(df)

            # Add source file information
            df['source_file'] = str(file_path.relative_to(self.raw_data_path))
            df['processed_date'] = datetime.now().isoformat()

            self.logger.info(f"Processed {file_path}: {len(df)} rows")
            return df

        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {e}")
            return None

    def combine_macro_micro_data(self, macro_df: pd.DataFrame, micro_df: pd.DataFrame) -> pd.DataFrame:
        """
        Combine macro and micro nutrient data based on common fields

        Args:
            macro_df: DataFrame with macro nutrient data
            micro_df: DataFrame with micro nutrient data

        Returns:
            Combined DataFrame
        """
        # Define key columns for merging
        key_columns = ['year', 'state', 'district', 'block', 'village', 'farmer_name', 'sample_id']

        # Find common key columns that exist in both dataframes
        available_keys = []
        for key in key_columns:
            if key in macro_df.columns and key in micro_df.columns:
                available_keys.append(key)

        if not available_keys:
            self.logger.warning("No common key columns found for merging macro and micro data")
            # If no common keys, concatenate vertically
            combined_df = pd.concat([macro_df, micro_df], ignore_index=True, sort=False)
        else:
            self.logger.info(f"Merging on columns: {available_keys}")

            # Perform outer join to keep all records
            combined_df = pd.merge(
                macro_df, micro_df,
                on=available_keys,
                how='outer',
                suffixes=('_macro', '_micro')
            )

            # Resolve conflicts in duplicate columns
            for col in combined_df.columns:
                if col.endswith('_macro') or col.endswith('_micro'):
                    base_col = col.replace('_macro', '').replace('_micro', '')
                    if base_col not in available_keys:  # Don't touch key columns
                        macro_col = f"{base_col}_macro"
                        micro_col = f"{base_col}_micro"

                        if macro_col in combined_df.columns and micro_col in combined_df.columns:
                            # Combine values, preferring non-null values
                            combined_df[base_col] = combined_df[macro_col].fillna(combined_df[micro_col])
                            # Drop the suffixed columns
                            combined_df = combined_df.drop([macro_col, micro_col], axis=1)

        return combined_df

    def generate_summary_statistics(self, df: pd.DataFrame) -> Dict:
        """
        Generate summary statistics for the consolidated data

        Args:
            df: Consolidated DataFrame

        Returns:
            Dictionary with summary statistics
        """
        stats = {
            'total_records': len(df),
            'total_unique_farmers': df['farmer_name'].nunique() if 'farmer_name' in df.columns else 0,
            'years_covered': sorted(df['year'].unique().tolist()) if 'year' in df.columns else [],
            'states_covered': sorted(df['state'].unique().tolist()) if 'state' in df.columns else [],
            'districts_covered': df['district'].nunique() if 'district' in df.columns else 0,
            'blocks_covered': df['block'].nunique() if 'block' in df.columns else 0,
            'missing_data_summary': {}
        }

        # Calculate missing data percentages
        for col in df.columns:
            missing_pct = (df[col].isnull().sum() / len(df)) * 100
            if missing_pct > 0:
                stats['missing_data_summary'][col] = round(missing_pct, 2)

        # Nutrient value ranges
        nutrient_cols = ['ph', 'ec', 'oc', 'nitrogen', 'phosphorus', 'potassium',
                         'iron', 'manganese', 'copper', 'zinc', 'boron', 'sulphur']

        stats['nutrient_ranges'] = {}
        for col in nutrient_cols:
            if col in df.columns and df[col].notna().any():
                stats['nutrient_ranges'][col] = {
                    'min': float(df[col].min()),
                    'max': float(df[col].max()),
                    'mean': float(df[col].mean()),
                    'median': float(df[col].median())
                }

        return stats

    def consolidate_all_data(self) -> Tuple[pd.DataFrame, Dict]:
        """
        Main method to consolidate all data

        Returns:
            Tuple of (consolidated DataFrame, summary statistics)
        """
        self.logger.info("Starting data consolidation process...")

        # Find all CSV files
        csv_files = self.find_csv_files()

        if not csv_files['macro'] and not csv_files['micro']:
            self.logger.error("No CSV files found in the raw data directory")
            return pd.DataFrame(), {}

        # Process macro nutrient files
        macro_dataframes = []
        for file_path, nutrient_type in csv_files['macro']:
            df = self.process_single_file(file_path, nutrient_type)
            if df is not None:
                macro_dataframes.append(df)

        # Process micro nutrient files
        micro_dataframes = []
        for file_path, nutrient_type in csv_files['micro']:
            df = self.process_single_file(file_path, nutrient_type)
            if df is not None:
                micro_dataframes.append(df)

        # Combine all macro and micro dataframes separately
        if macro_dataframes:
            macro_combined = pd.concat(macro_dataframes, ignore_index=True, sort=False)
            self.logger.info(f"Combined macro data: {len(macro_combined)} rows")
        else:
            macro_combined = pd.DataFrame()

        if micro_dataframes:
            micro_combined = pd.concat(micro_dataframes, ignore_index=True, sort=False)
            self.logger.info(f"Combined micro data: {len(micro_combined)} rows")
        else:
            micro_combined = pd.DataFrame()

        # Combine macro and micro data
        if not macro_combined.empty and not micro_combined.empty:
            final_df = self.combine_macro_micro_data(macro_combined, micro_combined)
        elif not macro_combined.empty:
            final_df = macro_combined
        elif not micro_combined.empty:
            final_df = micro_combined
        else:
            final_df = pd.DataFrame()

        if final_df.empty:
            self.logger.error("No data was successfully processed")
            return pd.DataFrame(), {}

        # Apply final data type conversions
        for col, dtype in self.column_dtypes.items():
            if col in final_df.columns:
                try:
                    if dtype == 'str':
                        final_df[col] = final_df[col].astype(str)
                    elif dtype == 'float64':
                        final_df[col] = pd.to_numeric(final_df[col], errors='coerce')
                except Exception as e:
                    self.logger.warning(f"Could not convert column {col} to {dtype}: {e}")

        # Generate summary statistics
        summary_stats = self.generate_summary_statistics(final_df)

        self.logger.info(f"Data consolidation completed: {len(final_df)} total records")
        return final_df, summary_stats

    def save_consolidated_data(self, df: pd.DataFrame, summary_stats: Dict):
        """
        Save the consolidated data and summary statistics

        Args:
            df: Consolidated DataFrame
            summary_stats: Summary statistics dictionary
        """
        if df.empty:
            self.logger.error("Cannot save empty DataFrame")
            return

        # Save main consolidated file
        consolidated_file = self.processed_data_path / 'soil_health_consolidated.csv'
        df.to_csv(consolidated_file, index=False)
        self.logger.info(f"Saved consolidated data to: {consolidated_file}")

        # Save summary statistics
        summary_file = self.processed_data_path / 'consolidation_summary.txt'
        with open(summary_file, 'w') as f:
            f.write("SOIL HEALTH DATA CONSOLIDATION SUMMARY\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write(f"Total Records: {summary_stats['total_records']:,}\n")
            f.write(f"Unique Farmers: {summary_stats['total_unique_farmers']:,}\n")
            f.write(f"Years Covered: {', '.join(map(str, summary_stats['years_covered']))}\n")
            f.write(f"States Covered: {len(summary_stats['states_covered'])}\n")
            f.write(f"Districts Covered: {summary_stats['districts_covered']:,}\n")
            f.write(f"Blocks Covered: {summary_stats['blocks_covered']:,}\n\n")

            if summary_stats['missing_data_summary']:
                f.write("MISSING DATA SUMMARY (% missing):\n")
                f.write("-" * 30 + "\n")
                for col, pct in sorted(summary_stats['missing_data_summary'].items()):
                    f.write(f"{col}: {pct}%\n")
                f.write("\n")

            if summary_stats['nutrient_ranges']:
                f.write("NUTRIENT VALUE RANGES:\n")
                f.write("-" * 30 + "\n")
                for nutrient, ranges in summary_stats['nutrient_ranges'].items():
                    f.write(f"{nutrient.upper()}:\n")
                    f.write(f"  Min: {ranges['min']:.2f}\n")
                    f.write(f"  Max: {ranges['max']:.2f}\n")
                    f.write(f"  Mean: {ranges['mean']:.2f}\n")
                    f.write(f"  Median: {ranges['median']:.2f}\n\n")

        self.logger.info(f"Saved summary statistics to: {summary_file}")

        # Save separate files by year for easier analysis
        if 'year' in df.columns:
            for year in df['year'].unique():
                if pd.notna(year):
                    year_df = df[df['year'] == year]
                    year_file = self.processed_data_path / f'soil_health_{year}.csv'
                    year_df.to_csv(year_file, index=False)
                    self.logger.info(f"Saved {year} data to: {year_file}")


def main():
    """Main function to run the consolidation process"""
    print("🚀 Starting Soil Health Data Consolidation...")

    # Initialize consolidator
    consolidator = SoilDataConsolidator()

    # Consolidate all data
    consolidated_df, summary_stats = consolidator.consolidate_all_data()

    if not consolidated_df.empty:
        # Save consolidated data
        consolidator.save_consolidated_data(consolidated_df, summary_stats)

        print("\n✅ Consolidation Summary:")
        print(f"   📊 Total Records: {summary_stats['total_records']:,}")
        print(f"   👥 Unique Farmers: {summary_stats['total_unique_farmers']:,}")
        print(f"   📅 Years: {', '.join(map(str, summary_stats['years_covered']))}")
        print(f"   🏛️  States: {len(summary_stats['states_covered'])}")
        print(f"   📍 Districts: {summary_stats['districts_covered']:,}")
        print(f"   🏘️  Blocks: {summary_stats['blocks_covered']:,}")

        print(f"\n📁 Files saved in: data/processed/")
        print("   - soil_health_consolidated.csv (main file)")
        print("   - consolidation_summary.txt (statistics)")
        print("   - soil_health_YYYY.csv (yearly files)")
        print("   - consolidation.log (processing log)")

    else:
        print("❌ No data was consolidated. Check the logs for details.")

    print("\n🎉 Consolidation process completed!")


if __name__ == "__main__":
    main()