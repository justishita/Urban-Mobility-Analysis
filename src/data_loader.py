import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from config import Config

class DataLoader:
    """Handle GTFS file loading, validation, and basic data cleaning"""
    
    @staticmethod
    def validate_gtfs_files(city):
        """Check if required GTFS files exist and have content"""
        city_path = Config.get_city_data_path(city)
        required_files = Config.get_required_files(city)
        
        if not required_files:
            print(f"No required files defined for city: {city}")
            return False
        
        missing_files = []
        empty_files = []
        
        print(f"Validating GTFS files for {city}...")
        
        for file in required_files:
            file_path = os.path.join(city_path, file)
            
            if not os.path.exists(file_path):
                missing_files.append(file)
                continue
            
            try:
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                    empty_files.append(file)
                    print(f"File is empty: {file}")
                else:
                    print(f"{file} - {file_size} bytes")
            except OSError as e:
                print(f"Error accessing {file}: {e}")
                empty_files.append(file)
        
        if missing_files:
            print(f"Missing files for {city}: {missing_files}")
        
        if empty_files:
            print(f"Empty files for {city}: {empty_files}")
        
        if not missing_files and not empty_files:
            print(f"All required files validated successfully for {city}!")
            return True
        else:
            return False
    
    @staticmethod
    def validate_gtfs_structure(df, file_type):
        """Validate basic GTFS file structure"""
        if df.empty:
            return False
        
        required_columns = {
            'routes': ['route_id', 'route_short_name', 'route_long_name'],
            'trips': ['trip_id', 'route_id', 'service_id'],
            'stops': ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'],
            'stop_times': ['trip_id', 'stop_id', 'stop_sequence', 'arrival_time'],
            'agency': ['agency_id', 'agency_name', 'agency_timezone'],
            'calendar': ['service_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'start_date', 'end_date']
        }
        
        if file_type in required_columns:
            missing_cols = [col for col in required_columns[file_type] if col not in df.columns]
            if missing_cols:
                print(f"Missing required columns in {file_type}: {missing_cols}")
                return False
        
        return True

    @staticmethod
    def load_gtfs_file(city, file_type):
        """Load a single GTFS file as pandas DataFrame with encoding detection"""
        file_path = os.path.join(Config.get_city_data_path(city), f"{file_type}.txt")
        
        try:
            encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, encoding=encoding)
                    print(f"Loaded {file_type} with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                print(f"Could not load {file_type} with any encoding")
                return pd.DataFrame()
            
            print(f"{file_type}: {len(df)} records, {len(df.columns)} columns")
            
            if len(df) > 0:
                print(f"Columns: {list(df.columns)}")
                print(f"Data types: {df.dtypes.to_dict()}")
                
                missing_stats = df.isnull().sum()
                if missing_stats.sum() > 0:
                    print(f"Missing values: {missing_stats[missing_stats > 0].to_dict()}")
            
            return df
            
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error loading {file_type}: {e}")
            return pd.DataFrame()
    
    @staticmethod
    def get_gtfs_file_info(city):
        """Get information about all GTFS files for a city"""
        city_path = Config.get_city_data_path(city)
        file_info = {}
        
        if not os.path.exists(city_path):
            print(f"Data directory not found: {city_path}")
            return file_info
        
        gtfs_files = [f for f in os.listdir(city_path) if f.endswith('.txt')]
        
        print(f"\nGTFS Files Information for {city}:")
        for file in gtfs_files:
            file_path = os.path.join(city_path, file)
            try:
                file_size = os.path.getsize(file_path)
                file_info[file] = {
                    'size_bytes': file_size,
                    'size_mb': round(file_size / (1024 * 1024), 2),
                    'exists': True
                }
                print(f"{file}: {file_info[file]['size_mb']} MB")
            except OSError:
                file_info[file] = {'exists': False}
                print(f"{file}: Unable to access")
        
        return file_info
    
    @staticmethod
    def clean_column_names(df):
        """Clean column names for Spark compatibility"""
        if df.empty:
            return df
        
        df.columns = [col.replace(' ', '_').replace('-', '_').lower() for col in df.columns]
        return df
    
    @staticmethod
    def get_data_quality_report(df, file_type):
        """Get comprehensive data quality report for EDA"""
        if df.empty:
            return {
                'records': 0,
                'columns': 0,
                'missing_values': {},
                'quality_issues': {},
                'memory_usage_mb': 0
            }
        
        # Missing values analysis
        missing = df.isnull().sum()
        missing_pct = (missing / len(df)) * 100
        
        # Data quality issues
        quality_issues = {}
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            # Negative values check
            if (df[col] < 0).any():
                quality_issues[f'negative_values_{col}'] = (df[col] < 0).sum()
            
            # Zero values check (where not expected)
            if (df[col] == 0).any() and col not in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
                quality_issues[f'zero_values_{col}'] = (df[col] == 0).sum()
            
            # Outliers detection
            if df[col].dtype in ['float64', 'int64']:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()
                if outliers > 0:
                    quality_issues[f'outliers_{col}'] = outliers
        
        return {
            'records': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'data_types': df.dtypes.astype(str).to_dict(),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'duplicates': df.duplicated().sum(),
            'missing_values': {
                'total_missing': missing.sum(),
                'missing_by_column': missing[missing > 0].to_dict(),
                'missing_pct_by_column': missing_pct[missing_pct > 0].to_dict(),
                'columns_with_missing': list(missing[missing > 0].index)
            },
            'quality_issues': quality_issues,
            'numeric_stats': df.select_dtypes(include=[np.number]).describe().to_dict() if len(numeric_cols) > 0 else {},
            'categorical_stats': {col: df[col].value_counts().head(10).to_dict() 
                                for col in df.select_dtypes(include=['object']).columns[:5]}  # Limit to first 5 categorical columns
        }

