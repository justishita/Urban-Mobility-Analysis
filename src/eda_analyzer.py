import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from config import Config

class EDAAnalyzer:
    
    def __init__(self):
        self.config = Config()
    
    def analyze_city_data(self, city):
        """Perform comprehensive EDA for a city"""
        print(f"\nPerforming EDA for {city}...")
        
        eda_results = {
            'city': city,
            'file_analysis': {},
            'data_quality': {},
            'missing_values': {}
        }
        
        city_path = self.config.get_city_data_path(city)
        gtfs_files = [f for f in os.listdir(city_path) if f.endswith('.txt')]
        
        for file in gtfs_files:
            file_type = file.replace('.txt', '')
            print(f"  Analyzing {file_type}...")
            
            file_analysis = self._analyze_file(city, file_type)
            eda_results['file_analysis'][file_type] = file_analysis
            eda_results['missing_values'][file_type] = self._analyze_missing_values(file_analysis['df'])
            eda_results['data_quality'][file_type] = self._analyze_data_quality(file_analysis['df'])
        
        self._generate_eda_report(eda_results, city)
        self._create_eda_visualizations(eda_results, city)
        
        return eda_results
    
    def _analyze_file(self, city, file_type):
        """Analyze a single GTFS file"""
        file_path = os.path.join(self.config.get_city_data_path(city), f"{file_type}.txt")
        
        try:
            df = pd.read_csv(file_path, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='latin-1')
        
        analysis = {
            'file_path': file_path,
            'records': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'data_types': df.dtypes.astype(str).to_dict(),
            'df': df,  # Keep for further analysis
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'duplicates': df.duplicated().sum()
        }
        
        # Basic statistics for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            analysis['numeric_stats'] = df[numeric_cols].describe().to_dict()
        
        # Basic statistics for categorical columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            analysis['categorical_stats'] = {}
            for col in categorical_cols[:5]:  # Limit to first 5 to avoid too much output
                analysis['categorical_stats'][col] = df[col].value_counts().head(10).to_dict()
        
        return analysis
    
    def _analyze_missing_values(self, df):
        """Analyze missing values in DataFrame"""
        missing = df.isnull().sum()
        missing_pct = (missing / len(df)) * 100
        
        return {
            'total_missing': missing.sum(),
            'missing_by_column': missing[missing > 0].to_dict(),
            'missing_pct_by_column': missing_pct[missing_pct > 0].to_dict(),
            'columns_with_missing': list(missing[missing > 0].index)
        }
    
    def _analyze_data_quality(self, df):
        """Analyze data quality issues"""
        quality_issues = {}
        
        # Check for negative values in numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if (df[col] < 0).any():
                quality_issues[f'negative_values_{col}'] = (df[col] < 0).sum()
        
        # Check for zeros in numeric columns (where not expected)
        for col in numeric_cols:
            if (df[col] == 0).any() and col not in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
                quality_issues[f'zero_values_{col}'] = (df[col] == 0).sum()
        
        # Check for extreme values
        for col in numeric_cols:
            if df[col].dtype in ['float64', 'int64']:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                outliers = ((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR))).sum()
                if outliers > 0:
                    quality_issues[f'outliers_{col}'] = outliers
        
        return quality_issues
    
    def _generate_eda_report(self, eda_results, city):
        """Generate EDA report"""
        report_path = f"outputs/data/{city}_eda_report.txt"
        
        with open(report_path, 'w') as f:
            f.write(f"EDA Report - {city.upper()}\n")
            f.write("=" * 50 + "\n\n")
            
            for file_type, analysis in eda_results['file_analysis'].items():
                f.write(f"{file_type.upper()} ANALYSIS\n")
                f.write(f"Records: {analysis['records']:,}\n")
                f.write(f"Columns: {analysis['columns']}\n")
                f.write(f"Memory: {analysis['memory_usage_mb']:.2f} MB\n")
                f.write(f"Duplicates: {analysis['duplicates']}\n\n")
                
                # Missing values
                missing_info = eda_results['missing_values'][file_type]
                if missing_info['columns_with_missing']:
                    f.write(" MISSING VALUES:\n")
                    for col in missing_info['columns_with_missing']:
                        f.write(f"  {col}: {missing_info['missing_by_column'][col]:,} ({missing_info['missing_pct_by_column'][col]:.1f}%)\n")
                else:
                    f.write(" No missing values\n")
                
                # Data quality issues
                quality_issues = eda_results['data_quality'][file_type]
                if quality_issues:
                    f.write(" DATA QUALITY ISSUES:\n")
                    for issue, count in quality_issues.items():
                        f.write(f"  {issue}: {count:,}\n")
                else:
                    f.write(" No data quality issues\n")
                
                f.write("\n" + "-" * 30 + "\n\n")
        
        print(f"EDA report saved: {report_path}")
    
    def _create_eda_visualizations(self, eda_results, city):
        """Create EDA visualizations"""
        try:
            # 1. Missing values heatmap
            self._plot_missing_values_heatmap(eda_results, city)
            
            # 2. File sizes comparison
            self._plot_file_sizes_comparison(eda_results, city)
            
            # 3. Data quality issues summary
            self._plot_data_quality_issues(eda_results, city)
            
        except Exception as e:
            print(f"Error creating EDA visualizations: {e}")
    
    def _plot_missing_values_heatmap(self, eda_results, city):
        """Create missing values heatmap"""
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        # Prepare data for heatmap
        missing_data = []
        for file_type, analysis in eda_results['file_analysis'].items():
            df = analysis['df']
            missing_pct = (df.isnull().sum() / len(df)) * 100
            for col, pct in missing_pct.items():
                if pct > 0:  # Only show columns with missing values
                    missing_data.append({
                        'file_type': file_type,
                        'column': col,
                        'missing_pct': pct
                    })
        
        if not missing_data:
            print(f"No missing values to visualize for {city}")
            return
        
        missing_df = pd.DataFrame(missing_data)
        pivot_df = missing_df.pivot(index='file_type', columns='column', values='missing_pct')
        
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_df, annot=True, fmt='.1f', cmap='Reds', cbar_kws={'label': 'Missing %'})
        plt.title(f'Missing Values Analysis - {city.title()}')
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_missing_values_heatmap.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f" Saved missing values heatmap: outputs/visuals/{city}_missing_values_heatmap.png")
    
    def _plot_file_sizes_comparison(self, eda_results, city):
        """Create file sizes comparison chart"""
        import matplotlib.pyplot as plt
        
        file_sizes = []
        for file_type, analysis in eda_results['file_analysis'].items():
            file_sizes.append({
                'file_type': file_type,
                'records': analysis['records'],
                'memory_mb': analysis['memory_usage_mb']
            })
        
        files_df = pd.DataFrame(file_sizes)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        # Records per file
        ax1.bar(files_df['file_type'], files_df['records'], color='skyblue')
        ax1.set_title(f'Records per File - {city.title()}')
        ax1.set_xlabel('File Type')
        ax1.set_ylabel('Number of Records')
        ax1.tick_params(axis='x', rotation=45)
        
        # Memory usage per file
        ax2.bar(files_df['file_type'], files_df['memory_mb'], color='lightgreen')
        ax2.set_title(f'Memory Usage per File - {city.title()}')
        ax2.set_xlabel('File Type')
        ax2.set_ylabel('Memory (MB)')
        ax2.tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_file_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved file analysis: outputs/visuals/{city}_file_analysis.png")
    
    def _plot_data_quality_issues(self, eda_results, city):
        """Create data quality issues summary"""
        import matplotlib.pyplot as plt
        
        quality_issues = []
        for file_type, issues in eda_results['data_quality'].items():
            for issue, count in issues.items():
                quality_issues.append({
                    'file_type': file_type,
                    'issue': issue,
                    'count': count
                })
        
        if not quality_issues:
            print(f"    ✅ No data quality issues to visualize for {city}")
            return
        
        issues_df = pd.DataFrame(quality_issues)
        
        plt.figure(figsize=(12, 6))
        issues_df.groupby(['file_type', 'issue'])['count'].sum().unstack().plot(
            kind='bar', stacked=True, figsize=(12, 6)
        )
        plt.title(f'Data Quality Issues - {city.title()}')
        plt.xlabel('File Type')
        plt.ylabel('Number of Issues')
        plt.legend(title='Issue Type', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_data_quality_issues.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"Saved data quality issues: outputs/visuals/{city}_data_quality_issues.png")