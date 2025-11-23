import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
from config import Config
from data_loader import DataLoader

class EDAAnalyzer:
    def __init__(self):
        self.config = Config()
        self.data_loader = DataLoader()
    
    def analyze_city_data(self, city):
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
            print(f"Analyzing {file_type}...")
            
            # Use DataLoader to load file and get quality report
            df = self.data_loader.load_gtfs_file(city, file_type)
            quality_report = self.data_loader.get_data_quality_report(df, file_type)
            
            eda_results['file_analysis'][file_type] = {
                **quality_report,
                'df': df,  
                'file_path': os.path.join(city_path, file)
            }
            eda_results['missing_values'][file_type] = quality_report['missing_values']
            eda_results['data_quality'][file_type] = quality_report['quality_issues']
        
        self._generate_eda_report(eda_results, city)
        self._create_eda_visualizations(eda_results, city)
        
        return eda_results
    
    def _generate_eda_report(self, eda_results, city):
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
                
                missing_info = analysis['missing_values']
                if missing_info['columns_with_missing']:
                    f.write(" MISSING VALUES:\n")
                    for col in missing_info['columns_with_missing']:
                        f.write(f"  {col}: {missing_info['missing_by_column'][col]:,} ({missing_info['missing_pct_by_column'][col]:.1f}%)\n")
                else:
                    f.write(" No missing values\n")
                
                # Data quality issues from DataLoader report
                quality_issues = analysis['quality_issues']
                if quality_issues:
                    f.write(" DATA QUALITY ISSUES:\n")
                    for issue, count in quality_issues.items():
                        f.write(f"  {issue}: {count:,}\n")
                else:
                    f.write(" No data quality issues\n")
                
                f.write("\n" + "-" * 30 + "\n\n")
        
        print(f"EDA report saved: {report_path}")
    
    def _create_eda_visualizations(self, eda_results, city):
        try:
            self._plot_missing_values_heatmap(eda_results, city)
            self._plot_file_sizes_comparison(eda_results, city)
            self._plot_data_quality_issues(eda_results, city)
        except Exception as e:
            print(f"Error creating EDA visualizations: {e}")
    
    def _plot_missing_values_heatmap(self, eda_results, city):
        """Create missing values heatmap"""
        missing_data = []
        for file_type, analysis in eda_results['file_analysis'].items():
            df = analysis['df']
            missing_pct = (df.isnull().sum() / len(df)) * 100
            for col, pct in missing_pct.items():
                if pct > 0:
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
        print(f"Saved missing values heatmap: outputs/visuals/{city}_missing_values_heatmap.png")
    
    def _plot_file_sizes_comparison(self, eda_results, city):
        """Create file sizes comparison chart"""
        file_sizes = []
        for file_type, analysis in eda_results['file_analysis'].items():
            file_sizes.append({
                'file_type': file_type,
                'records': analysis['records'],
                'memory_mb': analysis['memory_usage_mb']
            })
        
        files_df = pd.DataFrame(file_sizes)
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        ax1.bar(files_df['file_type'], files_df['records'], color='skyblue')
        ax1.set_title(f'Records per File - {city.title()}')
        ax1.set_xlabel('File Type')
        ax1.set_ylabel('Number of Records')
        ax1.tick_params(axis='x', rotation=45)
        
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
        quality_issues = []
        for file_type, issues in eda_results['data_quality'].items():
            for issue, count in issues.items():
                quality_issues.append({
                    'file_type': file_type,
                    'issue': issue,
                    'count': count
                })
        
        if not quality_issues:
            print(f"No data quality issues to visualize for {city}")
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