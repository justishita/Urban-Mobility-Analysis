import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
from config import Config

class AnalysisEngine:
    """Perform comprehensive data analysis and visualization"""
    
    def __init__(self):
        self.config = Config()
    
    def load_analysis_data(self, city):
        """Load processed data for analysis"""
        data_files = {
            'trip_stop_events': f'outputs/data/{city}_trip_stop_events.csv',
            'route_stats': f'outputs/data/{city}_route_stats.csv',
            'headways': f'outputs/data/{city}_headways.csv',
            'hourly_service': f'outputs/data/{city}_hourly_service.csv',
            'spatial_stats': f'outputs/data/{city}_spatial_stats.csv'
        }
        
        loaded_data = {}
        for name, file_path in data_files.items():
            if os.path.exists(file_path):
                try:
                    loaded_data[name] = pd.read_csv(file_path)
                    print(f"  📁 Loaded {name}: {len(loaded_data[name])} records")
                except Exception as e:
                    print(f"  ❌ Error loading {name}: {e}")
            else:
                print(f"  ⚠️  File not found: {file_path}")
        
        return loaded_data
    
    def perform_comprehensive_analysis(self, city):
        """Perform comprehensive analysis for a city"""
        print(f"\n📊 Performing comprehensive analysis for {city}...")
        
        data = self.load_analysis_data(city)
        
        if not data:
            print(f"❌ No analysis data found for {city}")
            return None
        
        analysis_results = {
            'city': city, 
            'timestamp': datetime.now().isoformat(),
            'summary': {}
        }
        
        # Basic statistics
        if 'route_stats' in data:
            analysis_results['route_analysis'] = self._analyze_routes(data['route_stats'])
        
        # Headway analysis
        if 'headways' in data and 'headway_seconds' in data['headways'].columns:
            analysis_results['headway_analysis'] = self._analyze_headways(data['headways'])
        
        # Service patterns
        if 'hourly_service' in data:
            analysis_results['service_analysis'] = self._analyze_service_patterns(data['hourly_service'])
        
        # Spatial analysis
        if 'spatial_stats' in data:
            analysis_results['spatial_analysis'] = self._analyze_spatial_patterns(data['spatial_stats'])
        
        # Generate summary
        analysis_results['summary'] = self._generate_summary(analysis_results)
        
        self._print_analysis_summary(analysis_results, city)
        return analysis_results
    
    def _analyze_routes(self, route_stats):
        """Analyze route-level statistics"""
        analysis = {
            'total_routes': len(route_stats),
            'avg_trips_per_route': route_stats['num_trips'].mean(),
            'median_trips_per_route': route_stats['num_trips'].median(),
            'avg_stops_per_route': route_stats['num_stops'].mean(),
            'total_stop_events': route_stats['total_stop_events'].sum(),
            'busiest_route': {
                'route_id': route_stats.loc[route_stats['num_trips'].idxmax(), 'route_id'],
                'trips': route_stats['num_trips'].max()
            },
            'route_distribution': {
                'high_frequency': len(route_stats[route_stats['num_trips'] > 100]),
                'medium_frequency': len(route_stats[(route_stats['num_trips'] >= 50) & (route_stats['num_trips'] <= 100)]),
                'low_frequency': len(route_stats[route_stats['num_trips'] < 50])
            }
        }
        return analysis
    
    def _analyze_headways(self, headways):
        """Analyze headway patterns"""
        headways_min = headways['headway_seconds'] / 60
        
        analysis = {
            'avg_headway_min': headways_min.mean(),
            'median_headway_min': headways_min.median(),
            'std_headway_min': headways_min.std(),
            'min_headway_min': headways_min.min(),
            'max_headway_min': headways_min.max(),
            'p95_headway_min': headways_min.quantile(0.95),
            'p05_headway_min': headways_min.quantile(0.05),
            'total_headways_analyzed': len(headways_min),
            'reliability_metrics': {
                'on_time_percentage': len(headways_min[headways_min <= 15]) / len(headways_min) * 100,
                'high_frequency_percentage': len(headways_min[headways_min <= 5]) / len(headways_min) * 100
            }
        }
        return analysis
    
    def _analyze_service_patterns(self, hourly_service):
        """Analyze service patterns throughout the day"""
        hourly_agg = hourly_service.groupby('arrival_hour').agg({
            'hourly_trips': 'sum',
            'hourly_stop_events': 'sum'
        }).reset_index()
        
        analysis = {
            'peak_hour': hourly_agg.loc[hourly_agg['hourly_trips'].idxmax(), 'arrival_hour'],
            'peak_hour_trips': hourly_agg['hourly_trips'].max(),
            'off_peak_hour': hourly_agg.loc[hourly_agg['hourly_trips'].idxmin(), 'arrival_hour'],
            'total_service_hours': hourly_agg['arrival_hour'].nunique(),
            'daily_trip_pattern': hourly_agg[['arrival_hour', 'hourly_trips']].to_dict('records'),
            'service_gaps': self._find_service_gaps(hourly_agg)
        }
        return analysis
    
    def _analyze_spatial_patterns(self, spatial_stats):
        """Analyze spatial distribution of stops"""
        analysis = {
            'total_stops_analyzed': len(spatial_stats),
            'avg_routes_per_stop': spatial_stats['num_routes'].mean(),
            'max_routes_per_stop': spatial_stats['num_routes'].max(),
            'major_hubs': len(spatial_stats[spatial_stats['num_routes'] >= 5]),
            'spread_metrics': {
                'lat_range': [spatial_stats['avg_lat'].min(), spatial_stats['avg_lat'].max()],
                'lon_range': [spatial_stats['avg_lon'].min(), spatial_stats['avg_lon'].max()]
            }
        }
        return analysis
    
    def _find_service_gaps(self, hourly_agg):
        """Identify service gaps in hourly service"""
        gaps = []
        for hour in range(24):
            if hour not in hourly_agg['arrival_hour'].values:
                gaps.append(hour)
            elif hourly_agg[hourly_agg['arrival_hour'] == hour]['hourly_trips'].iloc[0] == 0:
                gaps.append(hour)
        return gaps
    
    def _generate_summary(self, analysis_results):
        """Generate executive summary"""
        summary = {
            'overall_score': 0,
            'key_strengths': [],
            'improvement_areas': []
        }
        
        if 'route_analysis' in analysis_results:
            route_analysis = analysis_results['route_analysis']
            summary['total_routes'] = route_analysis['total_routes']
            summary['total_trips_estimate'] = route_analysis['total_stop_events']
            
            if route_analysis['avg_trips_per_route'] > 50:
                summary['key_strengths'].append("High frequency service coverage")
            else:
                summary['improvement_areas'].append("Consider increasing service frequency")
        
        if 'headway_analysis' in analysis_results:
            headway_analysis = analysis_results['headway_analysis']
            if headway_analysis['avg_headway_min'] < 10:
                summary['key_strengths'].append("Good headway consistency")
            else:
                summary['improvement_areas'].append("Improve headway consistency")
        
        return summary
    
    def _print_analysis_summary(self, analysis_results, city):
        """Print analysis summary to console"""
        print(f"\n📋 ANALYSIS SUMMARY - {city.upper()}")
        print("=" * 50)
        
        if 'route_analysis' in analysis_results:
            ra = analysis_results['route_analysis']
            print(f"🚌 Route Analysis:")
            print(f"   • Total routes: {ra['total_routes']}")
            print(f"   • Average trips per route: {ra['avg_trips_per_route']:.1f}")
            print(f"   • Busiest route: {ra['busiest_route']['route_id']} ({ra['busiest_route']['trips']} trips)")
        
        if 'headway_analysis' in analysis_results:
            ha = analysis_results['headway_analysis']
            print(f"⏱️  Headway Analysis:")
            print(f"   • Average headway: {ha['avg_headway_min']:.1f} minutes")
            print(f"   • On-time performance: {ha['reliability_metrics']['on_time_percentage']:.1f}%")
        
        if 'service_analysis' in analysis_results:
            sa = analysis_results['service_analysis']
            print(f"🕒 Service Patterns:")
            print(f"   • Peak hour: {sa['peak_hour']}:00 ({sa['peak_hour_trips']} trips)")
            print(f"   • Service gaps: {len(sa['service_gaps'])} hours")
    
    def create_comprehensive_visualizations(self, city, data):
        """Create comprehensive visualizations"""
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns
            
            print(f"  🎨 Creating visualizations for {city}...")
            
            plt.style.use('default')
            sns.set_palette("husl")
            
            # Create multiple visualization types
            if 'route_stats' in data:
                self._plot_route_analysis(city, data['route_stats'])
            
            if 'headways' in data and 'headway_seconds' in data['headways'].columns:
                self._plot_headway_analysis(city, data['headways'])
            
            if 'hourly_service' in data:
                self._plot_service_analysis(city, data['hourly_service'])
            
            if 'spatial_stats' in data:
                self._plot_spatial_analysis(city, data['spatial_stats'])
                
            print(f"  ✅ All visualizations created for {city}")
                
        except ImportError:
            print("  ⚠️  matplotlib/seaborn not available for visualizations")
        except Exception as e:
            print(f"  ❌ Error creating visualizations: {e}")
    
    def _plot_route_analysis(self, city, route_stats):
        """Plot route analysis charts"""
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle(f'Route Analysis - {city.title()}', fontsize=16, fontweight='bold')
        
        # Trips per route distribution
        axes[0,0].hist(route_stats['num_trips'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        axes[0,0].set_title('Trips per Route Distribution')
        axes[0,0].set_xlabel('Number of Trips')
        axes[0,0].set_ylabel('Frequency')
        axes[0,0].grid(True, alpha=0.3)
        
        # Stops per route distribution
        axes[0,1].hist(route_stats['num_stops'], bins=30, alpha=0.7, color='lightgreen', edgecolor='black')
        axes[0,1].set_title('Stops per Route Distribution')
        axes[0,1].set_xlabel('Number of Stops')
        axes[0,1].set_ylabel('Frequency')
        axes[0,1].grid(True, alpha=0.3)
        
        # Top 10 busiest routes
        top_routes = route_stats.nlargest(10, 'num_trips')
        axes[1,0].bar(range(len(top_routes)), top_routes['num_trips'], color='coral', alpha=0.7)
        axes[1,0].set_title('Top 10 Busiest Routes')
        axes[1,0].set_xlabel('Route')
        axes[1,0].set_ylabel('Number of Trips')
        axes[1,0].set_xticks(range(len(top_routes)))
        axes[1,0].set_xticklabels(top_routes['route_short_name'].fillna(top_routes['route_id']), rotation=45)
        axes[1,0].grid(True, alpha=0.3)
        
        # Route frequency categories
        freq_categories = ['Low (<50)', 'Medium (50-100)', 'High (>100)']
        freq_counts = [
            len(route_stats[route_stats['num_trips'] < 50]),
            len(route_stats[(route_stats['num_trips'] >= 50) & (route_stats['num_trips'] <= 100)]),
            len(route_stats[route_stats['num_trips'] > 100])
        ]
        axes[1,1].pie(freq_counts, labels=freq_categories, autopct='%1.1f%%', startangle=90)
        axes[1,1].set_title('Route Frequency Distribution')
        
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_route_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"    ✅ Saved route analysis: outputs/visuals/{city}_route_analysis.png")
    
    def _plot_headway_analysis(self, city, headways):
        """Plot headway analysis charts"""
        import matplotlib.pyplot as plt
        
        headways_min = headways['headway_seconds'] / 60
        headways_filtered = headways_min[headways_min < 60]  # Filter extreme values
        
        fig, axes = plt.subplots(1, 3, figsize=(18, 5))
        fig.suptitle(f'Headway Analysis - {city.title()}', fontsize=16, fontweight='bold')
        
        # Headway distribution
        axes[0].hist(headways_filtered, bins=50, alpha=0.7, color='purple', edgecolor='black')
        axes[0].set_title('Headway Distribution')
        axes[0].set_xlabel('Headway (minutes)')
        axes[0].set_ylabel('Frequency')
        axes[0].grid(True, alpha=0.3)
        
        # Headway box plot
        axes[1].boxplot(headways_filtered, vert=True, patch_artist=True)
        axes[1].set_title('Headway Statistics')
        axes[1].set_ylabel('Minutes')
        axes[1].grid(True, alpha=0.3)
        
        # Headway cumulative distribution
        axes[2].hist(headways_filtered, bins=50, cumulative=True, density=True, 
                    alpha=0.7, color='orange', edgecolor='black')
        axes[2].set_title('Cumulative Headway Distribution')
        axes[2].set_xlabel('Headway (minutes)')
        axes[2].set_ylabel('Cumulative Probability')
        axes[2].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_headway_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"    ✅ Saved headway analysis: outputs/visuals/{city}_headway_analysis.png")
    
    def _plot_service_analysis(self, city, hourly_service):
        """Plot service pattern analysis"""
        import matplotlib.pyplot as plt
        
        hourly_agg = hourly_service.groupby('arrival_hour').agg({
            'hourly_trips': 'sum',
            'hourly_stop_events': 'sum'
        }).reset_index()
        
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
        fig.suptitle(f'Service Pattern Analysis - {city.title()}', fontsize=16, fontweight='bold')
        
        # Hourly trips
        axes[0].plot(hourly_agg['arrival_hour'], hourly_agg['hourly_trips'], 
                    marker='o', linewidth=2, markersize=6, color='teal', label='Trips')
        axes[0].set_title('Hourly Service Pattern')
        axes[0].set_xlabel('Hour of Day')
        axes[0].set_ylabel('Number of Trips')
        axes[0].grid(True, alpha=0.3)
        axes[0].set_xticks(range(0, 24))
        axes[0].legend()
        
        # Hourly stop events
        axes[1].plot(hourly_agg['arrival_hour'], hourly_agg['hourly_stop_events'], 
                    marker='s', linewidth=2, markersize=6, color='red', label='Stop Events')
        axes[1].set_title('Hourly Stop Events')
        axes[1].set_xlabel('Hour of Day')
        axes[1].set_ylabel('Number of Stop Events')
        axes[1].grid(True, alpha=0.3)
        axes[1].set_xticks(range(0, 24))
        axes[1].legend()
        
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_service_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"    ✅ Saved service patterns: outputs/visuals/{city}_service_patterns.png")
    
    def _plot_spatial_analysis(self, city, spatial_stats):
        """Plot spatial analysis charts"""
        import matplotlib.pyplot as plt
        
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
        fig.suptitle(f'Spatial Analysis - {city.title()}', fontsize=16, fontweight='bold')
        
        # Routes per stop distribution
        axes[0].hist(spatial_stats['num_routes'], bins=20, alpha=0.7, color='green', edgecolor='black')
        axes[0].set_title('Routes per Stop Distribution')
        axes[0].set_xlabel('Number of Routes')
        axes[0].set_ylabel('Frequency')
        axes[0].grid(True, alpha=0.3)
        
        # Top transfer points
        top_stops = spatial_stats.nlargest(10, 'num_routes')
        axes[1].bar(range(len(top_stops)), top_stops['num_routes'], color='orange', alpha=0.7)
        axes[1].set_title('Top 10 Transfer Points')
        axes[1].set_xlabel('Stop')
        axes[1].set_ylabel('Number of Routes')
        axes[1].set_xticks(range(len(top_stops)))
        axes[1].set_xticklabels(top_stops['stop_name'].str[:20], rotation=45)
        axes[1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_spatial_analysis.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"    ✅ Saved spatial analysis: outputs/visuals/{city}_spatial_analysis.png")