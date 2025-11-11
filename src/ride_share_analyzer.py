import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col, count, mean, stddev, hour, dayofweek, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from config import Config
from pymongo import MongoClient

class RideShareAnalyzer:
    """Analyze ride-sharing data for patterns and insights"""
    
    def __init__(self, spark):
        self.spark = spark
        self.config = Config()
    
    def load_ride_data(self, city):
        """Load ride data from MongoDB"""
        try:
            client = MongoClient(self.config.MONGO_URI)
            db = client[self.config.DATABASE_NAME]
            
            collection = db[f"{city}_rides"]
            rides = list(collection.find({}, {'_id': 0}))
            client.close()
            
            if rides:
                df = pd.DataFrame(rides)
                return self.spark.createDataFrame(df)
            else:
                return None
                
        except Exception as e:
            print(f"Error loading ride data: {e}")
            return None
    
    def preprocess_ride_data(self, rides_df):
        """Clean and preprocess ride data"""
        if rides_df is None:
            return None
            
        # Convert timestamp
        rides_clean = rides_df.withColumn('timestamp', col('timestamp').cast('timestamp'))
        
        # Add time features
        rides_clean = rides_clean \
            .withColumn('hour', hour('timestamp')) \
            .withColumn('day_of_week', dayofweek('timestamp')) \
            .withColumn('is_weekend', when(col('day_of_week').isin([1, 7]), 1).otherwise(0)) \
            .withColumn('is_peak_hour', when((col('hour') >= 8) & (col('hour') <= 11) | 
                                           (col('hour') >= 16) & (col('hour') <= 21), 1).otherwise(0))
        
        # Calculate additional metrics
        rides_clean = rides_clean \
            .withColumn('fare_per_km', col('fare') / col('distance_km')) \
            .withColumn('speed_kmh', col('distance_km') / (col('duration_mins') / 60))
        
        # Filter outliers
        rides_clean = rides_clean \
            .filter(col('distance_km') > 0.5) \
            .filter(col('distance_km') < 100) \
            .filter(col('fare') > 10) \
            .filter(col('fare') < 5000) \
            .filter(col('duration_mins') > 1) \
            .filter(col('duration_mins') < 180)
        
        return rides_clean
    
    def analyze_ride_patterns(self, rides_df):
        """Analyze ride patterns and pricing"""
        analysis = {}
        
        # Price analysis by service
        price_analysis = rides_df.groupBy('service', 'vehicle_type').agg(
            mean('fare').alias('avg_fare'),
            mean('fare_per_km').alias('avg_fare_per_km'),
            mean('surge_multiplier').alias('avg_surge'),
            count('*').alias('ride_count')
        ).orderBy('service', 'vehicle_type')
        
        # Distance analysis
        distance_analysis = rides_df.groupBy('service', 'vehicle_type').agg(
            mean('distance_km').alias('avg_distance'),
            mean('duration_mins').alias('avg_duration'),
            stddev('distance_km').alias('std_distance')
        )
        
        # Service popularity
        service_popularity = rides_df.groupBy('service').agg(
            count('*').alias('total_rides'),
            mean('fare').alias('avg_fare')
        ).orderBy(col('total_rides').desc())
        
        return {
            'price_analysis': price_analysis,
            'distance_analysis': distance_analysis,
            'service_popularity': service_popularity
        }
    
    def analyze_surge_patterns(self, rides_df):
        """Analyze surge pricing patterns with Indian context"""
        
        # Peak vs Non-peak surge comparison
        peak_surge = rides_df.filter(col('is_peak_hour') == 1) \
            .groupBy('service') \
            .agg(mean('surge_multiplier').alias('avg_peak_surge'))
        
        non_peak_surge = rides_df.filter(col('is_peak_hour') == 0) \
            .groupBy('service') \
            .agg(mean('surge_multiplier').alias('avg_non_peak_surge'))
        
        # Weekend vs Weekday surge
        weekend_surge = rides_df.filter(col('is_weekend') == 1) \
            .groupBy('service') \
            .agg(mean('surge_multiplier').alias('avg_weekend_surge'))
        
        weekday_surge = rides_df.filter(col('is_weekend') == 0) \
            .groupBy('service') \
            .agg(mean('surge_multiplier').alias('avg_weekday_surge'))
        
        # Hourly surge patterns
        hourly_surge = rides_df.groupBy('hour', 'service') \
            .agg(mean('surge_multiplier').alias('avg_surge')) \
            .orderBy('hour', 'service')
        
        return {
            'peak_vs_non_peak': peak_surge.join(non_peak_surge, 'service'),
            'weekend_vs_weekday': weekend_surge.join(weekday_surge, 'service'),
            'hourly_patterns': hourly_surge
        }
    
    def create_ride_visualizations(self, analysis_results, city):
        """Create visualizations for ride data"""
        # Price comparison by service
        price_data = analysis_results['price_analysis'].toPandas()
        
        plt.figure(figsize=(12, 6))
        sns.barplot(data=price_data, x='service', y='avg_fare', hue='vehicle_type')
        plt.title(f'Average Ride Prices by Service - {city}')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_ride_prices.png')
        plt.close()
        
        # Service popularity
        popularity_data = analysis_results['service_popularity'].toPandas()
        
        plt.figure(figsize=(10, 6))
        plt.bar(popularity_data['service'], popularity_data['total_rides'])
        plt.title(f'Ride Service Popularity - {city}')
        plt.xlabel('Service')
        plt.ylabel('Number of Rides')
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_service_popularity.png')
        plt.close()
    
    def create_surge_visualizations(self, analysis_results, city):
        """Create visualizations focusing on Indian surge patterns"""
        # Hourly surge patterns
        hourly_data = analysis_results['hourly_patterns'].toPandas()
        
        plt.figure(figsize=(15, 8))
        
        for service in hourly_data['service'].unique():
            service_data = hourly_data[hourly_data['service'] == service]
            plt.plot(service_data['hour'], service_data['avg_surge'], 
                    marker='o', label=service, linewidth=2, markersize=6)
        
        # Highlight Indian peak hours
        plt.axvspan(8, 11, alpha=0.2, color='red', label='Morning Peak (8AM-11AM)')
        plt.axvspan(16, 21, alpha=0.2, color='orange', label='Evening Peak (4PM-9PM)')
        
        plt.title(f'Surge Pricing Patterns - {city} (Indian Peak Hours Highlighted)')
        plt.xlabel('Hour of Day')
        plt.ylabel('Average Surge Multiplier')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.xticks(range(0, 24))
        plt.tight_layout()
        plt.savefig(f'outputs/visuals/{city}_indian_surge_patterns.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Saved Indian surge patterns: outputs/visuals/{city}_indian_surge_patterns.png")
    
    

    def create_city_comparison(delhi_data, bangalore_data):
        """Create comparison between cities"""
        try:
            # Ensure we're working with Spark DataFrames
            if hasattr(delhi_data, 'select'):  # Spark DataFrame
                # Your Spark operations here
                delhi_count = delhi_data.count()
                bangalore_count = bangalore_data.count()
            else:  # Pandas DataFrame
                # Convert or handle pandas operations
                delhi_count = len(delhi_data)
                bangalore_count = len(bangalore_data)
                
            comparison = {
                'delhi_records': delhi_count,
                'bangalore_records': bangalore_count,
                # Add more comparison metrics
            }
            return comparison
            
        except Exception as e:
            print(f"Comparison error: {e}")
            return None
        
    def create_comparison_visualizations(self, rides_df, city):
        """Create comparison visualizations with robust error handling"""
        try:
            # Select only numeric and categorical columns to avoid datetime issues
            pandas_df = rides_df.select([
                'service', 'vehicle_type', 'fare', 'distance_km', 
                'duration_mins', 'surge_multiplier', 'hour', 'is_weekend'
            ]).toPandas()

            print(f"Creating comparison visualizations for {city} with {len(pandas_df)} rides")

            # Your existing visualization code here...
            
        except Exception as e:
            print(f"Error in comparison visualizations for {city}: {e}")
            import traceback
            traceback.print_exc()