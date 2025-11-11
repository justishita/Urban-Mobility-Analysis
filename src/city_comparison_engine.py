import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col, mean, count, hour, dayofweek
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from config import Config
from pymongo import MongoClient

class CityComparisonEngine:
    """Engine for comparing transportation patterns between cities"""
    
    def __init__(self, spark):
        self.spark = spark
        self.config = Config()
    
    def load_city_data(self, city, data_type):
        """Load specific data type for a city"""
        try:
            client = MongoClient(self.config.MONGO_URI)
            db = client[self.config.DATABASE_NAME]
            
            collection_name = f"{city}_{data_type}"
            collection = db[collection_name]
            data = list(collection.find({}, {'_id': 0}))
            client.close()
            
            if data:
                return self.spark.createDataFrame(data)
            return None
        except Exception as e:
            print(f"Error loading {data_type} for {city}: {e}")
            return None
    
    def generate_comparison_analysis(self):
        """Generate comprehensive city comparison"""
        try:
            print("Loading data for both cities...")
            
            # Load ride data for comparison
            delhi_rides = self.load_city_data('delhi', 'rides')
            bangalore_rides = self.load_city_data('bangalore', 'rides')
            
            comparisons = {}
            
            if delhi_rides and bangalore_rides:
                # Basic stats comparison
                delhi_stats = self._calculate_city_stats(delhi_rides, 'Delhi')
                bangalore_stats = self._calculate_city_stats(bangalore_rides, 'Bangalore')
                
                comparisons['basic_stats'] = {
                    'delhi': delhi_stats,
                    'bangalore': bangalore_stats
                }
                
                # Create visualizations
                self._create_comparison_visualizations(delhi_rides, bangalore_rides)
                
                # Price comparison
                price_comparison = self._compare_pricing(delhi_rides, bangalore_rides)
                comparisons['pricing'] = price_comparison
                
                print("City comparison analysis completed successfully")
                return comparisons
            else:
                print("Insufficient data for comprehensive comparison")
                return None
                
        except Exception as e:
            print(f"Comparison analysis error: {e}")
            return None
    
    def _calculate_city_stats(self, rides_df, city_name):
        """Calculate basic statistics for a city"""
        if rides_df is None:
            return {}
            
        # Convert to pandas for easier calculation
        pandas_df = rides_df.select([
            'service', 'vehicle_type', 'fare', 'distance_km', 
            'duration_mins', 'surge_multiplier'
        ]).toPandas()
        
        stats = {
            'city': city_name,
            'total_rides': len(pandas_df),
            'avg_fare': pandas_df['fare'].mean(),
            'avg_distance': pandas_df['distance_km'].mean(),
            'avg_duration': pandas_df['duration_mins'].mean(),
            'avg_surge': pandas_df['surge_multiplier'].mean(),
            'services': pandas_df['service'].nunique(),
            'vehicle_types': pandas_df['vehicle_type'].nunique()
        }
        
        print(f"{city_name}: {stats['total_rides']} rides, Avg fare: ₹{stats['avg_fare']:.2f}")
        return stats
    
    def _compare_pricing(self, delhi_rides, bangalore_rides):
        """Compare pricing between cities"""
        delhi_pandas = delhi_rides.select(['service', 'fare', 'distance_km']).toPandas()
        bangalore_pandas = bangalore_rides.select(['service', 'fare', 'distance_km']).toPandas()
        
        comparison = {}
        
        for service in delhi_pandas['service'].unique():
            delhi_service = delhi_pandas[delhi_pandas['service'] == service]
            bangalore_service = bangalore_pandas[bangalore_pandas['service'] == service]
            
            if len(delhi_service) > 0 and len(bangalore_service) > 0:
                comparison[service] = {
                    'delhi_avg_fare': delhi_service['fare'].mean(),
                    'bangalore_avg_fare': bangalore_service['fare'].mean(),
                    'delhi_fare_per_km': (delhi_service['fare'] / delhi_service['distance_km']).mean(),
                    'bangalore_fare_per_km': (bangalore_service['fare'] / bangalore_service['distance_km']).mean()
                }
        
        return comparison
    
    def _create_comparison_visualizations(self, delhi_rides, bangalore_rides):
        """Create comparison charts"""
        try:
            # Prepare data
            delhi_data = delhi_rides.select(['service', 'fare', 'distance_km', 'surge_multiplier']).toPandas()
            bangalore_data = bangalore_rides.select(['service', 'fare', 'distance_km', 'surge_multiplier']).toPandas()
            
            # Add city identifier
            delhi_data['city'] = 'Delhi'
            bangalore_data['city'] = 'Bangalore'
            
            combined_data = pd.concat([delhi_data, bangalore_data], ignore_index=True)
            
            # 1. Average Fare Comparison
            plt.figure(figsize=(12, 6))
            fare_comparison = combined_data.groupby(['city', 'service'])['fare'].mean().unstack()
            fare_comparison.plot(kind='bar', ax=plt.gca())
            plt.title('Average Ride Fare Comparison: Delhi vs Bangalore')
            plt.xlabel('City')
            plt.ylabel('Average Fare (INR)')
            plt.xticks(rotation=45)
            plt.legend(title='Service')
            plt.tight_layout()
            plt.savefig('outputs/visuals/city_comparison_fares.png')
            plt.close()
            
            # 2. Service Distribution
            plt.figure(figsize=(10, 6))
            service_dist = combined_data.groupby(['city', 'service']).size().unstack()
            service_dist.plot(kind='bar', stacked=True, ax=plt.gca())
            plt.title('Ride Service Distribution: Delhi vs Bangalore')
            plt.xlabel('City')
            plt.ylabel('Number of Rides')
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig('outputs/visuals/city_comparison_service_dist.png')
            plt.close()
            
            # 3. Fare vs Distance Scatter
            plt.figure(figsize=(12, 8))
            for city in combined_data['city'].unique():
                city_data = combined_data[combined_data['city'] == city]
                plt.scatter(city_data['distance_km'], city_data['fare'], 
                          alpha=0.6, label=city, s=30)
            
            plt.title('Fare vs Distance: Delhi vs Bangalore')
            plt.xlabel('Distance (km)')
            plt.ylabel('Fare (INR)')
            plt.legend()
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            plt.savefig('outputs/visuals/city_comparison_fare_distance.png')
            plt.close()
            
            print("City comparison visualizations saved successfully")
            
        except Exception as e:
            print(f"Visualization error: {e}")