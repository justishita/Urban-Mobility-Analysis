from pyspark.sql.functions import col, count, countDistinct, mean, stddev, when, lit, lag, unix_timestamp, split, hour, minute, second
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd
from pymongo import MongoClient
from config import Config

class SparkProcessor:
    """Handle Spark data processing for GTFS data analysis"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def pandas_to_spark_with_schema(self, pandas_df):
        """Convert pandas DataFrame to Spark DataFrame with proper schema handling"""
        if pandas_df.empty:
            return None
        
        # Clean column names for Spark
        pandas_df = self._clean_dataframe_columns(pandas_df)
        
        try:
            # Let Spark infer schema first
            spark_df = self.spark.createDataFrame(pandas_df)
            return spark_df
            
        except Exception as e:
            print(f"⚠️  Schema inference failed, using string schema: {e}")
            # Fallback: create with string schema
            schema = StructType([StructField(col, StringType()) for col in pandas_df.columns])
            return self.spark.createDataFrame(pandas_df, schema)
    
    def _clean_dataframe_columns(self, pandas_df):
        """Clean column names for Spark compatibility"""
        pandas_df.columns = [col.replace(' ', '_').replace('-', '_').lower() for col in pandas_df.columns]
        return pandas_df
    
    def process_city_data(self, city, mongo_manager):
        """Process GTFS data for a city using Spark"""
        print(f"\n🔧 Processing {city} data with Spark...")
        
        # Load data from MongoDB collections
        collections_data = self._load_collections_from_mongodb(city, mongo_manager)
        
        # Check if we have the essential data
        essential_collections = ['routes', 'trips', 'stop_times', 'stops']
        if not all(collections_data.get(col) for col in essential_collections):
            print(f"❌ Missing essential data for {city}")
            return None
        
        # Process the data
        try:
            results = self.process_gtfs_data(collections_data, city)
            return results
        except Exception as e:
            print(f"❌ Error processing {city} data: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _load_collections_from_mongodb(self, city, mongo_manager):
        """Load all collections from MongoDB for a city"""
        collections_data = {}
        file_types = ['routes', 'trips', 'stop_times', 'stops', 'agency', 'calendar']
        
        for file_type in file_types:
            collection_name = f"{city}_{file_type}"
            try:
                collection = mongo_manager.db[collection_name]
                pandas_df = pd.DataFrame(list(collection.find({}, {'_id': 0})))
                
                if not pandas_df.empty:
                    spark_df = self.pandas_to_spark_with_schema(pandas_df)
                    collections_data[file_type] = spark_df
                    print(f"  ✅ Loaded {file_type}: {pandas_df.shape[0]} records")
                else:
                    print(f"  ⚠️  No data found for {file_type}")
                    collections_data[file_type] = None
                    
            except Exception as e:
                print(f"  ❌ Error loading {file_type}: {e}")
                collections_data[file_type] = None
        
        return collections_data
    
    def process_gtfs_data(self, collections_data, city):
        """Process GTFS data and calculate comprehensive metrics"""
        routes = collections_data['routes']
        trips = collections_data['trips']
        stop_times = collections_data['stop_times']
        stops = collections_data['stops']
        
        print("  🧹 Cleaning and transforming data...")
        
        # Basic data cleaning
        routes_clean = self._clean_routes_data(routes)
        trips_clean = self._clean_trips_data(trips)
        stops_clean = self._clean_stops_data(stops)
        stop_times_clean = self._clean_stop_times_data(stop_times)
        
        print("  🔗 Joining datasets...")
        
        # Create comprehensive trip-stop events
        trip_stop_events = self._create_trip_stop_events(
            stop_times_clean, trips_clean, routes_clean, stops_clean
        )
        
        # Calculate various metrics
        print("  📊 Calculating route statistics...")
        route_stats = self._calculate_route_statistics(trip_stop_events)
        
        print("  ⏱️  Calculating headways...")
        headways = self._calculate_headways(trip_stop_events)
        
        print("  🕒 Calculating service frequency...")
        hourly_service = self._calculate_hourly_service(trip_stop_events)
        
        print("  📈 Calculating spatial statistics...")
        spatial_stats = self._calculate_spatial_statistics(trip_stop_events)
        
        return {
            'trip_stop_events': trip_stop_events,
            'route_stats': route_stats,
            'headways': headways,
            'hourly_service': hourly_service,
            'spatial_stats': spatial_stats
        }
    
    def _clean_routes_data(self, routes):
        """Clean routes data"""
        return routes.dropDuplicates(['route_id']).filter(col('route_id').isNotNull())
    
    def _clean_trips_data(self, trips):
        """Clean trips data"""
        return trips.dropDuplicates(['trip_id']).filter(col('trip_id').isNotNull())
    
    def _clean_stops_data(self, stops):
        """Clean stops data"""
        return stops.dropDuplicates(['stop_id']).filter(col('stop_id').isNotNull())
    
    def _clean_stop_times_data(self, stop_times):
        """Clean stop_times data and parse time formats"""
        stop_times_clean = stop_times \
            .filter(col('trip_id').isNotNull()) \
            .filter(col('stop_id').isNotNull()) \
            .filter(col('arrival_time').isNotNull())
        
        # Parse time with better error handling
        stop_times_clean = stop_times_clean \
            .withColumn('arrival_time_clean', 
                       when(col('arrival_time').rlike('^\\d{1,2}:\\d{2}:\\d{2}$'), col('arrival_time'))
                       .otherwise(lit('00:00:00'))) \
            .withColumn('arrival_hour', 
                       split(col('arrival_time_clean'), ':').getItem(0).cast('int')) \
            .withColumn('arrival_minute',
                       split(col('arrival_time_clean'), ':').getItem(1).cast('int')) \
            .withColumn('arrival_second',
                       split(col('arrival_time_clean'), ':').getItem(2).cast('int')) \
            .withColumn('arrival_total_seconds',
                       col('arrival_hour') * 3600 + col('arrival_minute') * 60 + col('arrival_second'))
        
        return stop_times_clean
    
    def _create_trip_stop_events(self, stop_times, trips, routes, stops):
        """Create comprehensive trip-stop events table"""
        return stop_times \
            .join(trips, 'trip_id') \
            .join(routes, 'route_id') \
            .join(stops, 'stop_id')
    
    def _calculate_route_statistics(self, trip_stop_events):
        """Calculate comprehensive route statistics"""
        return trip_stop_events.groupBy('route_id', 'route_short_name', 'route_long_name').agg(
            countDistinct('trip_id').alias('num_trips'),
            countDistinct('stop_id').alias('num_stops'),
            count('*').alias('total_stop_events'),
            mean('arrival_hour').alias('avg_service_hour'),
            stddev('arrival_hour').alias('std_service_hour'),
            countDistinct('service_id').alias('num_services')
        ).orderBy(col('num_trips').desc())
    
    def _calculate_headways(self, trip_stop_events):
        """Calculate headways between consecutive vehicles"""
        window_spec = Window.partitionBy('route_id', 'stop_id').orderBy('arrival_total_seconds')
        
        headways = trip_stop_events \
            .withColumn('prev_arrival', lag('arrival_total_seconds').over(window_spec)) \
            .withColumn('headway_seconds',
                       when(col('prev_arrival').isNotNull(),
                            col('arrival_total_seconds') - col('prev_arrival'))
                       .otherwise(lit(None))) \
            .filter(col('headway_seconds').isNotNull()) \
            .filter(col('headway_seconds') > 0) \
            .filter(col('headway_seconds') < 3600)  # Remove headways > 1 hour
        
        return headways
    
    def _calculate_hourly_service(self, trip_stop_events):
        """Calculate hourly service frequency"""
        return trip_stop_events.groupBy('route_id', 'arrival_hour').agg(
            count('*').alias('hourly_stop_events'),
            countDistinct('trip_id').alias('hourly_trips'),
            countDistinct('stop_id').alias('hourly_stops')
        ).orderBy('route_id', 'arrival_hour')
    
    def _calculate_spatial_statistics(self, trip_stop_events):
        """Calculate spatial statistics for stops"""
        return trip_stop_events.groupBy('stop_id', 'stop_name').agg(
            countDistinct('route_id').alias('num_routes'),
            countDistinct('trip_id').alias('num_trips'),
            count('*').alias('total_events'),
            mean('stop_lat').alias('avg_lat'),
            mean('stop_lon').alias('avg_lon')
        ).filter(col('avg_lat').isNotNull() & col('avg_lon').isNotNull())
    
    def save_spark_results(self, results, city):
        """Save Spark results to CSV and MongoDB"""
        if not results:
            return
        
        print(f"  💾 Saving {city} results...")
        
        for result_name, spark_df in results.items():
            try:
                # Limit for large datasets and convert to pandas
                pandas_df = spark_df.limit(50000).toPandas()
                
                # Save to CSV
                output_path = f"outputs/data/{city}_{result_name}.csv"
                pandas_df.to_csv(output_path, index=False)
                print(f"    ✅ CSV: {output_path} ({len(pandas_df)} records)")
                
                # Save to MongoDB
                self._save_to_mongodb(pandas_df, f"{city}_analytics_{result_name}")
                
            except Exception as e:
                print(f"    ❌ Error saving {result_name}: {e}")
    
    def _save_to_mongodb(self, pandas_df, collection_name):
        """Save DataFrame to MongoDB"""
        try:
            client = MongoClient(Config.MONGO_URI)
            db = client[Config.DATABASE_NAME]
            collection = db[collection_name]
            
            # Clear existing data
            collection.delete_many({})
            
            # Insert new data
            if not pandas_df.empty:
                records = pandas_df.to_dict('records')
                collection.insert_many(records)
                print(f"    ✅ MongoDB: {collection_name} ({len(records)} records)")
                
        except Exception as e:
            print(f"    ❌ MongoDB save error for {collection_name}: {e}")
        finally:
            if 'client' in locals():
                client.close()